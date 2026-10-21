/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.elasticsearch.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.DeleteStrategy;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.FlinkRuntimeException;

import co.elastic.clients.elasticsearch.core.bulk.BulkOperationVariant;
import co.elastic.clients.elasticsearch.core.bulk.DeleteOperation;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import co.elastic.clients.elasticsearch.core.bulk.UpdateOperation;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Converts a {@link RowData} into the {@link BulkOperationVariant} that is sent to Elasticsearch.
 *
 * <p>The document body is produced by the table's {@code format} (by default the {@code json}
 * format), exactly like the legacy Elasticsearch table sinks did. This keeps nested {@code ROW},
 * {@code ARRAY<ROW>} and {@code MAP} columns intact and honours the format options such as {@code
 * json.timestamp-format.standard}. The JSON is parsed back into a map so that the Elasticsearch
 * client can (de)serialize it for the bulk request and for the sink's checkpoint state.
 */
@Internal
public class RowDataElementConverter implements ElementConverter<RowData, BulkOperationVariant> {
    private static final TypeReference<LinkedHashMap<String, Object>> DOCUMENT_TYPE =
            new TypeReference<LinkedHashMap<String, Object>>() {};

    private final SerializationSchema<RowData> serializationSchema;
    private final IndexGenerator indexGenerator;
    private final Function<RowData, String> keyExtractor;
    private final DeleteStrategy deleteStrategy;
    private final Set<String> primaryKeyFieldNames;

    private transient ObjectMapper documentReader;

    public RowDataElementConverter(
            SerializationSchema<RowData> serializationSchema,
            IndexGenerator indexGenerator,
            Function<RowData, String> keyExtractor,
            DeleteStrategy deleteStrategy,
            Set<String> primaryKeyFieldNames) {
        this.serializationSchema = checkNotNull(serializationSchema);
        this.indexGenerator = checkNotNull(indexGenerator);
        this.keyExtractor = checkNotNull(keyExtractor);
        this.deleteStrategy = checkNotNull(deleteStrategy);
        this.primaryKeyFieldNames = new HashSet<>(checkNotNull(primaryKeyFieldNames));
    }

    @Override
    public void open(WriterInitContext context) {
        indexGenerator.open();
        try {
            serializationSchema.open(context.asSerializationSchemaInitializationContext());
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to open the document serialization schema", e);
        }
        documentReader = createDocumentReader();
    }

    @Override
    public BulkOperationVariant apply(RowData rowData, SinkWriter.Context context) {
        RowKind rowKind = rowData.getRowKind();
        if (deleteStrategy.ignores(rowKind)) {
            // Ignored kinds are excluded from the changelog mode or filtered out in front of the
            // sink (see ElasticSearch8AsyncDynamicSink); reaching this point is a bug.
            throw new IllegalStateException(
                    String.format(
                            "Rows of kind %s must not reach the Elasticsearch sink with '%s' = '%s'.",
                            rowKind,
                            Elasticsearch8ConnectorOptions.DELETE_STRATEGY_OPTION.key(),
                            deleteStrategy));
        }

        BulkOperationVariant operation;

        switch (rowKind) {
            case INSERT:
                {
                    Map<String, Object> document = toDocument(rowData);
                    operation =
                            new IndexOperation.Builder<>()
                                    .index(indexGenerator.generate(rowData))
                                    .id(keyExtractor.apply(rowData))
                                    .document(document)
                                    .build();
                    break;
                }
            case UPDATE_AFTER:
                {
                    Map<String, Object> document = toDocument(rowData);
                    operation =
                            new UpdateOperation.Builder<>()
                                    .index(indexGenerator.generate(rowData))
                                    .id(keyExtractor.apply(rowData))
                                    .action(a -> a.doc(document).docAsUpsert(true))
                                    .build();
                    break;
                }
            case UPDATE_BEFORE:
            case DELETE:
                operation = createRetractOperation(rowData);
                break;
            default:
                throw new TableException("Unsupported message kind: " + rowKind);
        }

        return operation;
    }

    private BulkOperationVariant createRetractOperation(RowData rowData) {
        String index = indexGenerator.generate(rowData);
        String id = keyExtractor.apply(rowData);

        if (deleteStrategy == DeleteStrategy.NON_PK_FIELD_TO_NULL) {
            // Keep the document (and its primary key fields) but null out everything else. The
            // upsert makes a retraction for a missing document a no-op instead of a bulk failure
            // that would be retried forever.
            Map<String, Object> document = toDocument(rowData);
            for (Map.Entry<String, Object> entry : document.entrySet()) {
                if (!primaryKeyFieldNames.contains(entry.getKey())) {
                    entry.setValue(null);
                }
            }
            return new UpdateOperation.Builder<>()
                    .index(index)
                    .id(id)
                    .action(a -> a.doc(document).docAsUpsert(true))
                    .build();
        }

        return new DeleteOperation.Builder().index(index).id(id).build();
    }

    /** Serializes the row with the table format and parses the JSON back into a document map. */
    Map<String, Object> toDocument(RowData rowData) {
        byte[] json = serializationSchema.serialize(rowData);
        try {
            return documentReader().readValue(json, DOCUMENT_TYPE);
        } catch (IOException e) {
            throw new FlinkRuntimeException(
                    "The configured format did not produce a JSON object for row " + rowData, e);
        }
    }

    private ObjectMapper documentReader() {
        if (documentReader == null) {
            // the converter may be used without open() (e.g. in tests)
            documentReader = createDocumentReader();
        }
        return documentReader;
    }

    private static ObjectMapper createDocumentReader() {
        return JsonMapper.builder()
                // keep DECIMAL values exact instead of parsing them into doubles
                .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                .build();
    }
}
