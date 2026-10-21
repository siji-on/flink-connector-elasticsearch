/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.flink.connector.elasticsearch.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.DeleteStrategy;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import co.elastic.clients.elasticsearch.core.bulk.BulkOperationVariant;
import co.elastic.clients.elasticsearch.core.bulk.DeleteOperation;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import co.elastic.clients.elasticsearch.core.bulk.UpdateOperation;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Implementation of an {@link ElementConverter} for the ElasticSearch Table sink. The element
 * converter maps the Flink internal type of {@link RowData} to a {@link BulkOperationVariant} to be
 * used by Elasticsearch Java API
 */
@Internal
public class RowDataElementConverter implements ElementConverter<RowData, BulkOperationVariant> {
    private final IndexGenerator indexGenerator;
    private final Function<RowData, String> keyExtractor;
    private final RowDataToMapConverter rowDataToMapConverter;
    private final DeleteStrategy deleteStrategy;
    private final Set<String> primaryKeyFieldNames;

    public RowDataElementConverter(
            DataType physicalDataType,
            IndexGenerator indexGenerator,
            Function<RowData, String> keyExtractor) {
        this(
                physicalDataType,
                indexGenerator,
                keyExtractor,
                DeleteStrategy.DELETE_ROW_ON_PK,
                Collections.emptySet());
    }

    public RowDataElementConverter(
            DataType physicalDataType,
            IndexGenerator indexGenerator,
            Function<RowData, String> keyExtractor,
            DeleteStrategy deleteStrategy,
            Set<String> primaryKeyFieldNames) {
        this.rowDataToMapConverter = new RowDataToMapConverter(physicalDataType);
        this.indexGenerator = indexGenerator;
        this.keyExtractor = keyExtractor;
        this.deleteStrategy = checkNotNull(deleteStrategy);
        this.primaryKeyFieldNames = new HashSet<>(checkNotNull(primaryKeyFieldNames));
    }

    @Override
    public void open(WriterInitContext context) {
        indexGenerator.open();
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
                    Map<String, Object> dataMap = rowDataToMapConverter.toMap(rowData);
                    operation =
                            new IndexOperation.Builder<>()
                                    .index(indexGenerator.generate(rowData))
                                    .id(keyExtractor.apply(rowData))
                                    .document(dataMap)
                                    .build();
                    break;
                }
            case UPDATE_AFTER:
                {
                    Map<String, Object> dataMap = rowDataToMapConverter.toMap(rowData);
                    operation =
                            new UpdateOperation.Builder<>()
                                    .index(indexGenerator.generate(rowData))
                                    .id(keyExtractor.apply(rowData))
                                    .action(a -> a.doc(dataMap).docAsUpsert(true))
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
            Map<String, Object> dataMap = rowDataToMapConverter.toMap(rowData);
            for (Map.Entry<String, Object> entry : dataMap.entrySet()) {
                if (!primaryKeyFieldNames.contains(entry.getKey())) {
                    entry.setValue(null);
                }
            }
            return new UpdateOperation.Builder<>()
                    .index(index)
                    .id(id)
                    .action(a -> a.doc(dataMap).docAsUpsert(true))
                    .build();
        }

        return new DeleteOperation.Builder().index(index).id(id).build();
    }
}
