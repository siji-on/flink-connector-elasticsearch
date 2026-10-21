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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.sink.writer.TestSinkInitContext;
import org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.DeleteStrategy;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.formats.json.JsonRowDataSerializationSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import co.elastic.clients.elasticsearch.core.bulk.UpdateOperation;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that {@link RowDataElementConverter} builds the document with the table format, so nested
 * types survive and the format options are honoured.
 */
class RowDataElementConverterTest {

    private static final DataType ROW_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("id", DataTypes.STRING().notNull()),
                    DataTypes.FIELD("price", DataTypes.DECIMAL(20, 2)),
                    DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3)),
                    DataTypes.FIELD("ts_ltz", DataTypes.TIMESTAMP_LTZ(3)),
                    DataTypes.FIELD("vec", DataTypes.ARRAY(DataTypes.FLOAT())),
                    DataTypes.FIELD("cnt", DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())),
                    DataTypes.FIELD("opt", optionType()),
                    DataTypes.FIELD("opts", DataTypes.ARRAY(optionType())));

    private static DataType optionType() {
        return DataTypes.ROW(
                DataTypes.FIELD("code", DataTypes.STRING()),
                DataTypes.FIELD("qty", DataTypes.INT()));
    }

    @Test
    void testNestedTypesAreSerializedByTheFormat() {
        IndexOperation<?> operation =
                (IndexOperation<?>)
                        converter(DeleteStrategy.DELETE_ROW_ON_PK, TimestampFormat.ISO_8601)
                                .apply(row(RowKind.INSERT), null);

        assertThat(operation.index()).isEqualTo("idx");
        assertThat(operation.id()).isEqualTo("ad-1");
        assertThat(operation.document()).isEqualTo(expectedDocument("2026-10-22T01:02:03.456"));
    }

    @Test
    void testTimestampFormatOptionIsHonoured() {
        IndexOperation<?> operation =
                (IndexOperation<?>)
                        converter(DeleteStrategy.DELETE_ROW_ON_PK, TimestampFormat.SQL)
                                .apply(row(RowKind.INSERT), null);

        assertThat(operation.document()).isEqualTo(expectedDocument("2026-10-22 01:02:03.456"));
    }

    @Test
    void testUpsertUsesTheSameDocument() {
        UpdateOperation<?, ?> operation =
                (UpdateOperation<?, ?>)
                        converter(DeleteStrategy.DELETE_ROW_ON_PK, TimestampFormat.ISO_8601)
                                .apply(row(RowKind.UPDATE_AFTER), null);

        assertThat(operation.action().docAsUpsert()).isTrue();
        assertThat(operation.action().doc()).isEqualTo(expectedDocument("2026-10-22T01:02:03.456"));
    }

    @Test
    void testNonPkFieldToNullKeepsOnlyPrimaryKeyFields() {
        UpdateOperation<?, ?> operation =
                (UpdateOperation<?, ?>)
                        converter(DeleteStrategy.NON_PK_FIELD_TO_NULL, TimestampFormat.ISO_8601)
                                .apply(row(RowKind.DELETE), null);

        Map<String, Object> expected = new HashMap<>();
        expected.put("id", "ad-1");
        for (String field : DataType.getFieldNames(ROW_TYPE)) {
            expected.putIfAbsent(field, null);
        }
        assertThat(operation.action().docAsUpsert()).isTrue();
        assertThat(operation.action().doc()).isEqualTo(expected);
    }

    @Test
    void testDecimalsKeepTheirPrecision() {
        Map<String, Object> document =
                converter(DeleteStrategy.DELETE_ROW_ON_PK, TimestampFormat.ISO_8601)
                        .toDocument(row(RowKind.INSERT));

        assertThat(document.get("price")).isEqualTo(new BigDecimal("1234.5"));
    }

    static SerializationSchema<RowData> jsonSerializer(
            DataType rowType, TimestampFormat timestampFormat) {
        return new JsonRowDataSerializationSchema(
                (RowType) rowType.getLogicalType(),
                timestampFormat,
                JsonFormatOptions.MapNullKeyMode.FAIL,
                "null",
                false,
                false);
    }

    private static RowDataElementConverter converter(
            DeleteStrategy strategy, TimestampFormat timestampFormat) {
        IndexGenerator indexGenerator =
                IndexGeneratorFactory.createIndexGenerator(
                        "idx",
                        DataType.getFieldNames(ROW_TYPE),
                        DataType.getFieldDataTypes(ROW_TYPE),
                        ZoneId.systemDefault());
        RowDataElementConverter converter =
                new RowDataElementConverter(
                        jsonSerializer(ROW_TYPE, timestampFormat),
                        indexGenerator,
                        row -> row.getString(0).toString(),
                        strategy,
                        Collections.singleton("id"));
        converter.open(new TestSinkInitContext());
        return converter;
    }

    private static RowData row(RowKind kind) {
        GenericRowData opt = GenericRowData.of(StringData.fromString("A"), 2);
        Map<Object, Object> cnt = new HashMap<>();
        cnt.put(StringData.fromString("view"), 7L);
        GenericRowData row =
                GenericRowData.of(
                        StringData.fromString("ad-1"),
                        DecimalData.fromBigDecimal(new BigDecimal("1234.50"), 20, 2),
                        TimestampData.fromLocalDateTime(
                                LocalDateTime.of(2026, 10, 22, 1, 2, 3, 456_000_000)),
                        TimestampData.fromInstant(Instant.parse("2026-10-21T16:02:03.456Z")),
                        new GenericArrayData(new Float[] {0.1f, 0.2f}),
                        new GenericMapData(cnt),
                        opt,
                        new GenericArrayData(new RowData[] {opt}));
        row.setRowKind(kind);
        return row;
    }

    private static Map<String, Object> expectedDocument(String timestamp) {
        Map<String, Object> opt = new LinkedHashMap<>();
        opt.put("code", "A");
        opt.put("qty", 2);

        Map<String, Object> expected = new LinkedHashMap<>();
        expected.put("id", "ad-1");
        expected.put("price", new BigDecimal("1234.5"));
        expected.put("ts", timestamp);
        expected.put(
                "ts_ltz",
                timestamp.contains("T")
                        ? "2026-10-21T16:02:03.456Z"
                        : "2026-10-21 16:02:03.456Z");
        expected.put("vec", Arrays.asList(new BigDecimal("0.1"), new BigDecimal("0.2")));
        expected.put("cnt", Collections.singletonMap("view", 7));
        expected.put("opt", opt);
        expected.put("opts", Collections.singletonList(opt));
        return expected;
    }
}
