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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.DeleteStrategy;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import co.elastic.clients.elasticsearch.core.bulk.BulkOperationVariant;
import co.elastic.clients.elasticsearch.core.bulk.DeleteOperation;
import co.elastic.clients.elasticsearch.core.bulk.UpdateOperation;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.DELETE_STRATEGY_OPTION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DeleteStrategy} handling in the Elasticsearch 8 table sink. */
class ElasticSearch8AsyncDynamicSinkDeleteStrategyTest {

    private static final ResolvedSchema PK_SCHEMA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical("a", DataTypes.INT().notNull()),
                            Column.physical("b", DataTypes.STRING())),
                    Collections.emptyList(),
                    UniqueConstraint.primaryKey("pk", Collections.singletonList("a")));

    private static final ResolvedSchema NO_PK_SCHEMA =
            ResolvedSchema.of(
                    Column.physical("a", DataTypes.INT().notNull()),
                    Column.physical("b", DataTypes.STRING()));

    @Test
    void testChangelogModeDropsUpdateBeforeUnlessChangelogStandard() {
        for (DeleteStrategy strategy : DeleteStrategy.values()) {
            ChangelogMode mode =
                    createSink(strategy, PK_SCHEMA).getChangelogMode(ChangelogMode.all());

            assertThat(mode.contains(RowKind.INSERT)).as("%s", strategy).isTrue();
            assertThat(mode.contains(RowKind.UPDATE_AFTER)).as("%s", strategy).isTrue();
            // the planner does not allow a sink to refuse DELETE, it is filtered in front of it
            assertThat(mode.contains(RowKind.DELETE)).as("%s", strategy).isTrue();
            assertThat(mode.contains(RowKind.UPDATE_BEFORE))
                    .as("%s", strategy)
                    .isEqualTo(!strategy.isIgnoreUpdateBefore());
        }
    }

    @Test
    void testRuntimeProviderFiltersOnlyWhenDeletesAreIgnored() {
        for (DeleteStrategy strategy : DeleteStrategy.values()) {
            DynamicTableSink.SinkRuntimeProvider provider =
                    createSink(strategy, PK_SCHEMA).getSinkRuntimeProvider(new MockContext());

            if (strategy.isIgnoreDelete()) {
                assertThat(provider)
                        .as("%s", strategy)
                        .isInstanceOf(
                                ElasticSearch8AsyncDynamicSink.RowKindFilteringSinkProvider.class);
                assertThat(((DataStreamSinkProvider) provider).getParallelism()).isEmpty();
            } else {
                assertThat(provider).as("%s", strategy).isInstanceOf(SinkV2Provider.class);
            }
        }
    }

    @Test
    void testNonPkFieldToNullRequiresPrimaryKey() {
        assertThatThrownBy(() -> createSink(DeleteStrategy.NON_PK_FIELD_TO_NULL, NO_PK_SCHEMA))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("requires a primary key");

        // every other strategy is fine without a primary key (append-only use)
        for (DeleteStrategy strategy : DeleteStrategy.values()) {
            if (strategy != DeleteStrategy.NON_PK_FIELD_TO_NULL) {
                createSink(strategy, NO_PK_SCHEMA);
            }
        }
    }

    @Test
    void testIgnoredRowKindFilter() throws Exception {
        ElasticSearch8AsyncDynamicSink.IgnoredRowKindFilter filter =
                new ElasticSearch8AsyncDynamicSink.IgnoredRowKindFilter(
                        DeleteStrategy.IGNORE_DELETE);

        assertThat(filter.filter(row(RowKind.INSERT))).isTrue();
        assertThat(filter.filter(row(RowKind.UPDATE_AFTER))).isTrue();
        assertThat(filter.filter(row(RowKind.UPDATE_BEFORE))).isFalse();
        assertThat(filter.filter(row(RowKind.DELETE))).isFalse();
    }

    @Test
    void testDeleteRowOnPkConvertsDeleteToDeleteOperation() {
        BulkOperationVariant operation =
                converter(DeleteStrategy.DELETE_ROW_ON_PK).apply(row(RowKind.DELETE), null);

        assertThat(operation).isInstanceOf(DeleteOperation.class);
        DeleteOperation delete = (DeleteOperation) operation;
        assertThat(delete.index()).isEqualTo("idx");
        assertThat(delete.id()).isEqualTo("1");
    }

    @Test
    void testChangelogStandardConvertsUpdateBeforeToDeleteOperation() {
        BulkOperationVariant operation =
                converter(DeleteStrategy.CHANGELOG_STANDARD)
                        .apply(row(RowKind.UPDATE_BEFORE), null);

        assertThat(operation).isInstanceOf(DeleteOperation.class);
        assertThat(((DeleteOperation) operation).id()).isEqualTo("1");
    }

    @Test
    void testNonPkFieldToNullConvertsDeleteToPartialUpdate() {
        BulkOperationVariant operation =
                converter(DeleteStrategy.NON_PK_FIELD_TO_NULL).apply(row(RowKind.DELETE), null);

        assertThat(operation).isInstanceOf(UpdateOperation.class);
        UpdateOperation<?, ?> update = (UpdateOperation<?, ?>) operation;
        Map<String, Object> expectedDoc = new HashMap<>();
        expectedDoc.put("a", 1);
        expectedDoc.put("b", null);
        assertThat(update.index()).isEqualTo("idx");
        assertThat(update.id()).isEqualTo("1");
        assertThat(update.action().doc()).isEqualTo(expectedDoc);
        assertThat(update.action().docAsUpsert()).isTrue();
    }

    @Test
    void testIgnoredRowKindsNeverReachTheConverter() {
        assertThatThrownBy(
                        () ->
                                converter(DeleteStrategy.IGNORE_DELETE)
                                        .apply(row(RowKind.DELETE), null))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(
                        () ->
                                converter(DeleteStrategy.DELETE_ROW_ON_PK)
                                        .apply(row(RowKind.UPDATE_BEFORE), null))
                .isInstanceOf(IllegalStateException.class);
    }

    private static DynamicTableSink createSink(DeleteStrategy strategy, ResolvedSchema schema) {
        TestContext context =
                TestContext.context()
                        .withSchema(schema)
                        .withOption(
                                Elasticsearch8ConnectorOptions.HOSTS_OPTION.key(),
                                "http://localhost:9200")
                        .withOption(Elasticsearch8ConnectorOptions.INDEX_OPTION.key(), "idx")
                        .withOption(DELETE_STRATEGY_OPTION.key(), strategy.name());
        return new ElasticSearch8AsyncDynamicTableFactory().createDynamicTableSink(context.build());
    }

    private static RowDataElementConverter converter(DeleteStrategy strategy) {
        DataType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("a", DataTypes.INT().notNull()),
                        DataTypes.FIELD("b", DataTypes.STRING()));
        IndexGenerator indexGenerator =
                IndexGeneratorFactory.createIndexGenerator(
                        "idx",
                        DataType.getFieldNames(rowType),
                        DataType.getFieldDataTypes(rowType),
                        ZoneId.systemDefault());
        RowDataElementConverter converter =
                new RowDataElementConverter(
                        rowType,
                        indexGenerator,
                        row -> String.valueOf(row.getInt(0)),
                        strategy,
                        Collections.singleton("a"));
        converter.open(null);
        return converter;
    }

    private static RowData row(RowKind kind) {
        GenericRowData row = GenericRowData.of(1, StringData.fromString("x"));
        row.setRowKind(kind);
        return row;
    }

    /** Minimal {@link DynamicTableSink.Context}; the sink does not use it. */
    private static class MockContext implements DynamicTableSink.Context {
        @Override
        public boolean isBounded() {
            return false;
        }

        @Override
        public TypeInformation<?> createTypeInformation(DataType consumedDataType) {
            return null;
        }

        @Override
        public TypeInformation<?> createTypeInformation(LogicalType consumedLogicalType) {
            return null;
        }

        @Override
        public DynamicTableSink.DataStructureConverter createDataStructureConverter(
                DataType consumedDataType) {
            return null;
        }

        @Override
        public Optional<int[][]> getTargetColumns() {
            return Optional.empty();
        }
    }
}
