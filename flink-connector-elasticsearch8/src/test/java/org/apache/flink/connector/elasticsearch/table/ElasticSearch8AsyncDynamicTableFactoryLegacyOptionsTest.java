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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The DDL options of the legacy (SinkFunction based) Elasticsearch connectors must be accepted so
 * that existing tables can be moved to this connector without touching their definitions.
 */
class ElasticSearch8AsyncDynamicTableFactoryLegacyOptionsTest {

    private static final ResolvedSchema SCHEMA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical("id", DataTypes.STRING().notNull()),
                            Column.physical("ts", DataTypes.TIMESTAMP(3)),
                            Column.physical(
                                    "opts",
                                    DataTypes.ARRAY(
                                            DataTypes.ROW(
                                                    DataTypes.FIELD("code", DataTypes.STRING()),
                                                    DataTypes.FIELD("qty", DataTypes.INT()))))),
                    Collections.emptyList(),
                    UniqueConstraint.primaryKey("pk", Collections.singletonList("id")));

    @Test
    void testLegacyDdlOptionsAreAccepted() {
        TestContext context =
                legacyContext()
                        .withOption("sink.flush-on-checkpoint", "true")
                        .withOption("sink.bulk-flush.backoff.strategy", "exponential")
                        .withOption("sink.bulk-flush.backoff.delay", "1s")
                        .withOption("sink.bulk-flush.backoff.max-retries", "8")
                        .withOption("sink.bulk-flush.interval", "3000")
                        .withOption("sink.bulk-flush.max-size", "5mb")
                        .withOption("format", "json")
                        .withOption("json.timestamp-format.standard", "ISO-8601");

        DynamicTableSink sink =
                new ElasticSearch8AsyncDynamicTableFactory().createDynamicTableSink(context.build());

        assertThat(sink).isInstanceOf(ElasticSearch8AsyncDynamicSink.class);
        assertThat(((ElasticSearch8AsyncDynamicSink) sink).config.getBulkFlushInterval())
                .isEqualTo(3000L);
    }

    @Test
    void testFlushOnCheckpointFalseIsAcceptedButIgnored() {
        DynamicTableSink sink =
                new ElasticSearch8AsyncDynamicTableFactory()
                        .createDynamicTableSink(
                                legacyContext()
                                        .withOption("sink.flush-on-checkpoint", "false")
                                        .build());

        assertThat(sink).isInstanceOf(ElasticSearch8AsyncDynamicSink.class);
    }

    @Test
    void testUnknownOptionsAreStillRejected() {
        assertThatThrownBy(
                        () ->
                                new ElasticSearch8AsyncDynamicTableFactory()
                                        .createDynamicTableSink(
                                                legacyContext()
                                                        .withOption("failure-handler", "ignore")
                                                        .build()))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Unsupported options");
    }

    private static TestContext legacyContext() {
        return TestContext.context()
                .withSchema(SCHEMA)
                .withOption("connector", "elasticsearch-8")
                .withOption("hosts", "http://localhost:9200")
                .withOption("index", "idx")
                .withOption("sink.delete-strategy", "IGNORE_DELETE");
    }
}
