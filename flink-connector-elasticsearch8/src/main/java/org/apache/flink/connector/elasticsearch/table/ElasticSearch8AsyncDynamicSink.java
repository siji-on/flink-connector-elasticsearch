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
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.table.sink.AsyncDynamicTableSink;
import org.apache.flink.connector.base.table.sink.AsyncDynamicTableSinkBuilder;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch8AsyncSink;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch8AsyncSinkBuilder;
import org.apache.flink.connector.elasticsearch.sink.Operation;
import org.apache.flink.connector.elasticsearch.table.Elasticsearch8ConnectorOptions.DeleteStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.StringUtils;

import org.apache.http.HttpHost;

import javax.annotation.Nullable;

import java.time.ZoneId;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** ElasticSearch backed {@link AsyncDynamicTableSink}. */
@Internal
public class ElasticSearch8AsyncDynamicSink extends AsyncDynamicTableSink<Operation> {
    final transient EncodingFormat<SerializationSchema<RowData>> format;
    final DataType physicalRowDataType;
    final List<LogicalTypeWithIndex> primaryKeyLogicalTypesWithIndex;
    final Elasticsearch8Configuration config;
    final ZoneId localTimeZoneId;

    final String summaryString;
    final boolean isDynamicIndexWithSystemTime;

    public ElasticSearch8AsyncDynamicSink(
            EncodingFormat<SerializationSchema<RowData>> format,
            Elasticsearch8Configuration config,
            List<LogicalTypeWithIndex> primaryKeyLogicalTypesWithIndex,
            DataType physicalRowDataType,
            String summaryString,
            ZoneId localTimeZoneId) {
        super(
                config.getBulkFlushMaxActions(),
                config.getBulkFlushMaxInFlightActions(),
                config.getBulkFlushMaxBufferedActions(),
                config.getBulkFlushMaxByteSize().getBytes(),
                config.getBulkFlushInterval());
        this.format = checkNotNull(format);
        this.physicalRowDataType = checkNotNull(physicalRowDataType);
        this.primaryKeyLogicalTypesWithIndex = checkNotNull(primaryKeyLogicalTypesWithIndex);
        this.config = checkNotNull(config);
        this.summaryString = checkNotNull(summaryString);
        this.localTimeZoneId = localTimeZoneId;
        this.isDynamicIndexWithSystemTime = isDynamicIndexWithSystemTime();
    }

    public boolean isDynamicIndexWithSystemTime() {
        IndexGeneratorFactory.IndexHelper indexHelper = new IndexGeneratorFactory.IndexHelper();
        return indexHelper.checkIsDynamicIndexWithSystemTimeFormat(config.getIndex());
    }

    Function<RowData, String> createKeyExtractor() {
        return KeyExtractor.createKeyExtractor(
                primaryKeyLogicalTypesWithIndex, config.getKeyDelimiter());
    }

    IndexGenerator createIndexGenerator() {
        return IndexGeneratorFactory.createIndexGenerator(
                config.getIndex(),
                DataType.getFieldNames(physicalRowDataType),
                DataType.getFieldDataTypes(physicalRowDataType),
                localTimeZoneId);
    }

    Set<String> primaryKeyFieldNames() {
        List<String> fieldNames = DataType.getFieldNames(physicalRowDataType);
        return primaryKeyLogicalTypesWithIndex.stream()
                .map(pk -> fieldNames.get(pk.index))
                .collect(Collectors.toSet());
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        DeleteStrategy deleteStrategy = config.getDeleteStrategy();
        ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        boolean hasUpdateOrDelete = false;
        for (RowKind kind : requestedMode.getContainedKinds()) {
            if (kind == RowKind.UPDATE_BEFORE && deleteStrategy.isIgnoreUpdateBefore()) {
                // UPDATE_BEFORE is dropped at the planner level; DELETE (if ignored) is dropped in
                // front of the sink because the planner does not allow to refuse it.
                continue;
            }
            builder.addContainedKind(kind);
            if (kind != RowKind.INSERT) {
                hasUpdateOrDelete = true;
            }
        }
        if (isDynamicIndexWithSystemTime && !requestedMode.containsOnly(RowKind.INSERT)) {
            throw new ValidationException(
                    "Dynamic indexing based on system time only works on append only stream.");
        }
        if (hasUpdateOrDelete && primaryKeyLogicalTypesWithIndex.isEmpty()) {
            throw new ValidationException(
                    "Primary key is required when the changelog contains UPDATE or DELETE operations.");
        }
        return builder.build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        DeleteStrategy deleteStrategy = config.getDeleteStrategy();
        Elasticsearch8AsyncSinkBuilder<RowData> builder = new Elasticsearch8AsyncSinkBuilder<>();

        builder.setHosts(config.getHosts().toArray(new HttpHost[0]));
        builder.setMaxBatchSize(config.getBulkFlushMaxActions());
        builder.setMaxBufferedRequests(config.getBulkFlushMaxBufferedActions());
        builder.setMaxInFlightRequests(config.getBulkFlushMaxInFlightActions());
        builder.setMaxBatchSizeInBytes(config.getBulkFlushMaxByteSize().getBytes());
        builder.setMaxTimeInBufferMS(config.getBulkFlushInterval());
        // The document body comes from the table format (json by default), so nested types and
        // the format options (e.g. json.timestamp-format.standard) behave like in other sinks.
        SerializationSchema<RowData> documentSerializer =
                format.createRuntimeEncoder(context, physicalRowDataType);
        builder.setElementConverter(
                new RowDataElementConverter(
                        documentSerializer,
                        createIndexGenerator(),
                        createKeyExtractor(),
                        deleteStrategy,
                        primaryKeyFieldNames()));

        config.getUsername()
                .filter(username -> !StringUtils.isNullOrWhitespaceOnly(username))
                .ifPresent(builder::setUsername);

        config.getPassword()
                .filter(password -> !StringUtils.isNullOrWhitespaceOnly(password))
                .ifPresent(builder::setPassword);

        config.getPathPrefix()
                .filter(pathPrefix -> !StringUtils.isNullOrWhitespaceOnly(pathPrefix))
                .ifPresent(builder::setConnectionPathPrefix);

        config.getConnectionRequestTimeout()
                .map(timeout -> (int) timeout.toMillis())
                .ifPresent(builder::setConnectionRequestTimeout);

        config.getConnectionTimeout()
                .map(timeout -> (int) timeout.toMillis())
                .ifPresent(builder::setConnectionTimeout);

        config.getSocketTimeout()
                .map(timeout -> (int) timeout.toMillis())
                .ifPresent(builder::setSocketTimeout);

        config.getCertificateFingerprint().ifPresent(builder::setCertificateFingerprint);

        Elasticsearch8AsyncSink<RowData> sink = builder.build();
        Integer parallelism = config.getParallelism().orElse(null);
        if (deleteStrategy.isIgnoreUpdateBefore() || deleteStrategy.isIgnoreDelete()) {
            return new RowKindFilteringSinkProvider(sink, deleteStrategy, parallelism);
        }
        return SinkV2Provider.of(sink, parallelism);
    }

    @Override
    public DynamicTableSink copy() {
        return new ElasticSearch8AsyncDynamicSink(
                format,
                config,
                primaryKeyLogicalTypesWithIndex,
                physicalRowDataType,
                summaryString,
                localTimeZoneId);
    }

    @Override
    public String asSummaryString() {
        return summaryString;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        ElasticSearch8AsyncDynamicSink that = (ElasticSearch8AsyncDynamicSink) o;
        return Objects.equals(format, that.format)
                && Objects.equals(physicalRowDataType, that.physicalRowDataType)
                && Objects.equals(
                        primaryKeyLogicalTypesWithIndex, that.primaryKeyLogicalTypesWithIndex)
                && Objects.equals(config, that.config)
                && Objects.equals(summaryString, that.summaryString)
                && Objects.equals(localTimeZoneId, that.localTimeZoneId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                super.hashCode(),
                format,
                physicalRowDataType,
                primaryKeyLogicalTypesWithIndex,
                config,
                summaryString,
                localTimeZoneId);
    }

    /**
     * {@link DataStreamSinkProvider} that drops the row kinds ignored by the configured {@link
     * DeleteStrategy} before the rows reach the sink.
     *
     * <p>A {@link ChangelogMode} can only refuse UPDATE_BEFORE; a sink that is asked to consume
     * DELETE rows must accept them. The {@link org.apache.flink.connector.base.sink.AsyncSinkBase}
     * element converter in turn must produce exactly one request per record, so the only place to
     * skip a row is in front of the sink.
     *
     * <p>The filter is also used for strategies that only ignore UPDATE_BEFORE. The changelog mode
     * already asks the planner to drop those rows, but not every planner configuration honours it
     * (Flink 2.3 with {@code table.exec.sink.upsert-materialize = NONE} forwards them), and the
     * converter must never see a row kind it is supposed to ignore.
     */
    @Internal
    static class RowKindFilteringSinkProvider implements DataStreamSinkProvider {
        private final Elasticsearch8AsyncSink<RowData> sink;
        private final DeleteStrategy deleteStrategy;
        @Nullable private final Integer parallelism;

        RowKindFilteringSinkProvider(
                Elasticsearch8AsyncSink<RowData> sink,
                DeleteStrategy deleteStrategy,
                @Nullable Integer parallelism) {
            this.sink = checkNotNull(sink);
            this.deleteStrategy = checkNotNull(deleteStrategy);
            this.parallelism = parallelism;
        }

        @Override
        public DataStreamSink<?> consumeDataStream(
                ProviderContext providerContext, DataStream<RowData> dataStream) {
            // Mirror what the planner does for a SinkV2Provider: the sink runs with the configured
            // parallelism, or with the parallelism of its input if none is configured.
            int sinkParallelism = parallelism != null ? parallelism : dataStream.getParallelism();

            SingleOutputStreamOperator<RowData> filtered =
                    dataStream
                            .filter(new IgnoredRowKindFilter(deleteStrategy))
                            .setParallelism(sinkParallelism)
                            .name("DropIgnoredRowKinds[" + deleteStrategy + "]");
            providerContext
                    .generateUid("elasticsearch8-drop-ignored-row-kinds")
                    .ifPresent(filtered::uid);

            DataStreamSink<RowData> dataStreamSink =
                    filtered.sinkTo(sink).setParallelism(sinkParallelism);
            providerContext.generateUid("elasticsearch8-sink").ifPresent(dataStreamSink::uid);
            return dataStreamSink;
        }

        @Override
        public Optional<Integer> getParallelism() {
            return Optional.ofNullable(parallelism);
        }
    }

    /** Filters out the row kinds ignored by a {@link DeleteStrategy}. */
    @Internal
    static class IgnoredRowKindFilter implements FilterFunction<RowData> {
        private static final long serialVersionUID = 1L;

        private final DeleteStrategy deleteStrategy;

        IgnoredRowKindFilter(DeleteStrategy deleteStrategy) {
            this.deleteStrategy = deleteStrategy;
        }

        @Override
        public boolean filter(RowData row) {
            return !deleteStrategy.ignores(row.getRowKind());
        }
    }

    /** Builder class for {@link ElasticSearch8AsyncDynamicSink}. */
    @Internal
    public static class ElasticSearch8AsyncDynamicSinkBuilder
            extends AsyncDynamicTableSinkBuilder<Operation, ElasticSearch8AsyncDynamicSinkBuilder> {

        DataType physicalRowDataType;
        List<LogicalTypeWithIndex> primaryKeyLogicalTypesWithIndex;
        ZoneId localTimeZoneId;
        String summaryString;
        Elasticsearch8Configuration config;
        EncodingFormat<SerializationSchema<RowData>> format;

        public ElasticSearch8AsyncDynamicSinkBuilder setConfig(Elasticsearch8Configuration config) {
            this.config = config;
            return this;
        }

        public ElasticSearch8AsyncDynamicSinkBuilder setFormat(
                EncodingFormat<SerializationSchema<RowData>> format) {
            this.format = format;
            return this;
        }

        public ElasticSearch8AsyncDynamicSinkBuilder setPhysicalRowDataType(
                DataType physicalRowDataType) {
            this.physicalRowDataType = physicalRowDataType;
            return this;
        }

        public ElasticSearch8AsyncDynamicSinkBuilder setPrimaryKeyLogicalTypesWithIndex(
                List<LogicalTypeWithIndex> primaryKeyLogicalTypesWithIndex) {
            this.primaryKeyLogicalTypesWithIndex = primaryKeyLogicalTypesWithIndex;
            return this;
        }

        public ElasticSearch8AsyncDynamicSinkBuilder setLocalTimeZoneId(ZoneId localTimeZoneId) {
            this.localTimeZoneId = localTimeZoneId;
            return this;
        }

        public ElasticSearch8AsyncDynamicSinkBuilder setSummaryString(String summaryString) {
            this.summaryString = summaryString;
            return this;
        }

        @Override
        public ElasticSearch8AsyncDynamicSink build() {
            return new ElasticSearch8AsyncDynamicSink(
                    format,
                    config,
                    primaryKeyLogicalTypesWithIndex,
                    physicalRowDataType,
                    summaryString,
                    localTimeZoneId);
        }
    }
}
