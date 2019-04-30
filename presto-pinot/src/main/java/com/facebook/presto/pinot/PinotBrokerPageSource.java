/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.pinot;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.facebook.presto.pinot.query.PinotQueryGenerator;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.pipeline.TableScanPipeline;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.TinyintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.airlift.http.client.Request;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;

public class PinotBrokerPageSource
        implements ConnectorPageSource
{
    private static final String REQUEST_PAYLOAD_TEMPLATE = "{\"pql\" : \"%s\" }";
    private static final String QUERY_URL_TEMPLATE = "http://%s/query";

    private final TableScanPipeline scanPipeline;
    private final PinotConfig pinotConfig;
    private final List<PinotColumnHandle> columnHandles;
    private final PinotClusterInfoFetcher clusterInfoFetcher;

    private boolean finished;
    private long readTimeNanos;

    public PinotBrokerPageSource(PinotConfig pinotConfig, TableScanPipeline scanPipeline, List<PinotColumnHandle> columnHandles, PinotClusterInfoFetcher clusterInfoFetcher)
    {
        this.pinotConfig = pinotConfig;
        this.scanPipeline = scanPipeline;
        this.clusterInfoFetcher = clusterInfoFetcher;
        this.columnHandles = ImmutableList.copyOf(columnHandles);
    }

    private static void setValue(Type type, BlockBuilder blockBuilder, String value)
    {
        if (value == null) {
            blockBuilder.appendNull();
            return;
        }

        if (type instanceof BigintType) {
            type.writeLong(blockBuilder, Double.valueOf(value).longValue());
        }
        else if (type instanceof IntegerType) {
            blockBuilder.writeInt(Double.valueOf(value).intValue());
        }
        else if (type instanceof TinyintType) {
            blockBuilder.writeByte(Double.valueOf(value).byteValue());
        }
        else if (type instanceof SmallintType) {
            blockBuilder.writeShort(Double.valueOf(value).shortValue());
        }
        else if (type instanceof BooleanType) {
            type.writeBoolean(blockBuilder, Boolean.valueOf(value));
        }
        else if (type instanceof DecimalType || type instanceof DoubleType) {
            type.writeDouble(blockBuilder, Double.valueOf(value));
        }
        else if (type instanceof TimestampType) {
            type.writeLong(blockBuilder, Long.valueOf(value));
        }
        else if (type instanceof VarcharType) {
            Slice slice = Slices.utf8Slice(value);
            blockBuilder.writeBytes(slice, 0, slice.length()).closeEntry();
        }
        else {
            throw new PinotException(PinotErrorCode.PINOT_UNSUPPORTED_COLUMN_TYPE, Optional.empty(), "type '" + type + "' not supported");
        }
    }

    private static void setValuesForGroupby(List<BlockBuilder> blockBuilders, List<Type> types, int numGroupByClause, JSONArray group, String[] values)
    {
        for (int k = 0; k < group.size(); k++) {
            setValue(types.get(k), blockBuilders.get(k), group.getString(k));
        }
        for (int aggrIndex = 0; aggrIndex < values.length; ++aggrIndex) {
            int metricColumnIndex = aggrIndex + numGroupByClause;
            if (metricColumnIndex < blockBuilders.size()) {
                setValue(types.get(metricColumnIndex), blockBuilders.get(metricColumnIndex), values[aggrIndex]);
            }
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public Page getNextPage()
    {
        if (finished) {
            return null;
        }

        long start = System.nanoTime();
        PinotQueryGenerator.GeneratedPql psql = PinotQueryGenerator.generateForSingleBrokerRequest(scanPipeline, Optional.of(columnHandles), Optional.of(pinotConfig));
        try {
            List<Type> expectedTypes = columnHandles.stream().map(PinotColumnHandle::getDataType).collect(Collectors.toList());
            PageBuilder pageBuilder = new PageBuilder(expectedTypes);
            ImmutableList.Builder<BlockBuilder> columnBlockBuilders = ImmutableList.builder();
            ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
            for (int idx : psql.getColumnIndicesExpected().get()) {
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(idx);
                columnBlockBuilders.add(blockBuilder);
                columnTypes.add(expectedTypes.get(idx));
            }

            int counter = issuePqlAndPopulate(psql.getTable(), psql.getPql(), psql.getNumGroupByClauses(), columnBlockBuilders.build(), columnTypes.build());
            pageBuilder.declarePositions(counter);
            Page page = pageBuilder.build();

            // TODO: Implement chunking if the result set is ginormous
            finished = true;

            return page;
        }
        finally {
            readTimeNanos += System.nanoTime() - start;
        }
    }

    private int issuePqlAndPopulate(String table, String psql, int numGroupByClause, List<BlockBuilder> blockBuilders, List<Type> types)
    {
        String queryHost;
        Optional<String> rpcService;
        if (pinotConfig.isQueryUsingController()) {
            queryHost = pinotConfig.getControllerUrl();
            rpcService = Optional.of(pinotConfig.getControllerRestService());
        }
        else {
            queryHost = clusterInfoFetcher.getBrokerHost(table);
            rpcService = Optional.empty();
        }
        Request.Builder builder = Request.Builder
                .preparePost()
                .setUri(URI.create(String.format(QUERY_URL_TEMPLATE, queryHost)));
        String body = clusterInfoFetcher.doHttpActionWithHeaders(builder, Optional.of(String.format(REQUEST_PAYLOAD_TEMPLATE, psql)), rpcService);
        return populateFromPqlResults(psql, numGroupByClause, blockBuilders, types, body);
    }

    @VisibleForTesting
    public static int populateFromPqlResults(String psql, int numGroupByClause, List<BlockBuilder> blockBuilders, List<Type> types, String body)
    {
        JSONObject jsonBody = JSONObject.parseObject(body);

        Integer numServersResponded = jsonBody.getInteger("numServersResponded");
        Integer numServersQueried = jsonBody.getInteger("numServersQueried");

        if (numServersQueried == null || numServersResponded == null || numServersQueried > numServersResponded) {
            throw new PinotException(
                    PinotErrorCode.PINOT_INSUFFICIENT_SERVER_RESPONSE,
                    Optional.of(psql),
                    String.format("Only %s out of %s servers responded for query %s", numServersResponded, numServersQueried, psql));
        }

        JSONArray exceptions = jsonBody.getJSONArray("exceptions");
        if (exceptions != null) {
            for (int i = 0; i < exceptions.size(); ++i) {
                JSONObject exception = exceptions.getJSONObject(i);
                // Pinot is known to return exceptions with benign errorcodes like 200
                // so we treat any exception as an error
                throw new PinotException(PinotErrorCode.PINOT_EXCEPTION,
                        Optional.of(psql),
                        String.format("Query %s encountered exception %s", psql, exception));
            }
        }

        JSONArray aggResults = jsonBody.getJSONArray("aggregationResults");
        JSONObject selectionResults = jsonBody.getJSONObject("selectionResults");

        int rowCount;
        if (aggResults != null) {
            // This is map is populated only when we have multiple aggregates with a group by
            Preconditions.checkState(aggResults.size() >= 1, "Expected atleast one metric to be present");
            Map<JSONArray, String[]> groupToValue = aggResults.size() == 1 || numGroupByClause == 0 ? null : new HashMap<>();
            rowCount = 0;
            String[] singleAgg = new String[1];
            Boolean sawGroupByResult = null;
            int actualNumColumns = aggResults.size() + numGroupByClause;
            if (actualNumColumns != blockBuilders.size()) {
                throw new PinotException(PinotErrorCode.PINOT_UNEXPECTED_RESPONSE,
                        Optional.of(psql),
                        String.format("Expected %d columns but got %s instead from pinot", blockBuilders.size(), actualNumColumns));
            }
            for (int aggrIndex = 0; aggrIndex < aggResults.size(); aggrIndex++) {
                JSONObject result = aggResults.getJSONObject(aggrIndex);

                JSONArray metricValuesForEachGroup = result.getJSONArray("groupByResult");

                if (metricValuesForEachGroup != null) {
                    Preconditions.checkState(sawGroupByResult == null || sawGroupByResult);
                    sawGroupByResult = true;
                    Preconditions.checkState(numGroupByClause > 0, "Expected having non zero group by clauses");
                    JSONArray groupByColumns = Preconditions.checkNotNull(result.getJSONArray("groupByColumns"), "groupByColumns missing in %s", psql);
                    if (groupByColumns.size() != numGroupByClause) {
                        throw new PinotException(PinotErrorCode.PINOT_UNEXPECTED_RESPONSE,
                                Optional.of(psql),
                                String.format("Expected %d gby columns but got %s instead from pinot", numGroupByClause, groupByColumns));
                    }
                    // group by aggregation
                    for (int groupByIndex = 0; groupByIndex < metricValuesForEachGroup.size(); groupByIndex++) {
                        JSONObject row = metricValuesForEachGroup.getJSONObject(groupByIndex);
                        JSONArray group = row.getJSONArray("group");
                        if (group == null || group.size() != numGroupByClause) {
                            throw new PinotException(PinotErrorCode.PINOT_UNEXPECTED_RESPONSE,
                                    Optional.of(psql),
                                    String.format("Expected %d group by columns but got only a group of size %d (%s)", numGroupByClause, group.size(), group));
                        }
                        if (groupToValue == null) {
                            singleAgg[0] = row.getString("value");
                            setValuesForGroupby(blockBuilders, types, numGroupByClause, group, singleAgg);
                            ++rowCount;
                        }
                        else {
                            groupToValue.computeIfAbsent(group, (ignored) -> new String[aggResults.size()])[aggrIndex] = row.getString("value");
                        }
                    }
                }
                else {
                    Preconditions.checkState(sawGroupByResult == null || !sawGroupByResult);
                    sawGroupByResult = false;
                    // simple aggregation
                    // TODO: Validate that this is expected semantically
                    Preconditions.checkState(numGroupByClause == 0, "Expected no group by columns in pinot");
                    setValue(types.get(aggrIndex), blockBuilders.get(aggrIndex), result.getString("value"));
                    rowCount = 1;
                }
            }

            if (groupToValue != null) {
                Preconditions.checkState(rowCount == 0, "Row count shouldn't have changed from zero");
                groupToValue.forEach((group, values) ->
                        setValuesForGroupby(blockBuilders, types, numGroupByClause, group, values));
                rowCount = groupToValue.size();
            }
        }
        else if (selectionResults != null) {
            JSONArray columns = selectionResults.getJSONArray("columns");
            JSONArray results = selectionResults.getJSONArray("results");
            if (columns == null || results == null || columns.size() != blockBuilders.size()) {
                throw new PinotException(
                        PinotErrorCode.PINOT_UNEXPECTED_RESPONSE,
                        Optional.of(psql),
                        String.format("Columns and results expected for %s, expected %d columns but got %d", psql, blockBuilders.size(), columns == null ? 0 : columns.size()));
            }
            for (int rowNumber = 0; rowNumber < results.size(); ++rowNumber) {
                JSONArray result = results.getJSONArray(rowNumber);
                if (result == null || result.size() != blockBuilders.size()) {
                    throw new PinotException(
                            PinotErrorCode.PINOT_UNEXPECTED_RESPONSE,
                            Optional.of(psql),
                            String.format("Expected row of %d columns", blockBuilders.size()));
                }
                for (int columnNumber = 0; columnNumber < blockBuilders.size(); ++columnNumber) {
                    setValue(types.get(columnNumber), blockBuilders.get(columnNumber), result.getString(columnNumber));
                }
            }
            rowCount = results.size();
        }
        else {
            throw new PinotException(
                    PinotErrorCode.PINOT_UNEXPECTED_RESPONSE,
                    Optional.of(psql),
                    "Expected one of aggregationResults or selectionResults to be present");
        }

        checkState(rowCount >= 0, "Expected row count to be initialized");
        return rowCount;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
    {
        finished = true;
    }
}
