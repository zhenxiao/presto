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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.airlift.http.client.Request;
import io.airlift.http.client.StaticBodyGenerator;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
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

    private static boolean isValidException(JSONObject exception)
    {
        Integer errorCode = exception.getInteger("errorCode");
        return (errorCode == null || !PinotUtils.isValidPinotHttpResponseCode(errorCode));
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
        PinotQueryGenerator.GeneratedPql psql = PinotQueryGenerator.generate(scanPipeline, Optional.of(columnHandles), Optional.empty(), Optional.empty(), Optional.of(pinotConfig));
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

            int counter = issuePqlAndPopulate(psql.getPql(), psql.getNumGroupByClauses(), columnBlockBuilders.build(), columnTypes.build());
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

    private int issuePqlAndPopulate(String psql, int numGroupByClause, List<BlockBuilder> blockBuilders, List<Type> types)
    {
        Request.Builder builder = Request.Builder
                .preparePost()
                .setUri(URI.create(String.format(QUERY_URL_TEMPLATE, pinotConfig.getBrokerUrl())))
                .setBodyGenerator(StaticBodyGenerator.createStaticBodyGenerator(String.format(REQUEST_PAYLOAD_TEMPLATE, psql), StandardCharsets.UTF_8));
        String body = clusterInfoFetcher.doHttpActionWithHeaders(builder, pinotConfig.getBrokerRestService());

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
                if (isValidException(exception)) {
                    throw new PinotException(PinotErrorCode.PINOT_EXCEPTION,
                            Optional.of(psql),
                            String.format("Query %s encountered exception %s", psql, exception));
                }
            }
        }

        JSONArray aggResults = jsonBody.getJSONArray("aggregationResults");

        if (aggResults == null) {
            throw new PinotException(
                    PinotErrorCode.PINOT_UNEXPECTED_RESPONSE,
                    Optional.of(psql),
                    "Expected aggregationResults to be present");
        }

        int rowCount = -1;
        checkState(aggResults.size() == 1, "Expected exactly one metric to be present");
        for (int aggrIndex = 0; aggrIndex < aggResults.size(); aggrIndex++) {
            JSONObject result = aggResults.getJSONObject(aggrIndex);

            JSONArray metricValuesForEachGroup = result.getJSONArray("groupByResult");

            if (metricValuesForEachGroup != null) {
                checkState(numGroupByClause > 0, "Expected having non zero GROUP BY clauses");
                if (aggResults.size() != 1) {
                    throw new PinotException(PinotErrorCode.PINOT_UNEXPECTED_RESPONSE,
                            Optional.of(psql),
                            "We shouldn't have asked for multiple metrics, but we did get " + aggResults.size());
                }
                JSONArray groupByColumns = Preconditions.checkNotNull(result.getJSONArray("groupByColumns"), "groupByColumns missing in %s", psql);
                if (groupByColumns.size() != numGroupByClause) {
                    throw new PinotException(PinotErrorCode.PINOT_UNEXPECTED_RESPONSE,
                            Optional.of(psql),
                            String.format("Expected %d GROUP BY columns but got %s instead from Pinot", numGroupByClause, groupByColumns));
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
                    for (int k = 0; k < group.size(); k++) {
                        setValue(types.get(k), blockBuilders.get(k), group.getString(k));
                    }
                    int metricColumnIndex = numGroupByClause;
                    setValue(types.get(metricColumnIndex), blockBuilders.get(metricColumnIndex), row.getString("value"));
                }

                // There is only one metric, so the row count is the last metric's number's results
                rowCount = metricValuesForEachGroup.size();
            }
            else {
                // simple aggregation
                // TODO: Validate that this is expected semantically
                checkState(numGroupByClause == 0, "Expected no group by columns in pinot");
                setValue(types.get(aggrIndex), blockBuilders.get(aggrIndex), result.getString("value"));
                rowCount = 1;
            }
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
