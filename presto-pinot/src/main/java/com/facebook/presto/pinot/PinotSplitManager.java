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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.predicate.Ranges;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.NestedField;
import com.facebook.presto.spi.type.VarcharType;
import com.linkedin.pinot.client.PinotClientException;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.pinot.Types.checkType;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class PinotSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(PinotSplitManager.class);
    private final String connectorId;
    private final PinotConfig pinotConfig;
    private final PinotConnection pinotPrestoConnection;

    @Inject
    public PinotSplitManager(PinotConnectorId connectorId, PinotConfig pinotConfig, PinotConnection pinotPrestoConnection)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.pinotConfig = pinotConfig;
        this.pinotPrestoConnection = requireNonNull(pinotPrestoConnection, "pinotPrestoConnection is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingStrategy splitSchedulingStrategy,
            Optional<Map<String, NestedField>> nestedFields,
            Optional<Map<String, String>> jsonPaths,
            Optional<Long> limit)
    {
        PinotTableLayoutHandle layoutHandle = checkType(layout, PinotTableLayoutHandle.class, "layout");
        PinotTableHandle tableHandle = layoutHandle.getTable();
        PinotTable table = null;
        PinotColumn timeColumn = null;
        Map<String, Map<String, List<String>>> routingTable = null;
        Map<String, String> timeBoundary = null;
        try {
            table = pinotPrestoConnection.getTable(tableHandle.getTableName());
            timeColumn = pinotPrestoConnection.getPinotTimeColumnForTable(tableHandle.getTableName());
            routingTable = pinotPrestoConnection.GetRoutingTable(tableHandle.getTableName());
            timeBoundary = pinotPrestoConnection.GetTimeBoundary(tableHandle.getTableName());
            // this can happen if table is removed during a query
            checkState(table != null, "Table %s no longer exists", tableHandle.getTableName());
        }
        catch (Exception e) {
            log.error("Failed to fetch table status for Pinot table: %s, Exceptions: %s", tableHandle.getTableName(), e);
            throw new PinotClientException("Failed to fetch table status for Pinot table: " + tableHandle.getTableName(), e);
        }

        List<ConnectorSplit> splits = new ArrayList<>();
        if (!routingTable.isEmpty()) {
            setSplits(splits, timeColumn, routingTable, timeBoundary, getOfflineTableName(tableHandle.getTableName()), tableHandle.getConstraintSummary(), limit);
            setSplits(splits, timeColumn, routingTable, timeBoundary, getRealtimeTableName(tableHandle.getTableName()), tableHandle.getConstraintSummary(), limit);
        }

        Collections.shuffle(splits);
        log.debug("PinotSplits is %s", Arrays.toString(splits.toArray()));

        return new FixedSplitSource(splits);
    }

    private String getTimePredicate(String type, String timeColumn, String maxTimeStamp)
    {
        if ("OFFLINE".equalsIgnoreCase(type)) {
            return String.format("%s < %s", timeColumn, maxTimeStamp);
        }
        if ("REALTIME".equalsIgnoreCase(type)) {
            return String.format("%s >= %s", timeColumn, maxTimeStamp);
        }
        return null;
    }

    private void setSplits(List<ConnectorSplit> splits, PinotColumn timeColumn, Map<String, Map<String, List<String>>> routingTable, Map<String, String> timeBoundary, String tableName, TupleDomain<ColumnHandle> constraintSummary, Optional<Long> optionalLimit)
    {
        String pinotFilter = getSimplePredicate(constraintSummary);
        String timeFilter = "";
        long limit = -1;
        if (optionalLimit.isPresent()) {
            limit = optionalLimit.get();
        }
        if (timeBoundary.containsKey("timeColumnName") && timeBoundary.containsKey("timeColumnValue")) {
            timeFilter = getTimePredicate(getTableType(tableName), timeBoundary.get("timeColumnName"), timeBoundary.get("timeColumnValue"));
        }
        for (String routingTableName : routingTable.keySet()) {
            if (routingTableName.equalsIgnoreCase(tableName)) {
                Map<String, List<String>> hostToSegmentsMap = routingTable.get(routingTableName);
                for (String host : hostToSegmentsMap.keySet()) {
                    for (String segment : hostToSegmentsMap.get(host)) {
                        splits.add(new PinotSplit(connectorId, routingTableName, host, segment, timeColumn, timeFilter, pinotFilter, limit));
                    }
                }
            }
        }
    }

    // TODO(xiangfu): This is very rough predicate pushdown, need to be refined.
    private String getSimplePredicate(TupleDomain<ColumnHandle> constraintSummary)
    {
        String pinotFilter = "";
        Map<ColumnHandle, Domain> columnHandleDomainMap = constraintSummary.getDomains().get();
        for (ColumnHandle k : columnHandleDomainMap.keySet()) {
            String columnName = ((PinotColumnHandle) k).getColumnName();
            String lowValue = null;
            String highValue = null;
            String lowComparator = null;
            String highComparator = null;

            Domain domain = columnHandleDomainMap.get(k);
            ValueSet values = domain.getValues();
            Ranges ranges = values.getRanges();
            Marker low = ranges.getSpan().getLow();
            Marker high = ranges.getSpan().getHigh();

            if (low != null && low.getValueBlock() != null && low.getValueBlock().isPresent()) {
                if (low.getType() instanceof VarcharType) {
                    Block lowBlock = low.getValueBlock().get();
                    Slice slice = lowBlock.getSlice(0, 0, lowBlock.getSliceLength(0));
                    lowValue = slice.toStringUtf8();
                }
                else {
                    lowValue = low.getValue().toString();
                }
                if (low.getBound().toString().equals("EXACTLY")) {
                    lowComparator = "<=";
                }
                else {
                    lowComparator = "<";
                }
            }
            if (high != null && high.getValueBlock() != null && high.getValueBlock().isPresent()) {
                if (high.getType() instanceof VarcharType) {
                    Block highBlock = high.getValueBlock().get();
                    Slice slice = highBlock.getSlice(0, 0, highBlock.getSliceLength(0));
                    highValue = slice.toStringUtf8();
                }
                else {
                    highValue = high.getValue().toString();
                }
                if (high.getBound().toString().equals("EXACTLY")) {
                    highComparator = "<=";
                }
                else {
                    highComparator = "<";
                }
            }
            if (lowValue != null && lowValue.equals(highValue) && lowComparator != null && lowComparator.equals(highComparator)) {
                if (!pinotFilter.isEmpty()) {
                    pinotFilter += " AND ";
                }
                pinotFilter += String.format(" %s = \"%s\" ", columnName, lowValue);
            }
            else {
                if (lowValue != null) {
                    if (!pinotFilter.isEmpty()) {
                        pinotFilter += " AND ";
                    }
                    pinotFilter += String.format(" \"%s\" %s %s ", lowValue, lowComparator, columnName);
                }
                if (highValue != null) {
                    if (!pinotFilter.isEmpty()) {
                        pinotFilter += " AND ";
                    }
                    pinotFilter += String.format(" %s %s \"%s\" ", columnName, highComparator, highValue);
                }
            }
        }
        return pinotFilter;
    }

    private String getOfflineTableName(String table)
    {
        return table + "_OFFLINE";
    }

    private String getRealtimeTableName(String table)
    {
        return table + "_REALTIME";
    }

    private String getTableType(String table)
    {
        if (table.endsWith("_REALTIME")) {
            return "REALTIME";
        }

        if (table.endsWith("_OFFLINE")) {
            return "OFFLINE";
        }

        return null;
    }
}
