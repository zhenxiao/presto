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

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.utils.DataTable;
import io.airlift.log.Logger;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class PinotRecordSet
        implements RecordSet
{
    public static final String SELECT_ALL_QUERY_TEMPLATE = "SELECT %s FROM %s LIMIT %d";
    public static final String SELECT_WITH_PREDICATE_QUERY_TEMPLATE = "SELECT %s FROM %s WHERE %s LIMIT %d";

    private static final Logger log = Logger.get(PinotRecordSet.class);
    private final PinotScatterGatherQueryClient pinotQueryClient;
    private final List<PinotColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final PinotSplit split;

    private final long limitAll;
    private final long limitLarge;
    private final long limitMedium;

    public PinotRecordSet(PinotConfig pinotConfig, PinotScatterGatherQueryClient pinotQueryClient, PinotSplit split, List<PinotColumnHandle> columnHandles)
    {
        requireNonNull(split, "split is null");
        this.pinotQueryClient = pinotQueryClient;
        this.split = split;
        this.columnHandles = requireNonNull(columnHandles, "column handles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (PinotColumnHandle column : columnHandles) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();
        this.limitAll = pinotConfig.getLimitAll();
        this.limitLarge = pinotConfig.getLimitLarge();
        this.limitMedium = pinotConfig.getLimitMedium();
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return getRecordCursor();
    }

    private String getPQL()
    {
        StringBuilder sb = new StringBuilder();
        for (PinotColumnHandle columnHandle : columnHandles) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(columnHandle.getColumnName());
        }
        String pql;
        String pinotPredicate = "";
        if (!split.getPinotFilter().isEmpty()) {
            pinotPredicate += String.format("(%s)", split.getPinotFilter());
        }
        if (!split.getTimeFilter().isEmpty()) {
            if (!pinotPredicate.isEmpty()) {
                pinotPredicate += " AND ";
            }
            pinotPredicate += String.format("(%s)", split.getTimeFilter());
        }
        long limit = limitAll;
        if (split.getLimit() > 0) {
            limit = split.getLimit();
        }
        else {
            if (columnHandles.size() > 5) {
                limit = limitLarge;
            }
            if (columnHandles.size() > 10) {
                limit = limitMedium;
            }
        }
        if (pinotPredicate.isEmpty()) {
            pql = String.format(SELECT_ALL_QUERY_TEMPLATE, sb.toString(), split.getTableName(), limit);
        }
        else {
            pql = String.format(SELECT_WITH_PREDICATE_QUERY_TEMPLATE, sb.toString(), split.getTableName(), pinotPredicate, limit);
        }
        log.debug("Plan to send PQL : %s", pql);
        return pql;
    }

    public RecordCursor getRecordCursor()
    {
        Map<ServerInstance, DataTable> dataTableMap = pinotQueryClient.queryPinotServerForDataTable(getPQL(), split.getHost(), split.getSegment());

        return new PinotRecordCursor(columnHandles, dataTableMap);
    }
}
