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

import com.google.inject.Inject;
import com.linkedin.pinot.common.data.Schema;
import io.airlift.log.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class PinotConnection
{
    private static final Logger log = Logger.get(PinotConnection.class);
    private final PinotClusterInfoFetcher pinotClusterInfoFetcher;

    @Inject
    public PinotConnection(PinotClusterInfoFetcher pinotClusterInfoFetcher)
    {
        this.pinotClusterInfoFetcher = pinotClusterInfoFetcher;
    }

    public Map<String, Map<String, List<String>>> GetRoutingTable(String table) throws Exception
    {
        Map<String, Map<String, List<String>>> routingTableForTable = this.pinotClusterInfoFetcher.getRoutingTableForTable(table);
        log.debug("RoutingTable for table: %s is %s", table, Arrays.toString(routingTableForTable.entrySet().toArray()));
        return routingTableForTable;
    }

    public Map<String, String> GetTimeBoundary(String table) throws Exception
    {
        Map<String, String> timeBoundaryForTable = this.pinotClusterInfoFetcher.getTimeBoundaryForTable(table);
        log.debug("TimeBoundary for table: %s is %s", table, Arrays.toString(timeBoundaryForTable.entrySet().toArray()));
        return timeBoundaryForTable;
    }

    public List<String> getTableNames() throws Exception
    {
        return pinotClusterInfoFetcher.getAllTables();
    }

    public PinotTable getTable(String tableName) throws Exception
    {
        List<PinotColumn> columns = getPinotColumnsForTable(tableName);
        return new PinotTable(tableName, columns);
    }

    private List<PinotColumn> getPinotColumnsForTable(String tableName) throws Exception
    {
        Schema tablePinotSchema = pinotClusterInfoFetcher.getTableSchema(tableName);
        return PinotColumnUtils.getPinotColumnsForPinotSchema(tablePinotSchema);
    }

    public PinotColumn getPinotTimeColumnForTable(String tableName) throws Exception
    {
        Schema pinotTableSchema = pinotClusterInfoFetcher.getTableSchema(tableName);
        String columnName = pinotTableSchema.getTimeColumnName();
        return new PinotColumn(columnName, PinotColumnUtils.getPrestoTypeFromPinotType(pinotTableSchema.getFieldSpecFor(columnName)));
    }
}
