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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import com.linkedin.pinot.common.data.Schema;
import io.airlift.log.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class PinotConnection
{
    private static final Logger log = Logger.get(PinotConnection.class);

    private final LoadingCache<String, List<PinotColumn>> pinotTableColumnCache;
    private final LoadingCache<String, PinotTable> pinotTableCache;
    private final Supplier<List<String>> allTablesCache;
    private final PinotClusterInfoFetcher pinotClusterInfoFetcher;

    @Inject
    public PinotConnection(PinotClusterInfoFetcher pinotClusterInfoFetcher, PinotConfig pinotConfig)
    {
        final long cacheExpiryMs = pinotConfig.getMetadataCacheExpiry().roundTo(TimeUnit.MILLISECONDS);
        this.pinotClusterInfoFetcher = requireNonNull(pinotClusterInfoFetcher, "cluster info fetcher is null");
        this.allTablesCache = Suppliers.memoizeWithExpiration(
                () -> pinotClusterInfoFetcher.getAllTables(),
                cacheExpiryMs,
                TimeUnit.MILLISECONDS);

        this.pinotTableCache =
                CacheBuilder.newBuilder()
                        .expireAfterWrite(cacheExpiryMs, TimeUnit.MILLISECONDS)
                        .build(new CacheLoader<String, PinotTable>()
                        {
                            @Override
                            public PinotTable load(String tableName)
                                    throws Exception
                            {
                                List<PinotColumn> columns = getPinotColumnsForTable(tableName);
                                return new PinotTable(tableName, columns);
                            }
                        });

        this.pinotTableColumnCache =
                CacheBuilder.newBuilder()
                        .expireAfterWrite(cacheExpiryMs, TimeUnit.MILLISECONDS)
                        .build(new CacheLoader<String, List<PinotColumn>>()
                        {
                            @Override
                            public List<PinotColumn> load(String tableName)
                                    throws Exception
                            {
                                Schema tablePinotSchema = pinotClusterInfoFetcher.getTableSchema(tableName);
                                return PinotColumnUtils.getPinotColumnsForPinotSchema(tablePinotSchema);
                            }
                        });
    }

    public List<String> getTableNames()
    {
        return allTablesCache.get();
    }

    public PinotTable getTable(String tableName)
    {
        try {
            return pinotTableCache.get(tableName);
        }
        catch (Exception e) {
            throw new PinotException(PinotErrorCode.PINOT_UNCLASSIFIED_ERROR, Optional.empty(), "Error when getting table " + tableName, e);
        }
    }

    private List<PinotColumn> getPinotColumnsForTable(String tableName)
            throws Exception
    {
        return pinotTableColumnCache.get(tableName);
    }

    public Map<String, Map<String, List<String>>> getRoutingTable(String tableName)
            throws Exception
    {
        return pinotClusterInfoFetcher.getRoutingTableForTable(tableName);
    }

    public Map<String, String> getTimeBoundary(String tableName)
    {
        return pinotClusterInfoFetcher.getTimeBoundaryForTable(tableName);
    }
}
