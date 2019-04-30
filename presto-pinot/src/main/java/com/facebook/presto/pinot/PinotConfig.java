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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.NotNull;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PinotConfig
{
    public static final long DEFAULT_LIMIT_LARGE = 1_000_000;

    private static final long DEFAULT_IDLE_TIMEOUT_MINUTE = 5L; // 5 minutes
    private static final long DEFAULT_CONNECTION_TIMEOUT_MINUTE = 1L; // 1 minute
    private static final int DEFAULT_MIN_CONNECTIONS_PER_SERVER = 10;
    private static final int DEFAULT_MAX_CONNECTIONS_PER_SERVER = 30;
    private static final int DEFAULT_MAX_BACKLOG_PER_SERVER = 30;
    private static final int DEFAULT_THREAD_POOL_SIZE = 30;

    private static final int DEFAULT_ESTIMATED_SIZE_IN_BYTES_FOR_NON_NUMERIC_COLUMN = 20;

    private String zkUrl;
    private String pinotCluster;
    private String controllerRestService;
    private String serviceHeaderParam = "RPC-Service";
    private String callerHeaderValue = "presto";
    private String callerHeaderParam = "RPC-Caller";

    private String controllerUrl;

    private long limitLarge = DEFAULT_LIMIT_LARGE;

    private Duration idleTimeout = new Duration(DEFAULT_IDLE_TIMEOUT_MINUTE, TimeUnit.MINUTES);
    private Duration connectionTimeout = new Duration(DEFAULT_CONNECTION_TIMEOUT_MINUTE, TimeUnit.MINUTES);

    private int threadPoolSize = DEFAULT_THREAD_POOL_SIZE;
    private int minConnectionsPerServer = DEFAULT_MIN_CONNECTIONS_PER_SERVER;
    private int maxConnectionsPerServer = DEFAULT_MAX_CONNECTIONS_PER_SERVER;
    private int maxBacklogPerServer = DEFAULT_MAX_BACKLOG_PER_SERVER;
    private int estimatedSizeInBytesForNonNumericColumn = DEFAULT_ESTIMATED_SIZE_IN_BYTES_FOR_NON_NUMERIC_COLUMN;
    private Map<String, String> extraHttpHeaders = ImmutableMap.of();
    private Duration metadataCacheExpiry = new Duration(2, TimeUnit.MINUTES);

    private boolean aggregationPushDownEnabled = true;
    private boolean filterPushDownEnabled = true;
    private boolean projectPushDownEnabled = true;
    private boolean limitPushDownEnabled = true;

    private boolean allowMultipleAggregations;
    private long maxSelectLimitWhenSinglePage = 1_000;
    private boolean scanParallelismEnabled = true;
    private boolean queryUsingController;

    @NotNull
    public String getZkUrl()
    {
        return zkUrl;
    }

    @Config("zk-uri")
    public PinotConfig setZkUrl(String zkUrl)
    {
        if (zkUrl != null && zkUrl.endsWith("/")) {
            zkUrl = zkUrl.substring(0, zkUrl.length() - 1);
        }
        this.zkUrl = zkUrl;
        return this;
    }

    @NotNull
    public String getPinotCluster()
    {
        return pinotCluster;
    }

    @Config("pinot-cluster")
    public PinotConfig setPinotCluster(String pinotCluster)
    {
        this.pinotCluster = pinotCluster;
        return this;
    }

    @NotNull
    public Map<String, String> getExtraHttpHeaders()
    {
        return extraHttpHeaders;
    }

    @Config("extra-http-headers")
    public PinotConfig setExtraHttpHeaders(String headers)
    {
        extraHttpHeaders = ImmutableMap.copyOf(Splitter.on(",").trimResults().omitEmptyStrings().withKeyValueSeparator(":").split(headers));
        return this;
    }

    @NotNull
    public String getControllerUrl()
    {
        return controllerUrl;
    }

    @Config("controller-url")
    public PinotConfig setControllerUrl(String controllerUrl)
    {
        this.controllerUrl = controllerUrl;
        return this;
    }

    @NotNull
    public String getControllerRestService()
    {
        return controllerRestService;
    }

    @Config("controller-rest-service")
    public PinotConfig setControllerRestService(String controllerRestService)
    {
        this.controllerRestService = controllerRestService;
        return this;
    }

    @NotNull
    public boolean isAllowMultipleAggregations()
    {
        return allowMultipleAggregations;
    }

    @Config("allow-multiple-aggregations")
    public PinotConfig setAllowMultipleAggregations(boolean allowMultipleAggregations)
    {
        this.allowMultipleAggregations = allowMultipleAggregations;
        return this;
    }

    @NotNull
    public long getLimitLarge()
    {
        return limitLarge;
    }

    @Config("limit-large")
    public PinotConfig setLimitLarge(String limitLarge)
    {
        try {
            this.limitLarge = Long.valueOf(limitLarge);
        }
        catch (Exception e) {
            this.limitLarge = DEFAULT_LIMIT_LARGE;
        }
        return this;
    }

    @NotNull
    public int getThreadPoolSize()
    {
        return threadPoolSize;
    }

    @Config("thread-pool-size")
    public PinotConfig setThreadPoolSize(String threadPoolSize)
    {
        try {
            this.threadPoolSize = Integer.valueOf(threadPoolSize);
        }
        catch (Exception e) {
            this.threadPoolSize = DEFAULT_THREAD_POOL_SIZE;
        }
        return this;
    }

    @NotNull
    public int getMinConnectionsPerServer()
    {
        return minConnectionsPerServer;
    }

    @Config("min-connections-per-server")
    public PinotConfig setMinConnectionsPerServer(String minConnectionsPerServer)
    {
        try {
            this.minConnectionsPerServer = Integer.valueOf(minConnectionsPerServer);
        }
        catch (Exception e) {
            this.minConnectionsPerServer = DEFAULT_MIN_CONNECTIONS_PER_SERVER;
        }
        return this;
    }

    @NotNull
    public int getMaxConnectionsPerServer()
    {
        return maxConnectionsPerServer;
    }

    @Config("max-connections-per-server")
    public PinotConfig setMaxConnectionsPerServer(String maxConnectionsPerServer)
    {
        try {
            this.maxConnectionsPerServer = Integer.valueOf(maxConnectionsPerServer);
        }
        catch (Exception e) {
            this.maxConnectionsPerServer = DEFAULT_MAX_CONNECTIONS_PER_SERVER;
        }
        return this;
    }

    @NotNull
    public int getMaxBacklogPerServer()
    {
        return maxBacklogPerServer;
    }

    @Config("max-backlog-per-server")
    public PinotConfig setMaxBacklogPerServer(String maxBacklogPerServer)
    {
        try {
            this.maxBacklogPerServer = Integer.valueOf(maxBacklogPerServer);
        }
        catch (Exception e) {
            this.maxBacklogPerServer = DEFAULT_MAX_BACKLOG_PER_SERVER;
        }
        return this;
    }

    @MinDuration("15s")
    @NotNull
    public Duration getIdleTimeout()
    {
        return idleTimeout;
    }

    @Config("idle-timeout")
    public PinotConfig setIdleTimeout(Duration idleTimeout)
    {
        this.idleTimeout = idleTimeout;
        return this;
    }

    @MinDuration("15s")
    @NotNull
    public Duration getConnectionTimeout()
    {
        return connectionTimeout;
    }

    @Config("connection-timeout")
    public PinotConfig setConnectionTimeout(Duration connectionTimeout)
    {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    @MinDuration("0s")
    @NotNull
    public Duration getMetadataCacheExpiry()
    {
        return metadataCacheExpiry;
    }

    @Config("metadata-expiry")
    public PinotConfig setMetadataCacheExpiry(Duration metadataCacheExpiry)
    {
        this.metadataCacheExpiry = metadataCacheExpiry;
        return this;
    }

    @NotNull
    public int getEstimatedSizeInBytesForNonNumericColumn()
    {
        return estimatedSizeInBytesForNonNumericColumn;
    }

    @Config("estimated-size-in-bytes-for-non-numeric-column")
    public PinotConfig setEstimatedSizeInBytesForNonNumericColumn(int estimatedSizeInBytesForNonNumericColumn)
    {
        try {
            this.estimatedSizeInBytesForNonNumericColumn = Integer.valueOf(estimatedSizeInBytesForNonNumericColumn);
        }
        catch (Exception e) {
            this.estimatedSizeInBytesForNonNumericColumn = DEFAULT_ESTIMATED_SIZE_IN_BYTES_FOR_NON_NUMERIC_COLUMN;
        }
        return this;
    }

    @NotNull
    public String getServiceHeaderParam()
    {
        return serviceHeaderParam;
    }

    @Config("service-header-param")
    public PinotConfig setServiceHeaderParam(String serviceHeaderParam)
    {
        this.serviceHeaderParam = serviceHeaderParam;
        return this;
    }

    @NotNull
    public String getCallerHeaderValue()
    {
        return callerHeaderValue;
    }

    @Config("caller-header-value")
    public PinotConfig setCallerHeaderValue(String callerHeaderValue)
    {
        this.callerHeaderValue = callerHeaderValue;
        return this;
    }

    @NotNull
    public String getCallerHeaderParam()
    {
        return callerHeaderParam;
    }

    @Config("caller-header-param")
    public PinotConfig setCallerHeaderParam(String callerHeaderParam)
    {
        this.callerHeaderParam = callerHeaderParam;
        return this;
    }

    public boolean isAggregationPushDownEnabled()
    {
        return aggregationPushDownEnabled;
    }

    @Config("aggregation-pushdown-enabled")
    public PinotConfig setAggregationPushDownEnabled(boolean aggregationPushDownEnabled)
    {
        this.aggregationPushDownEnabled = aggregationPushDownEnabled;
        return this;
    }

    public boolean isFilterPushDownEnabled()
    {
        return filterPushDownEnabled;
    }

    @Config("filter-pushdown-enabled")
    public PinotConfig setFilterPushDownEnabled(boolean filterPushDownEnabled)
    {
        this.filterPushDownEnabled = filterPushDownEnabled;
        return this;
    }

    public boolean isProjectPushDownEnabled()
    {
        return projectPushDownEnabled;
    }

    @Config("project-pushdown-enabled")
    public PinotConfig setProjectPushDownEnabled(boolean projectPushDownEnabled)
    {
        this.projectPushDownEnabled = projectPushDownEnabled;
        return this;
    }

    public boolean isLimitPushDownEnabled()
    {
        return limitPushDownEnabled;
    }

    @Config("limit-pushdown-enabled")
    public PinotConfig setLimitPushDownEnabled(boolean limitPushDownEnabled)
    {
        this.limitPushDownEnabled = limitPushDownEnabled;
        return this;
    }

    public long getMaxSelectLimitWhenSinglePage()
    {
        return maxSelectLimitWhenSinglePage;
    }

    @Config("max-select-limit-when-single-page")
    public PinotConfig setMaxSelectLimitWhenSinglePage(long maxSelectLimitWhenSinglePage)
    {
        this.maxSelectLimitWhenSinglePage = maxSelectLimitWhenSinglePage;
        return this;
    }

    public boolean isScanParallelismEnabled()
    {
        return scanParallelismEnabled;
    }

    @Config("scan-parallelism-enabled")
    public PinotConfig setScanParallelismEnabled(boolean scanParallelismEnabled)
    {
        this.scanParallelismEnabled = scanParallelismEnabled;
        return this;
    }

    public boolean isQueryUsingController()
    {
        return queryUsingController;
    }

    @Config("query-using-controller")
    public PinotConfig setQueryUsingController(boolean queryUsingController)
    {
        this.queryUsingController = queryUsingController;
        return this;
    }
}
