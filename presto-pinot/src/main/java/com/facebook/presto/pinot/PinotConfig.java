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

import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

public class PinotConfig
{
    private static final long DEFAULT_IDLE_TIMEOUT_MINUTE = 5L; // 5 minutes
    private static final long DEFAULT_CONNECTION_TIMEOUT_MINUTE = 1L; // 1 minute
    private static final int DEFAULT_MIN_CONNECTIONS_PER_SERVER = 10;
    private static final int DEFAULT_MAX_CONNECTIONS_PER_SERVER = 30;
    private static final int DEFAULT_MAX_BACKLOG_PER_SERVER = 30;
    private static final int DEFAULT_THREAD_POOL_SIZE = 30;
    private static final String DEFAULT_PINOT_CLUSTER_ENV = "adhoc";

    private static final long DEFAULT_LIMIT_ALL = 2147483647;
    private static final long DEFAULT_LIMIT_LARGE = 10000;
    private static final long DEFAULT_LIMIT_MEDIUM = 1000;

    private static final int DEFAULT_ESTIMATED_SIZE_IN_BYTES_FOR_NON_NUMERIC_COLUMN = 20;

    private static final boolean DEFAULT_IS_AGGREGATION_PUSHDOWN_ENABLED = true;

    private String zkUrl;
    private String pinotCluster;
    private String pinotClusterEnv = DEFAULT_PINOT_CLUSTER_ENV;
    private String controllerUrl;

    private long limitAll = DEFAULT_LIMIT_ALL;
    private long limitLarge = DEFAULT_LIMIT_LARGE;
    private long limitMedium = DEFAULT_LIMIT_MEDIUM;

    private Duration idleTimeout = new Duration(DEFAULT_IDLE_TIMEOUT_MINUTE, TimeUnit.MINUTES);
    private Duration connectionTimeout = new Duration(DEFAULT_CONNECTION_TIMEOUT_MINUTE, TimeUnit.MINUTES);

    private int threadPoolSize = DEFAULT_THREAD_POOL_SIZE;
    private int minConnectionsPerServer = DEFAULT_MIN_CONNECTIONS_PER_SERVER;
    private int maxConnectionsPerServer = DEFAULT_MAX_CONNECTIONS_PER_SERVER;
    private int maxBacklogPerServer = DEFAULT_MAX_BACKLOG_PER_SERVER;
    private int estimatedSizeInBytesForNonNumericColumn = DEFAULT_ESTIMATED_SIZE_IN_BYTES_FOR_NON_NUMERIC_COLUMN;
    private boolean isAggregationPushdownEnabled = DEFAULT_IS_AGGREGATION_PUSHDOWN_ENABLED;

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
    public String getPinotClusterEnv()
    {
        return pinotClusterEnv;
    }

    @Config("pinot-cluster-env")
    public PinotConfig setPinotClusterEnv(String pinotClusterEnv)
    {
        if (pinotClusterEnv == null || pinotClusterEnv.length() == 0) {
            pinotClusterEnv = DEFAULT_PINOT_CLUSTER_ENV;
        }
        this.pinotClusterEnv = pinotClusterEnv;
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
    public boolean getIsAggregationPushdownEnabled()
    {
        return isAggregationPushdownEnabled;
    }

    @Config("aggregation-pushdown.enabled")
    public PinotConfig setIsAggregationPushdownEnabled(String isAggregationPushdownEnabled)
    {
        try {
            this.isAggregationPushdownEnabled = Boolean.valueOf(isAggregationPushdownEnabled);
        }
        catch (Exception e) {
            this.isAggregationPushdownEnabled = DEFAULT_IS_AGGREGATION_PUSHDOWN_ENABLED;
        }
        return this;
    }

    @NotNull
    public long getLimitAll()
    {
        return limitAll;
    }

    @Config("limit-all")
    public PinotConfig setLimitAll(String limitAll)
    {
        try {
            this.limitAll = Long.valueOf(limitAll);
        }
        catch (Exception e) {
            this.limitAll = DEFAULT_LIMIT_ALL;
        }
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
    public long getLimitMedium()
    {
        return limitMedium;
    }

    @Config("limit-medium")
    public PinotConfig setLimitMedium(String limitMedium)
    {
        try {
            this.limitMedium = Long.valueOf(limitMedium);
        }
        catch (Exception e) {
            this.limitMedium = DEFAULT_LIMIT_MEDIUM;
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
}
