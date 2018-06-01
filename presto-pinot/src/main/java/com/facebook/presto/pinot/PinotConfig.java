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

import javax.validation.constraints.NotNull;

public class PinotConfig
{
    private static final long DEFAULT_IDLE_TIMEOUT_MS = 6 * 60L * 60 * 1000L; // 6 hours
    private static final int DEFAULT_MIN_CONNECTIONS_PER_SERVER = 10;
    private static final int DEFAULT_MAX_CONNECTIONS_PER_SERVER = 30;
    private static final int DEFAULT_MAX_BACKLOG_PER_SERVER = 30;
    private static final int DEFAULT_THREAD_POOL_SIZE = 30;

    private static final long DEFAULT_LIMIT_ALL = 2147483647;
    private static final long DEFAULT_LIMIT_LARGE = 10000;
    private static final long DEFAULT_LIMIT_MEDIUM = 1000;

    private String zkUrl;
    private String pinotCluster;
    private String controllerUrl;

    private long limitAll = DEFAULT_LIMIT_ALL;
    private long limitLarge = DEFAULT_LIMIT_LARGE;
    private long limitMedium = DEFAULT_LIMIT_MEDIUM;

    private int threadPoolSize = DEFAULT_THREAD_POOL_SIZE;
    private int minConnectionsPerServer = DEFAULT_MIN_CONNECTIONS_PER_SERVER;
    private int maxConnectionsPerServer = DEFAULT_MAX_CONNECTIONS_PER_SERVER;
    private int maxBacklogPerServer = DEFAULT_MAX_BACKLOG_PER_SERVER;
    private long idleTimeoutMs = DEFAULT_IDLE_TIMEOUT_MS;

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

    @NotNull
    public long getIdleTimeoutMs()
    {
        return idleTimeoutMs;
    }

    @Config("idle-timeout-ms")
    public PinotConfig setIdleTimeoutMs(String idleTimeoutMs)
    {
        try {
            this.idleTimeoutMs = Long.valueOf(idleTimeoutMs);
        }
        catch (Exception e) {
            this.idleTimeoutMs = DEFAULT_IDLE_TIMEOUT_MS;
        }
        return this;
    }
}
