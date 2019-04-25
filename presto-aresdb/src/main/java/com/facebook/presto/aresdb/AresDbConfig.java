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
package com.facebook.presto.aresdb;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.NotNull;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class AresDbConfig
{
    private String serviceName;
    private String serviceUrl;
    private String metadataServiceName;
    private String metadataServiceUrl;
    private String serviceHeaderParam = "RPC-Service";
    private String callerHeaderValue = "presto";
    private String callerHeaderParam = "RPC-Caller";
    private Map<String, String> extraHttpHeaders = ImmutableMap.of();

    private Duration metadataCacheExpiry = new Duration(1, TimeUnit.DAYS);

    private boolean aggregationPushDownEnabled = true;
    private boolean filterPushDownEnabled = true;
    private boolean projectPushDownEnabled = true;
    private boolean limitPushDownEnabled = true;

    @NotNull
    public String getServiceName()
    {
        return serviceName;
    }

    @Config("service-name")
    public AresDbConfig setServiceName(String serviceName)
    {
        this.serviceName = serviceName;
        return this;
    }

    @NotNull
    public String getServiceUrl()
    {
        return serviceUrl;
    }

    @Config("service-url")
    public AresDbConfig setServiceUrl(String serviceUrl)
    {
        this.serviceUrl = serviceUrl;
        return this;
    }

    @NotNull
    public String getMetadataServiceName()
    {
        return metadataServiceName;
    }

    @Config("metadata-service-name")
    public AresDbConfig setMetadataServiceName(String metadataServiceName)
    {
        this.metadataServiceName = metadataServiceName;
        return this;
    }

    @NotNull
    public String getMetadataServiceUrl()
    {
        return metadataServiceUrl;
    }

    @Config("metadata-service-url")
    public AresDbConfig setMetadataServiceUrl(String metadataServiceUrl)
    {
        this.metadataServiceUrl = metadataServiceUrl;
        return this;
    }

    @NotNull
    public String getServiceHeaderParam()
    {
        return serviceHeaderParam;
    }

    @Config("service-header-param")
    public AresDbConfig setServiceHeaderParam(String serviceHeaderParam)
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
    public AresDbConfig setCallerHeaderValue(String callerHeaderValue)
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
    public AresDbConfig setCallerHeaderParam(String callerHeaderParam)
    {
        this.callerHeaderParam = callerHeaderParam;
        return this;
    }

    @NotNull
    public Map<String, String> getExtraHttpHeaders()
    {
        return extraHttpHeaders;
    }

    @Config("extra-http-headers")
    public AresDbConfig setExtraHttpHeaders(String headers)
    {
        extraHttpHeaders = ImmutableMap.copyOf(Splitter.on(",").trimResults().omitEmptyStrings().withKeyValueSeparator(":").split(headers));
        return this;
    }

    @MinDuration("0s")
    @NotNull
    public Duration getMetadataCacheExpiry()
    {
        return metadataCacheExpiry;
    }

    @Config("metadata-expiry")
    public AresDbConfig setMetadataCacheExpiry(Duration metadataCacheExpiry)
    {
        this.metadataCacheExpiry = metadataCacheExpiry;
        return this;
    }

    public boolean isAggregationPushDownEnabled()
    {
        return aggregationPushDownEnabled;
    }

    @Config("aggregation-pushdown-enabled")
    public AresDbConfig setAggregationPushDownEnabled(boolean aggregationPushDownEnabled)
    {
        this.aggregationPushDownEnabled = aggregationPushDownEnabled;
        return this;
    }

    public boolean isFilterPushDownEnabled()
    {
        return filterPushDownEnabled;
    }

    @Config("filter-pushdown-enabled")
    public AresDbConfig setFilterPushDownEnabled(boolean filterPushDownEnabled)
    {
        this.filterPushDownEnabled = filterPushDownEnabled;
        return this;
    }

    public boolean isProjectPushDownEnabled()
    {
        return projectPushDownEnabled;
    }

    @Config("project-pushdown-enabled")
    public AresDbConfig setProjectPushDownEnabled(boolean projectPushDownEnabled)
    {
        this.projectPushDownEnabled = projectPushDownEnabled;
        return this;
    }

    public boolean isLimitPushDownEnabled()
    {
        return limitPushDownEnabled;
    }

    @Config("limit-pushdown-enabled")
    public AresDbConfig setLimitPushDownEnabled(boolean limitPushDownEnabled)
    {
        this.limitPushDownEnabled = limitPushDownEnabled;
        return this;
    }
}
