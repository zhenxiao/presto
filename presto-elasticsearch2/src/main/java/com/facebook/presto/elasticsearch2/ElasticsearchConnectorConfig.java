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
package com.facebook.presto.elasticsearch2;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.units.Duration;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ElasticsearchConnectorConfig
{
    /**
     * The schema name to use in the connector.
     */
    private String defaultSchema = "default";

    /**
     * Set of tables known to this connector. For each table, a description file may be present in the catalog folder which describes columns for the given topic.
     */
    private Set<String> tableNames = ImmutableSet.of();

    /**
     * Folder holding the JSON description files for Elasticsearch Index.
     */
    private File tableDescriptionDir = new File("etc/elasticsearch2/");

    /**
     * Scroll batch size.
     */
    private int scrollSize = 1000;

    /**
     * Scroll timeout.
     */
    private Duration scrollTime = new Duration(60, TimeUnit.SECONDS);

    /**
     * Max number of hits per Elasticsearch request could fetch.
     */
    private int maxHits = 1000000;

    /**
     * Elasticsearch request timeout.
     */
    private Duration requestTimeout = new Duration(60, TimeUnit.SECONDS);

    @NotNull
    public File getTableDescriptionDir()
    {
        return tableDescriptionDir;
    }

    @Config("elasticsearch2.table-description-dir")
    public ElasticsearchConnectorConfig setTableDescriptionDir(File tableDescriptionDir)
    {
        this.tableDescriptionDir = tableDescriptionDir;
        return this;
    }

    @NotNull
    public Set<String> getTableNames()
    {
        return tableNames;
    }

    @Config("elasticsearch2.table-names")
    public ElasticsearchConnectorConfig setTableNames(String tableNames)
    {
        this.tableNames = ImmutableSet.copyOf(Splitter.on(',').omitEmptyStrings().trimResults().split(tableNames));
        return this;
    }

    @NotNull
    public String getDefaultSchema()
    {
        return defaultSchema;
    }

    @Config("elasticsearch2.default-schema")
    public ElasticsearchConnectorConfig setDefaultSchema(String defaultSchema)
    {
        this.defaultSchema = defaultSchema;
        return this;
    }

    @NotNull
    public int getScrollSize()
    {
        return scrollSize;
    }

    @Config("elasticsearch2.scroll-size")
    public ElasticsearchConnectorConfig setScrollSize(int scrollSize)
    {
        this.scrollSize = scrollSize;
        return this;
    }

    @NotNull
    public Duration getScrollTime()
    {
        return scrollTime;
    }

    @Config("elasticsearch2.scroll-time")
    public ElasticsearchConnectorConfig setScrollTime(Duration scrollTime)
    {
        this.scrollTime = scrollTime;
        return this;
    }

    @NotNull
    public int getMaxHits()
    {
        return maxHits;
    }

    @Config("elasticsearch2.max-hits")
    public ElasticsearchConnectorConfig setMaxHits(int maxHits)
    {
        this.maxHits = maxHits;
        return this;
    }

    @NotNull
    public Duration getRequestTimeout()
    {
        return requestTimeout;
    }

    @Config("elasticsearch2.request-timeout")
    public ElasticsearchConnectorConfig setRequestTimeout(Duration requestTimeout)
    {
        this.requestTimeout = requestTimeout;
        return this;
    }
}
