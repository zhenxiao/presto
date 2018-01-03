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
package com.facebook.presto.schemaless;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.Set;

/**
 * TODO Add more properties as needed later.
 */
public class SchemalessConnectorConfig
{
    /**
     * The datastore name to use in the connector.
     */
    private String defaultdataStore = "default";

    /**
     * Set of tables known to this connector. For each table, a description file may be present in the catalog folder which describes columns for the given Schemaless index table.
     */
    private Set<String> tableNames = ImmutableSet.of();

    /**
     * Folder holding the JSON description files for Schemaless Index tables.
     */
    private File tableDescriptionDir = new File("etc/schemaless/");

    /**
     * Client ID prefix to be used as tags for all presto schemaless connector calls
     */
    private String defaultClientIDPrefix = "presto-schemaless";

    /**
     * Time out to be used for all Schemaless network calls.
     */
    private int defaultTimeOutInMs = 700;

    @NotNull
    public String getDefaultdataStore()
    {
        return defaultdataStore;
    }

    @Config("schemaless.default-datastore")
    public SchemalessConnectorConfig setDefaultdataStore(String defaultdataStore)
    {
        this.defaultdataStore = defaultdataStore;
        return this;
    }

    @NotNull
    public Set<String> getTableNames()
    {
        return tableNames;
    }

    @Config("schemaless.table-names")
    public SchemalessConnectorConfig setTableNames(String tableNames)
    {
        this.tableNames = ImmutableSet.copyOf(Splitter.on(',').omitEmptyStrings().trimResults().split(tableNames));
        return this;
    }

    @NotNull
    public File getTableDescriptionDir()
    {
        return tableDescriptionDir;
    }

    @Config("schemaless.table-description-dir")
    public SchemalessConnectorConfig setTableDescriptionDir(File tableDescriptionDir)
    {
        this.tableDescriptionDir = tableDescriptionDir;
        return this;
    }

    @NotNull
    public String getDefaultClientIDPrefix()
    {
        return defaultClientIDPrefix;
    }

    @Config("schemaless.default-client-id-prefix")
    public SchemalessConnectorConfig setDefaultClientIDPrefix(String defaultClientIDPrefix)
    {
        this.defaultClientIDPrefix = defaultClientIDPrefix;
        return this;
    }

    @NotNull
    public int getDefaultTimeOutInMs()
    {
        return defaultTimeOutInMs;
    }

    @Config("schemaless.default-timeout-ms")
    public SchemalessConnectorConfig setDefaultTimeOutInMs(int defaultTimeOutInMs)
    {
        this.defaultTimeOutInMs = defaultTimeOutInMs;
        return this;
    }
}
