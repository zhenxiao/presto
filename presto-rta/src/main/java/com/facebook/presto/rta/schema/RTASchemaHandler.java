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

package com.facebook.presto.rta.schema;

import com.facebook.presto.rta.RtaConfig;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * This class is responsble for top level schema/deployments operations and has a unified view of all info regarding query table, such as
 * - Creating namespace->table->deployments hierarchy
 * - Preloading/making all RTAMS calls
 * - Caching
 */
public class RTASchemaHandler
{
    private static final Logger log = Logger.get(RTASchemaHandler.class);
    private final RtaConfig config;
    private final RTAMSClient client;
    private final Supplier<State> stateSupplier;

    private static class CaseInsensitiveString
            implements Supplier<String>
    {
        private final String string;
        private final String lowerCaseString;

        public CaseInsensitiveString(String string)
        {
            this.string = string;
            this.lowerCaseString = this.string.toLowerCase(ENGLISH);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            CaseInsensitiveString that = o instanceof CaseInsensitiveString ? (CaseInsensitiveString) o : (o instanceof String) ? new CaseInsensitiveString((String) o) : null;
            return that != null && Objects.equals(lowerCaseString, that.lowerCaseString);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(lowerCaseString);
        }

        @Override
        public String get()
        {
            return string;
        }

        public String getLowerCaseString()
        {
            return lowerCaseString;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("lowerCaseString", lowerCaseString)
                    .toString();
        }
    }

    private static class State
    {
        private List<String> allNamespaces;
        private Map<String, Map<CaseInsensitiveString, RTATableEntity>> namespaceToTablesMap;
        private Map<String, List<String>> tablesInNamespace;

        public State(Set<String> allNamespaces, Map<String, Map<CaseInsensitiveString, RTATableEntity>> namespaceToTablesMap, Map<String, List<String>> tablesInNamespace)
        {
            this.allNamespaces = ImmutableList.copyOf(allNamespaces);

            ImmutableMap.Builder<String, Map<CaseInsensitiveString, RTATableEntity>> namespaceToTablesMapBuilder = ImmutableMap.builder();
            namespaceToTablesMap.forEach((namespace, tableMap) -> namespaceToTablesMapBuilder.put(namespace, ImmutableMap.copyOf(tableMap)));
            this.namespaceToTablesMap = namespaceToTablesMapBuilder.build();

            ImmutableMap.Builder<String, List<String>> tablesInNamespaceBuilder = ImmutableMap.builder();
            tablesInNamespace.forEach((namespace, tables) -> tablesInNamespaceBuilder.put(namespace, ImmutableList.copyOf(tables)));
            this.tablesInNamespace = tablesInNamespaceBuilder.build();
        }

        public List<String> getAllNamespaces()
        {
            return allNamespaces;
        }

        public Map<String, Map<CaseInsensitiveString, RTATableEntity>> getNamespaceToTablesMap()
        {
            return namespaceToTablesMap;
        }

        public Map<String, List<String>> getTablesInNamespace()
        {
            return tablesInNamespace;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("allNamespaces", allNamespaces)
                    .add("namespaceToTablesMap", namespaceToTablesMap)
                    .add("tablesInNamespace", tablesInNamespace)
                    .toString();
        }
    }

    private static class TableInNamespaceDetails
    {
        private final RTADefinition definition;
        private List<RTADeployment> deployments;

        public TableInNamespaceDetails(RTADefinition definition, List<RTADeployment> deployments)
        {
            this.definition = definition;
            this.deployments = deployments;
        }
    }

    private State populate()
    {
        State ret;
        try {
            List<RTADefinition> extraDefinitions = client.getExtraDefinitions(config.getExtraDefinitionFiles());
            List<List<RTADeployment>> extraDeployments = client.getExtraDeployments(config.getExtraDeploymentFiles());
            Map<String, Map<CaseInsensitiveString, RTATableEntity>> namespaceToTablesMap = new HashMap<>();
            Set<String> allNamespaces = new HashSet<>(client.getNamespaces().stream().map(s -> s.toLowerCase(ENGLISH)).collect(toImmutableList()));
            Map<SchemaTableName, TableInNamespaceDetails> schemaTableNameMap = new HashMap<>();
            Map<String, List<String>> tablesInNamespace = new HashMap<>();
            for (String namespace : allNamespaces) {
                for (String table : client.getTables(namespace)) {
                    List<RTADeployment> deployments = client.getDeployments(namespace, table);
                    RTADefinition definition = client.getDefinition(namespace, table);
                    schemaTableNameMap.put(new SchemaTableName(namespace, table), new TableInNamespaceDetails(definition, deployments));
                }
            }

            extraDefinitions.forEach(definition -> {
                schemaTableNameMap.put(new SchemaTableName(definition.getNamespace(), definition.getName()), new TableInNamespaceDetails(definition, null));
            });
            extraDeployments.forEach(deployments -> {
                TableInNamespaceDetails tableInNamespaceDetails = schemaTableNameMap.get(new SchemaTableName(deployments.get(0).getNamespace(), deployments.get(0).getName()));
                if (tableInNamespaceDetails != null) {
                    tableInNamespaceDetails.deployments = deployments;
                }
            });
            schemaTableNameMap.forEach((schemaTableName, detail) -> {
                RTATableEntity entity = new RTATableEntity(schemaTableName.getTableName(), detail.deployments, detail.definition);
                CaseInsensitiveString nonCasedTable = new CaseInsensitiveString(schemaTableName.getTableName());
                namespaceToTablesMap.computeIfAbsent(schemaTableName.getSchemaName(), (ignored) -> new HashMap<>()).put(nonCasedTable, entity);
                tablesInNamespace.computeIfAbsent(schemaTableName.getSchemaName(), ignored -> new ArrayList<>()).add(nonCasedTable.getLowerCaseString());
            });
            ret = new State(allNamespaces, namespaceToTablesMap, tablesInNamespace);
        }
        catch (IOException e) {
            throw new RuntimeException("Error when preloading RTA schema state", e);
        }
        log.info("Created Rta schema state " + ret);
        return ret;
    }

    @Inject
    public RTASchemaHandler(RTAMSClient client, RtaConfig config)
    {
        this.client = client;
        this.config = config;
        this.stateSupplier = Suppliers.memoizeWithExpiration(
                () -> populate(),
                (long) config.getMetadataCacheExpiryTime().getValue(TimeUnit.SECONDS),
                TimeUnit.SECONDS);
        this.stateSupplier.get(); // force a prefetch to avoid a query time hit
    }

    public List<String> getAllNamespaces()
    {
        return stateSupplier.get().getAllNamespaces();
    }

    public List<String> getTablesInNamespace(String namespace)
    {
        return requireNonNull(stateSupplier.get().getTablesInNamespace().get(namespace), "Unknown namespace " + namespace);
    }

    public RTATableEntity getEntity(SchemaTableName schemaTableName)
    {
        return getEntity(schemaTableName.getSchemaName(), schemaTableName.getTableName());
    }

    public RTATableEntity getEntity(String namespace, String table)
    {
        RTATableEntity entity = requireNonNull(stateSupplier.get().getNamespaceToTablesMap().get(namespace), "Unknown rta namespace " + namespace).get(new CaseInsensitiveString(table));
        if (entity == null) {
            throw new NoSuchElementException(String.format("Can't find deployments for namespace %s and table %s", namespace, table));
        }
        return entity;
    }
}
