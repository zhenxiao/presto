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

import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.nio.file.Files.readAllBytes;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

/**
 * Class to get the configured schemaless table descriptions
 */
public class SchemalessTableDescriptionSupplier
        implements Supplier<Map<SchemaTableName, SchemalessTableDescription>>
{
    private static final Logger log = Logger.get(SchemalessTableDescriptionSupplier.class);

    private final JsonCodec<SchemalessTableDescription> tableDescriptionCodec;
    private final File tableDescriptionDir;
    private final String defaultSchema;
    private final Set<String> tableNames;

    @Inject
    SchemalessTableDescriptionSupplier(SchemalessConnectorConfig schemalessConnectorConfig,
                                  JsonCodec<SchemalessTableDescription> tableDescriptionCodec)
    {
        this.tableDescriptionCodec = requireNonNull(tableDescriptionCodec, "tableDescriptionCodec is null");

        requireNonNull(schemalessConnectorConfig, "schemalessConfig is null");
        this.tableDescriptionDir = schemalessConnectorConfig.getTableDescriptionDir();
        this.defaultSchema = schemalessConnectorConfig.getDefaultdataStore();
        this.tableNames = ImmutableSet.copyOf(schemalessConnectorConfig.getTableNames());
    }

    @Override
    public Map<SchemaTableName, SchemalessTableDescription> get()
    {
        ImmutableMap.Builder<SchemaTableName, SchemalessTableDescription> builder = ImmutableMap.builder();

        log.debug("Loading schemaless table definitions from %s", tableDescriptionDir.getAbsolutePath());

        try {
            for (File file : listFiles(tableDescriptionDir)) {
                if (file.isFile() && file.getName().endsWith(".json")) {
                    SchemalessTableDescription table = tableDescriptionCodec.fromJson(readAllBytes(file.toPath()));
                    String dataStoreName = firstNonNull(table.getDataStoreName(), defaultSchema);
                    log.debug("Schemaless table %s.%s: %s", dataStoreName, table.getIndexTableName(), table);
                    builder.put(new SchemaTableName(dataStoreName, table.getIndexTableName()), table);
                }
            }

            Map<SchemaTableName, SchemalessTableDescription> tableDefinitions = builder.build();

            log.debug("Loaded Table definitions: %s", tableDefinitions.keySet());

            builder = ImmutableMap.builder();
            for (String definedTable : tableNames) {
                SchemaTableName tableName;
                try {
                    tableName = parseTableName(definedTable);
                }
                catch (IllegalArgumentException iae) {
                    tableName = new SchemaTableName(defaultSchema, definedTable);
                }

                if (tableDefinitions.containsKey(tableName)) {
                    SchemalessTableDescription schemalessTable = tableDefinitions.get(tableName);
                    log.debug("Found Table definition for %s: %s", tableName, schemalessTable);
                    builder.put(tableName, schemalessTable);
                }
                else {
                    throw new IllegalArgumentException("Missing table definition for: " + tableName);
                }
            }

            return builder.build();
        }
        catch (IOException e) {
            log.warn(e, "Error: ");
            throw Throwables.propagate(e);
        }
    }

    private static List<File> listFiles(File dir)
    {
        if ((dir != null) && dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                log.debug("Considering files: %s", asList(files));
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }

    private static SchemaTableName parseTableName(String schemaTableName)
    {
        checkArgument(!isNullOrEmpty(schemaTableName), "schemaTableName is null or is empty");
        List<String> parts = Splitter.on('.').splitToList(schemaTableName);
        checkArgument(parts.size() == 2, "Invalid schemaTableName: %s", schemaTableName);
        return new SchemaTableName(parts.get(0), parts.get(1));
    }
}
