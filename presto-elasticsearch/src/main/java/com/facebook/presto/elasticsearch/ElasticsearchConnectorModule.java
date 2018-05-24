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
package com.facebook.presto.elasticsearch;

import com.facebook.presto.decoder.DecoderModule;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.airlift.json.JsonCodec;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static java.nio.file.Files.readAllBytes;
import static java.util.Objects.requireNonNull;

public class ElasticsearchConnectorModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(ElasticsearchConnector.class).in(Scopes.SINGLETON);
        binder.bind(ElasticsearchMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ElasticsearchSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ElasticsearchRecordSetProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(ElasticsearchConnectorConfig.class);

        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
        jsonCodecBinder(binder).bindJsonCodec(ElasticsearchTableDescription.class);

        binder.install(new DecoderModule());
    }

    @Singleton
    @Provides
    public static ElasticsearchClient createElasticsearchClient(ElasticsearchConnectorConfig config, JsonCodec<ElasticsearchTableDescription> tableDescriptionCodec)
    {
        ImmutableMap.Builder<SchemaTableName, ElasticsearchTableDescription> builder = ImmutableMap.builder();
        try {
            for (File file : listFiles(config.getTableDescriptionDir())) {
                if (file.isFile() && file.getName().endsWith(".json")) {
                    ElasticsearchTableDescription table = tableDescriptionCodec.fromJson(readAllBytes(file.toPath()));
                    String schemaName = firstNonNull(table.getSchemaName(), config.getDefaultSchema());
                    builder.put(new SchemaTableName(schemaName, table.getTableName()), table);
                }
            }
            return buildClient(config, builder.build());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static ElasticsearchClient buildClient(ElasticsearchConnectorConfig config, Map<SchemaTableName, ElasticsearchTableDescription> tableDefinitions)
            throws IOException
    {
        ImmutableMap.Builder<SchemaTableName, ElasticsearchTableDescription> builder = ImmutableMap.builder();
        for (String definedTable : config.getTableNames()) {
            SchemaTableName tableName;
            tableName = parseTableName(definedTable);

            if (!tableDefinitions.containsKey(tableName)) {
                throw new IOException("Missing table definition for: " + tableName);
            }
            ElasticsearchTableDescription elasticsearchTable = tableDefinitions.get(tableName);
            builder.put(tableName, elasticsearchTable);
        }
        return new ElasticsearchClient(builder.build(), config.getRequestTimeout());
    }

    private static List<File> listFiles(File dir)
    {
        if ((dir != null) && dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
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

    private static final class TypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private static final long serialVersionUID = 1L;

        private final TypeManager typeManager;

        @Inject
        public TypeDeserializer(TypeManager typeManager)
        {
            super(Type.class);
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            Type type = typeManager.getType(parseTypeSignature(value));
            checkArgument(type != null, "Unknown type %s", value);
            return type;
        }
    }
}
