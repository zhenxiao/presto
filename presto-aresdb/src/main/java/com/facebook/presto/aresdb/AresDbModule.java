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

import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.inject.Inject;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class AresDbModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(AresDbConfig.class);
        binder.bind(AresDbConnector.class).in(Scopes.SINGLETON);
        binder.bind(AresDbMetadata.class).in(Scopes.SINGLETON);
        binder.bind(AresDbSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(AresDbPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(AresDbConnection.class).in(Scopes.SINGLETON);
        binder.bind(AresDbNodePartitioningProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorNodePartitioningProvider.class).to(AresDbNodePartitioningProvider.class).in(Scopes.SINGLETON);

        httpClientBinder(binder).bindHttpClient("aresDb", ForAresDb.class)
                .withConfigDefaults(cfg -> defaultHttpConfigs(cfg));

        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
        jsonCodecBinder(binder).bindMapJsonCodec(String.class, listJsonCodec(AresDbTable.class));
    }

    public static void defaultHttpConfigs(HttpClientConfig cfg)
    {
        cfg.setIdleTimeout(new Duration(60, SECONDS));
        cfg.setRequestTimeout(new Duration(60, SECONDS));
        cfg.setMaxConnectionsPerServer(250);
        cfg.setMaxContentLength(new DataSize(32, MEGABYTE));
    }

    @SuppressWarnings("serial")
    public static final class TypeDeserializer
            extends FromStringDeserializer<Type>
    {
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
