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
package com.facebook.presto.rta;

import com.facebook.presto.rta.schema.ForRTAMS;
import com.facebook.presto.rta.schema.RTAMSClient;
import com.facebook.presto.rta.schema.RTASchemaHandler;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.SECONDS;

public class RtaModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(RtaConfig.class);
        binder.bind(RtaConnector.class).in(Scopes.SINGLETON);
        binder.bind(RtaConnectorProvider.class).in(Scopes.SINGLETON);
        binder.bind(RtaMetadata.class).in(Scopes.SINGLETON);
        binder.bind(RtaSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(RtaPropertyManager.class).in(Scopes.SINGLETON);
        binder.bind(RTAMSClient.class).in(Scopes.SINGLETON);
        binder.bind(RtaPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(RTASchemaHandler.class).in(Scopes.SINGLETON);
        binder.bind(RtaNodePartitioningProvider.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorNodePartitioningProvider.class).to(RtaNodePartitioningProvider.class).in(Scopes.SINGLETON);
        binder.bind(RtaSessionProperties.class).in(Scopes.SINGLETON);

        httpClientBinder(binder).bindHttpClient("rtams", ForRTAMS.class)
                .withConfigDefaults(cfg -> defaultHttpConfigs(cfg));
    }

    public static void defaultHttpConfigs(HttpClientConfig cfg)
    {
        cfg.setIdleTimeout(new Duration(60, SECONDS));
        cfg.setRequestTimeout(new Duration(60, SECONDS));
        cfg.setMaxConnectionsPerServer(250);
        cfg.setMaxContentLength(new DataSize(32, MEGABYTE));
    }
}
