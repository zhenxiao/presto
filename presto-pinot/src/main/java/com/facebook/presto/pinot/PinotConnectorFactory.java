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

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import org.weakref.jmx.guice.MBeanModule;

import javax.management.MBeanServer;

import java.util.Map;

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static java.util.Objects.requireNonNull;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class PinotConnectorFactory
        implements ConnectorFactory
{
    public PinotConnectorFactory()
    {
    }

    @Override
    public String getName()
    {
        return "pinot";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new PinotHandleResolver();
    }

    @Override
    public Connector create(final String connectorId, Map<String, String> config, ConnectorContext context)
    {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(config, "config is null");

        try {
            Bootstrap app = new Bootstrap(new JsonModule(), new MBeanModule(), new PinotModule(), binder -> {
                binder.bind(MBeanServer.class).toInstance(new RebindSafeMBeanServer(getPlatformMBeanServer()));
                binder.bind(PinotConnectorId.class).toInstance(new PinotConnectorId(connectorId));
                binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                binder.bind(PinotMetrics.class).in(Scopes.SINGLETON);
                newExporter(binder).export(PinotMetrics.class).as(generatedNameOf(PinotMetrics.class, connectorId));
                binder.bind(ConnectorNodePartitioningProvider.class).to(PinotNodePartitioningProvider.class).in(Scopes.SINGLETON);
            });

            Injector injector = app.strictConfig().doNotInitializeLogging().setRequiredConfigurationProperties(config).initialize();

            return injector.getInstance(PinotConnector.class);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
