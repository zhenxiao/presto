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

import com.facebook.presto.aresdb.AresDbConnectorFactory;
import com.facebook.presto.pinot.PinotConnectorFactory;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableList;

import java.util.Map;

public class RtaPlugin
        implements Plugin
{
    public static class ConnectorFactoryResolverOnly
            implements ConnectorFactory
    {
        private final ConnectorFactory internal;

        public ConnectorFactoryResolverOnly(ConnectorFactory internal)
        {
            this.internal = internal;
        }

        @Override
        public String getName()
        {
            return "rta_internal_only_" + internal.getName();
        }

        @Override
        public ConnectorHandleResolver getHandleResolver()
        {
            return internal.getHandleResolver();
        }

        @Override
        public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
        {
            throw new UnsupportedOperationException("Don't actually support creating a connector for " + getName());
        }
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new RtaConnectorFactory(), new ConnectorFactoryResolverOnly(new AresDbConnectorFactory()), new ConnectorFactoryResolverOnly(new PinotConnectorFactory()));
    }
}
