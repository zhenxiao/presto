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
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;

import javax.inject.Inject;

import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class RtaConnectorProvider
{
    private final Map<RtaStorageKey, Connector> connectors;

    @Inject
    public RtaConnectorProvider(RtaPropertyManager propertyManager, RtaConnectorId connectorId, ConnectorContext connectorContext)
    {
        this.connectors = propertyManager.getProperties().entrySet().stream().collect(toImmutableMap(entry -> entry.getKey(), entry -> createConnector(entry.getKey(), connectorId, entry.getValue(), connectorContext)));
    }

    public Connector getConnector(RtaStorageKey key)
    {
        return requireNonNull(connectors.get(key), "Unknown RTA storage key " + key);
    }

    public Map<RtaStorageKey, Connector> getConnectors()
    {
        return connectors;
    }

    private static Connector createConnector(RtaStorageKey rtaStorageKey, RtaConnectorId connectorId, Map<String, String> properties, ConnectorContext connectorContext)
    {
        return createConnectFactory(rtaStorageKey.getType()).create(
                connectorId.getId() + "-" + rtaStorageKey.getEnvironment(),
                properties,
                connectorContext);
    }

    private static ConnectorFactory createConnectFactory(RtaStorageType type)
    {
        switch (type) {
            case ARESDB:
                return new AresDbConnectorFactory();
            case PINOT:
                return new PinotConnectorFactory();
            default:
                throw new UnsupportedOperationException("Invalid rta storage type " + type);
        }
    }
}
