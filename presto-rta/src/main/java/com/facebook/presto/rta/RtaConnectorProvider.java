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

import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class RtaConnectorProvider
{
    private final Map<RtaStorageKey, Map<String, String>> properties;
    private final RtaConnectorId connectorId;
    private final ConnectorContext connectorContext;

    @GuardedBy("this")
    private final Map<RtaStorageKey, Connector> connectors = new HashMap<>();

    @Inject
    public RtaConnectorProvider(RtaPropertyManager propertyManager, RtaConnectorId connectorId, ConnectorContext connectorContext)
    {
        this.properties = propertyManager.getProperties();
        this.connectorId = connectorId;
        this.connectorContext = connectorContext;
    }

    public synchronized Connector getConnector(RtaStorageKey key)
    {
        return connectors.computeIfAbsent(key, this::createConnector);
    }

    private synchronized Connector createConnector(RtaStorageKey rtaStorageKey)
    {
        return createConnectFactory(rtaStorageKey.getType()).create(
                connectorId.getId() + "-" + rtaStorageKey.getEnvironment(),
                requireNonNull(properties.get(rtaStorageKey), "Cannot find RTA cluster definition for " + rtaStorageKey),
                connectorContext);
    }

    private ConnectorFactory createConnectFactory(RtaStorageType type)
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
