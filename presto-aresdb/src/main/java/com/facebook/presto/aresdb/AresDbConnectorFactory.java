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

import com.facebook.presto.aresdb.AresDbConnector.AresDbTransactionHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.type.TypeManager;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;

import java.util.Map;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

public class AresDbConnectorFactory
        implements ConnectorFactory
{
    @Override
    public String getName()
    {
        return "aresdb";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new ConnectorHandleResolver()
        {
            @Override
            public Class<? extends ConnectorTableHandle> getTableHandleClass()
            {
                return AresDbTableHandle.class;
            }

            @Override
            public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass()
            {
                return AresDbTableLayoutHandle.class;
            }

            @Override
            public Class<? extends ColumnHandle> getColumnHandleClass()
            {
                return AresDbColumnHandle.class;
            }

            @Override
            public Class<? extends ConnectorSplit> getSplitClass()
            {
                return AresDbSplit.class;
            }

            @Override
            public Class<? extends ConnectorTransactionHandle> getTransactionHandleClass()
            {
                return AresDbTransactionHandle.class;
            }
        };
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        requireNonNull(catalogName, "connectorId is null");
        requireNonNull(config, "config is null");

        try {
            Bootstrap app = new Bootstrap(new JsonModule(), new AresDbModule(), binder -> {
                binder.bind(AresDbConnectorId.class).toInstance(new AresDbConnectorId(catalogName));
                binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                binder.bind(NodeManager.class).toInstance(context.getNodeManager());
            });

            Injector injector = app.strictConfig().doNotInitializeLogging().setRequiredConfigurationProperties(config).initialize();

            return injector.getInstance(AresDbConnector.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
