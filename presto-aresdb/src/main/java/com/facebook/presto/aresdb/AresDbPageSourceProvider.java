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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class AresDbPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final AresDbConnection aresDbConnection;
    private final AresDbConfig aresDbConfig;

    @Inject
    public AresDbPageSourceProvider(AresDbConnection aresDbConnection, AresDbConfig aresDbConfig)
    {
        this.aresDbConnection = requireNonNull(aresDbConnection, "aresDbConnection is null");
        this.aresDbConfig = requireNonNull(aresDbConfig, "aresDb config is null");
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<ColumnHandle> columns)
    {
        AresDbSplit aresDbSplit = (AresDbSplit) split;
        List<AresDbColumnHandle> aresDbColumns = columns.stream().map(c -> (AresDbColumnHandle) c).collect(Collectors.toList());

        return new AresDbPageSource(aresDbSplit, aresDbColumns, aresDbConnection, aresDbConfig, session);
    }
}
