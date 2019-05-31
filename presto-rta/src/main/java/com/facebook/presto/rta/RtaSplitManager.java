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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.util.concurrent.CompletableFuture;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class RtaSplitManager
        implements ConnectorSplitManager
{
    private final RtaConnectorProvider connectorProvider;
    private final RtaConnectorId rtaConnectorId;

    @Inject
    public RtaSplitManager(RtaConnectorProvider connectorProvider, RtaConnectorId rtaConnectorId)
    {
        this.connectorProvider = connectorProvider;
        this.rtaConnectorId = rtaConnectorId;
    }

    public static class RtaConnectorSplitSource
            implements ConnectorSplitSource
    {
        private final ConnectorSplitSource source;
        private final RtaConnectorId rtaConnectorId;
        private final RtaStorageKey rtaStorageKey;

        public RtaConnectorSplitSource(ConnectorSplitSource source, RtaConnectorId rtaConnectorId, RtaStorageKey rtaStorageKey)
        {
            this.source = source;
            this.rtaConnectorId = rtaConnectorId;
            this.rtaStorageKey = rtaStorageKey;
        }

        @Override
        public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
        {
            return source.getNextBatch(partitionHandle, maxSize).thenApply(inBatch ->
                    new ConnectorSplitBatch(inBatch.getSplits().stream().map(split -> new RtaSplit(rtaConnectorId, rtaStorageKey, split)).collect(toImmutableList()), inBatch.isNoMoreSplits()));
        }

        @Override
        public void close()
        {
            source.close();
        }

        @Override
        public boolean isFinished()
        {
            return source.isFinished();
        }
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        RtaTableLayoutHandle rtaTableLayoutHandle = (RtaTableLayoutHandle) layout;
        return new RtaConnectorSplitSource(connectorProvider.getConnector(rtaTableLayoutHandle.getTable().getKey()).getSplitManager().getSplits(transactionHandle, session, rtaTableLayoutHandle.createConnectorSpecificTableLayoutHandle(), splitSchedulingStrategy), rtaConnectorId, rtaTableLayoutHandle.getTable().getKey());
    }
}
