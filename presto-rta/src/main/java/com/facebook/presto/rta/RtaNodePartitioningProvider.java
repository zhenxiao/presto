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

import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.connector.ConnectorBucketNodeMap;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.type.Type;

import javax.inject.Inject;

import java.util.List;
import java.util.function.ToIntFunction;

public class RtaNodePartitioningProvider
        implements ConnectorNodePartitioningProvider
{
    private final RtaConnectorProvider connectorProvider;

    @Inject
    public RtaNodePartitioningProvider(RtaConnectorProvider connectorProvider)
    {
        this.connectorProvider = connectorProvider;
    }

    @Override
    public ConnectorBucketNodeMap getBucketNodeMap(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        RtaPartitioningHandle rtaPartitioningHandle = (RtaPartitioningHandle) partitioningHandle;
        return connectorProvider.getConnector(rtaPartitioningHandle.getKey()).getNodePartitioningProvider().getBucketNodeMap(transactionHandle, session, rtaPartitioningHandle.getPartitioningHandle());
    }

    @Override
    public ToIntFunction<ConnectorSplit> getSplitBucketFunction(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle)
    {
        RtaPartitioningHandle rtaPartitioningHandle = (RtaPartitioningHandle) partitioningHandle;
        return connectorProvider.getConnector(rtaPartitioningHandle.getKey()).getNodePartitioningProvider().getSplitBucketFunction(transactionHandle, session, rtaPartitioningHandle.getPartitioningHandle());
    }

    @Override
    public BucketFunction getBucketFunction(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle, List<Type> partitionChannelTypes, int bucketCount)
    {
        RtaPartitioningHandle rtaPartitioningHandle = (RtaPartitioningHandle) partitioningHandle;
        return connectorProvider.getConnector(rtaPartitioningHandle.getKey()).getNodePartitioningProvider().getBucketFunction(transactionHandle, session, rtaPartitioningHandle.getPartitioningHandle(), partitionChannelTypes, bucketCount);
    }
}
