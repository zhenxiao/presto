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

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.pipeline.TableScanPipeline;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class AresDbSplit
        implements ConnectorSplit
{
    private final AresDbConnectorId connectorId;
    private final TableScanPipeline pipeline;

    @JsonCreator
    public AresDbSplit(
            @JsonProperty("connectorId") AresDbConnectorId connectorId,
            @JsonProperty("pipeline") TableScanPipeline pipeline)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.pipeline = requireNonNull(pipeline, "pipeline is null");
    }

    @JsonProperty
    public AresDbConnectorId getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public TableScanPipeline getPipeline()
    {
        return pipeline;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return null;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }
}
