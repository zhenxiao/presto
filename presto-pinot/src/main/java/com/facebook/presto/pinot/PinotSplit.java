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

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.pipeline.TableScanPipeline;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class PinotSplit
        implements ConnectorSplit
{
    private final String connectorId;
    private final SplitType splitType;

    // Properties needed for broker split type
    private final Optional<TableScanPipeline> pipeline;

    // Properties needed for segment split type
    private final Optional<String> pql;
    private final Optional<String> segment;
    private final Optional<String> segmentHost;

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("splitType", splitType)
                .add("pipeline", pipeline)
                .add("pql", pql)
                .add("segment", segment)
                .add("segmentHost", segmentHost)
                .toString();
    }

    @JsonCreator
    public PinotSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("splitType") SplitType splitType,
            @JsonProperty("pipeline") Optional<TableScanPipeline> pipeline,
            @JsonProperty("pql") Optional<String> pql,
            @JsonProperty("segment") Optional<String> segment,
            @JsonProperty("segmentHost") Optional<String> segmentHost)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.splitType = requireNonNull(splitType, "splitType id is null");
        this.pipeline = requireNonNull(pipeline, "pipeline is null");
        this.pql = requireNonNull(pql, "table name is null");
        this.segment = requireNonNull(segment, "segment is null");
        this.segmentHost = requireNonNull(segmentHost, "host is null");

        // make sure the segment properties are present when the split type is segment
        if (splitType == SplitType.SEGMENT) {
            checkArgument(pql.isPresent(), "Table name is missing from the split");
            checkArgument(segment.isPresent(), "Segment is missing from the split");
            checkArgument(segmentHost.isPresent(), "Segment host address is missing from the split");
        }
        else {
            checkArgument(pipeline.isPresent(), "pipeline is missing from the split");
        }
    }

    public static PinotSplit createBrokerSplit(String connectorId, TableScanPipeline pipeline)
    {
        return new PinotSplit(
                requireNonNull(connectorId, "connector id is null"),
                SplitType.BROKER,
                Optional.of(requireNonNull(pipeline, "pipeline is null")),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    public static PinotSplit createSegmentSplit(String connectorId, String pql, String segment, String segmentHost)
    {
        return new PinotSplit(
                requireNonNull(connectorId, "connector id is null"),
                SplitType.SEGMENT,
                Optional.empty(),
                Optional.of(requireNonNull(pql, "pql is null")),
                Optional.of(requireNonNull(segment, "segment is null")),
                Optional.of(requireNonNull(segmentHost, "segmentHost is null")));
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public SplitType getSplitType()
    {
        return splitType;
    }

    @JsonProperty
    public Optional<TableScanPipeline> getPipeline()
    {
        return pipeline;
    }

    @JsonProperty
    public Optional<String> getPql()
    {
        return pql;
    }

    @JsonProperty
    public Optional<String> getSegmentHost()
    {
        return segmentHost;
    }

    @JsonProperty
    public Optional<String> getSegment()
    {
        return segment;
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

    public enum SplitType
    {
        SEGMENT,
        BROKER,
    }
}
