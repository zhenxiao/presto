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
package com.facebook.presto.spi.pipeline;

import com.facebook.presto.spi.ColumnHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Pipeline on top of the existing table to enable pushdown complex operations into the connector
 */
public class TableScanPipeline
{
    private final List<PipelineNode> pipelineNodes;
    private List<ColumnHandle> outputColumnHandles;

    @JsonCreator
    public TableScanPipeline(
            @JsonProperty("pipelineNodes") List<PipelineNode> pipelineNodes,
            @JsonProperty("outputColumnHandles") List<ColumnHandle> outputColumnsHandles)
    {
        this.pipelineNodes = requireNonNull(pipelineNodes, "pipelineNodes is null");
        this.outputColumnHandles = requireNonNull(outputColumnsHandles, "outputColumnHandles is null");
    }

    public TableScanPipeline()
    {
        this.pipelineNodes = new ArrayList<>();
        this.outputColumnHandles = new ArrayList<>();
    }

    public void addPipeline(PipelineNode pipelineNode, List<ColumnHandle> outputColumnHandles)
    {
        requireNonNull(pipelineNode, "pipelineNode is null");
        requireNonNull(outputColumnHandles, "outputColumnHandles is null");
        this.pipelineNodes.add(pipelineNode);
        this.outputColumnHandles = outputColumnHandles;
    }

    @JsonProperty
    public List<PipelineNode> getPipelineNodes()
    {
        return pipelineNodes;
    }

    @JsonProperty
    public List<ColumnHandle> getOutputColumnHandles()
    {
        return outputColumnHandles;
    }

    @Override
    public String toString()
    {
        return pipelineNodes.stream()
                .map(node -> node.toString())
                .collect(Collectors.joining(","));
    }

    public List<String> getLastPipelineNodeOutputColumns()
    {
        if (pipelineNodes.isEmpty()) {
            throw new IllegalStateException("Expected to have non empty pipeline");
        }
        return pipelineNodes.get(pipelineNodes.size() - 1).getOutputColumns();
    }
}
