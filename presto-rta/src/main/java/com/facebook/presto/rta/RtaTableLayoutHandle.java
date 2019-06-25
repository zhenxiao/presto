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

import com.facebook.presto.aresdb.AresDbTableHandle;
import com.facebook.presto.aresdb.AresDbTableLayoutHandle;
import com.facebook.presto.aresdb.query.AresDbQueryGenerator;
import com.facebook.presto.pinot.PinotTableHandle;
import com.facebook.presto.pinot.PinotTableLayoutHandle;
import com.facebook.presto.pinot.query.PinotQueryGenerator;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.pipeline.TableScanPipeline;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class RtaTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final RtaTableHandle table;
    private final Optional<TupleDomain<ColumnHandle>> constraint;
    private final Optional<TableScanPipeline> scanPipeline;

    @JsonCreator
    public RtaTableLayoutHandle(
            @JsonProperty("table") RtaTableHandle table,
            @JsonProperty("constraint") Optional<TupleDomain<ColumnHandle>> constraint,
            @JsonProperty("scanPipeline") Optional<TableScanPipeline> scanPipeline)
    {
        this.table = requireNonNull(table, "table is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.scanPipeline = requireNonNull(scanPipeline, "scanPipeline is null");
    }

    @JsonProperty
    public RtaTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public Optional<TupleDomain<ColumnHandle>> getConstraint()
    {
        return constraint;
    }

    @JsonProperty
    public Optional<TableScanPipeline> getScanPipeline()
    {
        return scanPipeline;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RtaTableLayoutHandle that = (RtaTableLayoutHandle) o;
        return Objects.equals(table, that.table) &&
                Objects.equals(constraint, that.constraint) &&
                Objects.equals(scanPipeline, that.scanPipeline);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, constraint, scanPipeline);
    }

    @Override
    public String toString()
    {
        StringBuilder result = new StringBuilder();
        result.append(table.toString());
        if (scanPipeline.isPresent()) {
            result.append(", query=" + getConnectorSpecificQuery());
        }
        if (constraint.isPresent()) {
            result.append(", constraint=" + constraint.get().toString());
        }
        return result.toString();
    }

    public Object getConnectorSpecificQuery()
    {
        switch (table.getKey().getType()) {
            case ARESDB:
                return AresDbQueryGenerator.generate(scanPipeline.get(), Optional.empty(), Optional.empty(), Optional.empty());
            case PINOT:
                return PinotQueryGenerator.generatePQL(scanPipeline.get(), Optional.empty());
            default:
                throw new IllegalStateException("Unknown connector type " + table.getKey().getType());
        }
    }

    public ConnectorTableLayoutHandle createConnectorSpecificTableLayoutHandle()
    {
        switch (table.getKey().getType()) {
            case ARESDB:
                return new AresDbTableLayoutHandle((AresDbTableHandle) table.getHandle(), constraint, scanPipeline);
            case PINOT:
                return new PinotTableLayoutHandle((PinotTableHandle) table.getHandle(), constraint, scanPipeline);
            default:
                throw new IllegalStateException("Unknown connector type " + table.getKey().getType());
        }
    }
}
