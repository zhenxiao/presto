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

public class PinotTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final PinotTableHandle table;
    private final Optional<TupleDomain<ColumnHandle>> constraint;
    private final Optional<TableScanPipeline> scanPipeline;

    @JsonCreator
    public PinotTableLayoutHandle(
            @JsonProperty("table") PinotTableHandle table,
            @JsonProperty("constraint") Optional<TupleDomain<ColumnHandle>> constraint,
            @JsonProperty("scanPipeline") Optional<TableScanPipeline> scanPipeline)
    {
        this.table = requireNonNull(table, "table is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.scanPipeline = requireNonNull(scanPipeline, "scanPipeline is null");
    }

    @JsonProperty
    public PinotTableHandle getTable()
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
        PinotTableLayoutHandle that = (PinotTableLayoutHandle) o;
        return Objects.equals(table, that.table) && Objects.equals(constraint, that.constraint) && Objects.equals(scanPipeline, that.scanPipeline);
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
        if (constraint.isPresent()) {
            result.append(", constraint=" + constraint.get().toString());
        }
        if (scanPipeline.isPresent()) {
            result.append(", pql=").append(PinotQueryGenerator.generatePQL(scanPipeline.get(), Optional.empty()));
        }
        return result.toString();
    }
}
