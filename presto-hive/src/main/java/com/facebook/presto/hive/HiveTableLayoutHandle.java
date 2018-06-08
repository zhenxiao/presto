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
package com.facebook.presto.hive;

import com.facebook.presto.hive.HiveBucketing.HiveBucketFilter;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class HiveTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final SchemaTableName schemaTableName;
    private final List<ColumnHandle> partitionColumns;
    private final List<HivePartition> partitions;
    private final TupleDomain<? extends ColumnHandle> compactEffectivePredicate;
    private final TupleDomain<ColumnHandle> promisedPredicate;
    private final Optional<HiveBucketHandle> bucketHandle;
    private final Optional<HiveBucketFilter> bucketFilter;
    private final Optional<TupleDomain<List<String>>> nestedTupleDomain;
    private final Optional<Map<String, List<String>>> aggregations;

    @JsonCreator
    public HiveTableLayoutHandle(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("partitionColumns") List<ColumnHandle> partitionColumns,
            @JsonProperty("compactEffectivePredicate") TupleDomain<ColumnHandle> compactEffectivePredicate,
            @JsonProperty("promisedPredicate") TupleDomain<ColumnHandle> promisedPredicate,
            @JsonProperty("bucketHandle") Optional<HiveBucketHandle> bucketHandle,
            @JsonProperty("bucketFilter") Optional<HiveBucketFilter> bucketFilter,
            @JsonProperty("nestedTupleDomain") Optional<TupleDomain<List<String>>> nestedTupleDomain,
            @JsonProperty("aggregations") Optional<Map<String, List<String>>> aggregations)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "table is null");
        this.partitionColumns = ImmutableList.copyOf(requireNonNull(partitionColumns, "partitionColumns is null"));
        this.compactEffectivePredicate = requireNonNull(compactEffectivePredicate, "compactEffectivePredicate is null");
        this.partitions = null;
        this.promisedPredicate = requireNonNull(promisedPredicate, "promisedPredicate is null");
        this.bucketHandle = requireNonNull(bucketHandle, "bucketHandle is null");
        this.bucketFilter = requireNonNull(bucketFilter, "bucketFilter is null");
        this.nestedTupleDomain = requireNonNull(nestedTupleDomain, "nestedTupleDomain is null");
        this.aggregations = requireNonNull(aggregations, "aggregations is null");
    }

    public HiveTableLayoutHandle(
            SchemaTableName schemaTableName,
            List<ColumnHandle> partitionColumns,
            List<HivePartition> partitions,
            TupleDomain<? extends ColumnHandle> compactEffectivePredicate,
            TupleDomain<ColumnHandle> promisedPredicate,
            Optional<HiveBucketHandle> bucketHandle,
            Optional<HiveBucketFilter> bucketFilter,
            Optional<TupleDomain<List<String>>> nestedTupleDomain,
            Optional<Map<String, List<String>>> aggregations)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "table is null");
        this.partitionColumns = ImmutableList.copyOf(requireNonNull(partitionColumns, "partitionColumns is null"));
        this.partitions = requireNonNull(partitions, "partitions is null");
        this.compactEffectivePredicate = requireNonNull(compactEffectivePredicate, "compactEffectivePredicate is null");
        this.promisedPredicate = requireNonNull(promisedPredicate, "promisedPredicate is null");
        this.bucketHandle = requireNonNull(bucketHandle, "bucketHandle is null");
        this.bucketFilter = requireNonNull(bucketFilter, "bucketFilter is null");
        this.nestedTupleDomain = requireNonNull(nestedTupleDomain, "nestedTupleDomain is null");
        this.aggregations = requireNonNull(aggregations, "aggregations is null");
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    public List<ColumnHandle> getPartitionColumns()
    {
        return partitionColumns;
    }

    /**
     * Partitions are dropped when HiveTableLayoutHandle is serialized.
     *
     * @return list of partitions if available, {@code Optional.empty()} if dropped
     */
    @JsonIgnore
    public Optional<List<HivePartition>> getPartitions()
    {
        return Optional.ofNullable(partitions);
    }

    @JsonProperty
    public TupleDomain<? extends ColumnHandle> getCompactEffectivePredicate()
    {
        return compactEffectivePredicate;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getPromisedPredicate()
    {
        return promisedPredicate;
    }

    @JsonProperty
    public Optional<HiveBucketHandle> getBucketHandle()
    {
        return bucketHandle;
    }

    @JsonProperty
    public Optional<HiveBucketFilter> getBucketFilter()
    {
        return bucketFilter;
    }

    @JsonProperty
    public Optional<TupleDomain<List<String>>> getNestedTupleDomain()
    {
        return nestedTupleDomain;
    }

    @JsonProperty
    public Optional<Map<String, List<String>>> getAggregations()
    {
        return aggregations;
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
        HiveTableLayoutHandle that = (HiveTableLayoutHandle) o;
        return Objects.equals(schemaTableName, that.schemaTableName) &&
                Objects.equals(partitionColumns, that.partitionColumns) &&
                Objects.equals(partitions, that.partitions) &&
                Objects.equals(aggregations, that.aggregations) &&
                Objects.equals(nestedTupleDomain, that.nestedTupleDomain);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaTableName, partitionColumns, partitions, nestedTupleDomain, aggregations);
    }

    @Override
    public String toString()
    {
        return schemaTableName.toString();
    }
}
