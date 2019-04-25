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

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class AresDbTable
{
    private final String name;
    private final List<AresDbColumn> columns;
    private final List<ColumnMetadata> columnsMetadata;

    @JsonCreator
    public AresDbTable(
            @JsonProperty("name") String name,
            @JsonProperty("columns") List<AresDbColumn> columns)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.name = name;
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));

        this.columnsMetadata = new ArrayList<>();
        for (AresDbColumn column : columns) {
            this.columnsMetadata.add(new ColumnMetadata(column.getName(), column.getDataType()));
        }
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public List<AresDbColumn> getColumns()
    {
        return columns;
    }

    public List<ColumnMetadata> getColumnsMetadata()
    {
        return columnsMetadata;
    }

    public static class AresDbColumn
    {
        private final String name;
        private final Type dataType;
        private final boolean isTimeColumn;

        @JsonCreator
        public AresDbColumn(String name, Type dataType, boolean isTimeColumn)
        {
            this.name = name;
            this.dataType = dataType;
            this.isTimeColumn = isTimeColumn;
        }

        @JsonProperty
        public String getName()
        {
            return name;
        }

        @JsonProperty
        public Type getDataType()
        {
            return dataType;
        }

        @JsonProperty
        public boolean isTimeColumn()
        {
            return isTimeColumn;
        }
    }
}
