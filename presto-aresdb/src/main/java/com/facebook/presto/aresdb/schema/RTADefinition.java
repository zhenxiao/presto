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

package com.facebook.presto.aresdb.schema;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

/**
 * This class communicates with the rta-ums (muttley)/rtaums (udeploy) service. It's api is available here:
 * https://rtaums.uberinternal.com/tables/definitions/{namespace}/{tablename}
 */
public class RTADefinition
{
    @JsonProperty
    private List<Field> fields;

    @JsonProperty("rtaTableMetadata")
    private RTAMetadata metadata;

    public List<Field> getFields()
    {
        return fields;
    }

    public RTAMetadata getMetadata()
    {
        return metadata;
    }

    public static class Field
    {
        @JsonProperty
        private String type;

        @JsonProperty
        private String name;

        @JsonProperty
        private String uberLogicalType;

        @JsonProperty
        private String columnType;

        @JsonProperty
        private String cardinality;

        public Field(@JsonProperty("type") String type, @JsonProperty("name") String name, @JsonProperty("uberLogicalType") String uberLogicalType, @JsonProperty("columnType") String columnType, @JsonProperty("cardinality") String cardinality)
        {
            this.type = type;
            this.name = name;
            this.uberLogicalType = uberLogicalType;
            this.columnType = columnType;
            this.cardinality = cardinality;
        }

        public String getType()
        {
            return type;
        }

        public String getName()
        {
            return name;
        }

        public String getUberLogicalType()
        {
            return uberLogicalType;
        }

        public String getColumnType()
        {
            return columnType;
        }

        public String getCardinality()
        {
            return cardinality;
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
            Field field = (Field) o;
            return Objects.equals(type, field.type)
                    && Objects.equals(name, field.name)
                    && Objects.equals(uberLogicalType, field.uberLogicalType)
                    && Objects.equals(columnType, field.columnType)
                    && Objects.equals(cardinality, field.cardinality);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(type, name, uberLogicalType, columnType, cardinality);
        }
    }

    public static class RTAMetadata
    {
        @JsonProperty
        private boolean isFactTable;

        @JsonProperty
        private List<String> primaryKeys;

        @JsonProperty
        private List<String> queryTypes;

        public boolean isFactTable()
        {
            return isFactTable;
        }

        public List<String> getPrimaryKeys()
        {
            return primaryKeys;
        }

        public List<String> getQueryTypes()
        {
            return queryTypes;
        }
    }
}
