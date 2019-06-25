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
package com.facebook.presto.rta.schema;

import com.facebook.presto.rta.RtaColumnMetadata;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import io.airlift.units.Duration;

import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class RTATableEntity
{
    private final String table;
    private List<RTADeployment> deployments;
    private RTADefinition definition;

    private List<ColumnMetadata> columns;

    public RTATableEntity(String table, List<RTADeployment> deployments, RTADefinition definition)
    {
        this.table = table;
        this.deployments = deployments;
        this.definition = definition;
        this.columns = this.definition.getFields().stream().map(field -> new RtaColumnMetadata(field.getName(), getPrestoTypeFromRtaType(field.getType()))).collect(toImmutableList());
    }

    private static Type getPrestoTypeFromRtaType(String type)
    {
        switch (type.toLowerCase(Locale.ENGLISH)) {
            case "string":
                return VarcharType.VARCHAR;
            case "long":
                return BigintType.BIGINT;
            case "double":
                return DoubleType.DOUBLE;
            case "boolean":
                return BooleanType.BOOLEAN;
            default:
                throw new UnsupportedOperationException("Don't know how to convert RTA type " + type);
        }
    }

    public List<RTADeployment> getDeployments()
    {
        return deployments;
    }

    public String getTable()
    {
        return table;
    }

    public RTADefinition getDefinition()
    {
        return definition;
    }

    private Optional<RTADefinition.Field> getTimestampFieldHelper()
    {
        List<RTADefinition.Field> timeFields = getDefinition().getFields().stream().filter(t -> t.getColumnType().equals("time")).collect(Collectors.toList());
        if (timeFields.isEmpty()) {
            return Optional.empty();
        }
        else if (timeFields.size() > 1) {
            throw new NoSuchElementException("Multiple time fields to choose from for " + this + " : " + timeFields);
        }
        else {
            return Optional.of(timeFields.get(0));
        }
    }

    public Optional<String> getTimestampField()
    {
        return getTimestampFieldHelper().map(RTADefinition.Field::getName);
    }

    public List<ColumnMetadata> getColumnsMetadata()
    {
        return columns;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("deployments", deployments)
                .add("definition", definition)
                .add("columns", columns)
                .toString();
    }

    public Optional<Duration> getRetention()
    {
        String numDaysBack = getDefinition().getMetadata().getRetentionDays();
        try {
            return Optional.of(new Duration(Double.parseDouble(numDaysBack), TimeUnit.DAYS));
        }
        catch (NumberFormatException ne) {
            return Optional.empty();
        }
    }

    public Optional<Type> getTimestampType()
    {
        return getTimestampFieldHelper().map(field -> getPrestoTypeFromRtaType(field.getType()));
    }
}
