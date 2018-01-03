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
package com.facebook.presto.schemaless;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;

import java.util.List;

public class SchemalessColumnMetadata
        extends ColumnMetadata
{
    private final String columnKey;
    private final String sqlType;
    private final String fieldType;
    private final List<String> columnKeys;
    private final String expr;

    public SchemalessColumnMetadata(String name, Type prestoType, String fieldType, String columnKey, String sqlType, List<String> columnKeys, String expr)
    {
        super(name, prestoType, "fieldType: " + fieldType + ", MySqlType: " + sqlType, false);
        this.columnKey = columnKey;
        this.sqlType = sqlType;
        this.fieldType = fieldType;
        this.columnKeys = columnKeys;
        this.expr = expr;
    }

    public String getColumnKey()
    {
        return columnKey;
    }

    public String getSqlType()
    {
        return sqlType;
    }

    public List<String> getColumnKeys()
    {
        return columnKeys;
    }

    public String getExpr()
    {
        return expr;
    }

    public String getFieldType()
    {
        return fieldType;
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
        if (!super.equals(o)) {
            return false;
        }

        SchemalessColumnMetadata that = (SchemalessColumnMetadata) o;

        if (!columnKey.equals(that.columnKey)) {
            return false;
        }
        if (!sqlType.equals(that.sqlType)) {
            return false;
        }
        if (!fieldType.equals(that.fieldType)) {
            return false;
        }

        boolean listEquals = true;
        for (String key : columnKeys) {
            if (!that.columnKeys.contains(key)) {
                listEquals = false;
            }
        }
        if (!listEquals) {
            return false;
        }

        return expr.equals(that.expr);
    }

    @Override
    public int hashCode()
    {
        int result = super.hashCode();
        result = 31 * result + columnKey.hashCode();
        result = 31 * result + sqlType.hashCode();
        result = 31 * result + fieldType.hashCode();
        result = 31 * result + columnKeys.hashCode();
        result = 31 * result + expr.hashCode();
        return result;
    }
}
