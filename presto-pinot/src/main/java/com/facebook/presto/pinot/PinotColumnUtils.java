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

import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.Schema;

import java.util.ArrayList;
import java.util.List;

public class PinotColumnUtils
{
    private PinotColumnUtils()
    {
    }

    public static List<PinotColumn> getPinotColumnsForPinotSchema(Schema pinotTableSchema)
    {
        List<PinotColumn> pinotColumns = new ArrayList<>();
        for (String columnName : pinotTableSchema.getColumnNames()) {
            PinotColumn pinotColumn = new PinotColumn(columnName, getPrestoTypeFromPinotType(pinotTableSchema.getFieldSpecFor(columnName)));
            pinotColumns.add(pinotColumn);
        }
        return pinotColumns;
    }

    public static Type getPrestoTypeFromPinotType(FieldSpec pinotFieldSpec)
    {
        if (pinotFieldSpec.isSingleValueField()) {
            return getPrestoTypeFromPinotType(pinotFieldSpec.getDataType());
        }
        else {
            return VarcharType.VARCHAR;
        }
    }

    public static Type getPrestoTypeFromPinotType(DataType dataType)
    {
        switch (dataType) {
            case BOOLEAN:
                return BooleanType.BOOLEAN;
            case BYTE:
            case CHAR:
                return VarcharType.createVarcharType(1);
            case DOUBLE:
            case FLOAT:
                return DoubleType.DOUBLE;
            case SHORT:
            case INT:
                return IntegerType.INTEGER;
            case LONG:
                return BigintType.BIGINT;
            case STRING:
            case OBJECT:
                return VarcharType.VARCHAR;
            case CHAR_ARRAY:
            case BYTE_ARRAY:
            case SHORT_ARRAY:
            case STRING_ARRAY:
                return new ArrayType(VarcharType.VARCHAR);
            case INT_ARRAY:
                return new ArrayType(IntegerType.INTEGER);
            case LONG_ARRAY:
                return new ArrayType(BigintType.BIGINT);
            case DOUBLE_ARRAY:
            case FLOAT_ARRAY:
                return new ArrayType(DoubleType.DOUBLE);
            default:
                break;
        }
        throw new UnsupportedOperationException("Not support type conversion for pinot data type: " + dataType);
    }
}
