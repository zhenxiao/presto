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

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.Type;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.utils.DataTable;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;

public class PinotRecordCursor
        implements RecordCursor
{
    private static final Logger log = Logger.get(PinotRecordCursor.class);
    private final List<PinotColumnHandle> columnHandles;
    private final int[] fieldToColumnIndex;
    private final List<DataTable> dataTableList;
    private final long totalBytes = 0;
    private Optional<DataTable> currentDataTable;
    private int tableIdx;
    private int rowIdx = -1;

    public PinotRecordCursor(List<PinotColumnHandle> columnHandles, Map<ServerInstance, DataTable> dataTableMap)
    {
        this.columnHandles = columnHandles;
        this.dataTableList = new ArrayList<>();
        dataTableList.addAll(dataTableMap.values());
        if (dataTableMap.isEmpty()) {
            currentDataTable = Optional.empty();
        }
        else {
            currentDataTable = Optional.of(dataTableList.get(tableIdx));
        }
        fieldToColumnIndex = new int[columnHandles.size()];
        for (int i = 0; i < columnHandles.size(); i++) {
            PinotColumnHandle columnHandle = columnHandles.get(i);
            fieldToColumnIndex[i] = columnHandle.getOrdinalPosition();
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (!currentDataTable.isPresent()) {
            return false;
        }
        if (rowIdx < currentDataTable.get().getNumberOfRows() - 1) {
            rowIdx++;
            return true;
        }
        if (tableIdx < dataTableList.size() - 1) {
            rowIdx = 0;
            tableIdx++;
            currentDataTable = Optional.of(dataTableList.get(tableIdx));
            return true;
        }
        currentDataTable = Optional.empty();
        return false;
    }

    @Override
    public boolean getBoolean(int field)
    {
        return currentDataTable.get().getBoolean(rowIdx, field);
    }

    @Override
    public long getLong(int field)
    {
        if (getType(field) == IntegerType.INTEGER) {
            return currentDataTable.get().getInt(rowIdx, field);
        }
        return currentDataTable.get().getLong(rowIdx, field);
    }

    @Override
    public double getDouble(int field)
    {
        return currentDataTable.get().getDouble(rowIdx, field);
    }

    @Override
    public Slice getSlice(int field)
    {
        checkFieldType(field, VARCHAR);
        FieldSpec.DataType columnType = currentDataTable.get().getDataSchema().getColumnType(field);
        switch (columnType) {
            case INT_ARRAY:
                int[] intArray = currentDataTable.get().getIntArray(rowIdx, field);
                return utf8Slice(Arrays.toString(intArray));
            case LONG_ARRAY:
                long[] longArray = currentDataTable.get().getLongArray(rowIdx, field);
                return utf8Slice(Arrays.toString(longArray));
            case FLOAT_ARRAY:
                float[] floatArray = currentDataTable.get().getFloatArray(rowIdx, field);
                return utf8Slice(Arrays.toString(floatArray));
            case DOUBLE_ARRAY:
                double[] doubleArray = currentDataTable.get().getDoubleArray(rowIdx, field);
                return utf8Slice(Arrays.toString(doubleArray));
            case STRING_ARRAY:
                String[] stringArray = currentDataTable.get().getStringArray(rowIdx, field);
                return utf8Slice(Arrays.toString(stringArray));
            case STRING:
                String fieldStr = currentDataTable.get().getString(rowIdx, field);
                if (fieldStr == null || fieldStr.isEmpty()) {
                    return Slices.EMPTY_SLICE;
                }
                return Slices.utf8Slice(fieldStr);
        }
        return Slices.EMPTY_SLICE;
    }

    @Override
    public Object getObject(int field)
    {
        return null;
    }

    private void checkFieldType(int field, Type expected)
    {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return false;
    }

    @Override
    public void close()
    {
        currentDataTable = null;
    }
}
