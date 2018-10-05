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

import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.utils.DataTable;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.facebook.presto.pinot.PinotErrorCode.PINOT_UNSUPPORTED_COLUMN_TYPE;
import static com.facebook.presto.pinot.PinotQueryGenerator.getPinotQuery;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;

/**
 * This class retrieves Pinot data from a Pinot client, and re-constructs the data into Presto Pages.
 */

public class PinotPageSource
        implements ConnectorPageSource
{
    private final List<PinotColumnHandle> columnHandles;
    private List<Type> columnTypes;

    private PinotConfig pinotConfig;
    private PinotSplit split;
    private PinotScatterGatherQueryClient pinotQueryClient;

    // dataTableList stores the dataTable returned from each server. Each dataTable is constructed to a Page, and then destroyed to save memory.
    private LinkedList<PinotDataTableWithSize> dataTableList = new LinkedList<>();
    private long completedBytes;
    private long readTimeNanos;
    private long estimatedMemoryUsageInBytes;
    private PinotDataTableWithSize currentDataTable;

    private boolean closed;
    private boolean isPinotDataFetched;

    public PinotPageSource(PinotConfig pinotConfig, PinotScatterGatherQueryClient pinotQueryClient, PinotSplit split, List<PinotColumnHandle> columnHandles)
    {
        requireNonNull(split, "split is null");
        this.pinotConfig = requireNonNull(pinotConfig, "pinotConfig is null");
        this.split = requireNonNull(split, "split is null");
        this.pinotQueryClient = requireNonNull(pinotQueryClient, "pinotQueryClient is null");
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return estimatedMemoryUsageInBytes;
    }

    /**
     * @return true if is closed or all Pinot data have been processed.
     */
    @Override
    public boolean isFinished()
    {
        return closed || dataTableList.isEmpty();
    }

    /**
     * Iterate through each Pinot {@link com.linkedin.pinot.common.utils.DataTable}, and construct a {@link com.facebook.presto.spi.Page} out of it.
     *
     * @return constructed page for pinot data.
     */
    @Override
    public Page getNextPage()
    {
        if (!isPinotDataFetched) {
            fetchPinotData();
        }
        if (isFinished()) {
            close();
            return null;
        }
        // To reduce memory usage, remove dataTable from dataTableList once it's processed.
        if (currentDataTable != null) {
            estimatedMemoryUsageInBytes -= currentDataTable.getEstimatedSizeInBytes();
        }
        currentDataTable = dataTableList.pop();

        PageBuilder pageBuilder = new PageBuilder(columnTypes);
        // Note that declared positions in the Page should be the same with number of rows in each Block
        pageBuilder.declarePositions(currentDataTable.getDataTable().getNumberOfRows());
        for (int column = 0; column < columnTypes.size(); column++) {
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(column);
            Type columnType = columnTypes.get(column);
            writeBlock(blockBuilder, columnType, column);
        }
        Page page = pageBuilder.build();
        return page;
    }

    /**
     * Fetch data from Pinot for the current split and store the {@link com.linkedin.pinot.common.utils.DataTable} returned from each Pinto server.
     */
    private void fetchPinotData()
    {
        long startTimeNanos = System.nanoTime();
        Map<ServerInstance, DataTable> dataTableMap = pinotQueryClient.queryPinotServerForDataTable(getPinotQuery(pinotConfig, columnHandles, split), split.getHost(), split.getSegment());
        dataTableMap.values().stream().filter(m -> m != null).forEach(dataTable ->
        {
            // Store each dataTable which will later be constructed into Pages.
            // Also update estimatedMemoryUsage, mostly represented by the size of all dataTables, using numberOfRows and fieldTypes combined as an estimate
            int estimatedTableSizeInBytes = IntStream.rangeClosed(0, dataTable.getDataSchema().size() - 1)
                    .map(i -> getEstimatedColumnSizeInBytes(dataTable.getDataSchema().getColumnType(i)) * dataTable.getNumberOfRows())
                    .reduce(0, Integer::sum);
            dataTableList.add(new PinotDataTableWithSize(dataTable, estimatedTableSizeInBytes));
            estimatedMemoryUsageInBytes += estimatedTableSizeInBytes;
        });
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        columnHandles.stream()
                .map(PinotColumnHandle::getColumnType)
                .forEach(types::add);
        this.columnTypes = types.build();
        readTimeNanos = System.nanoTime() - startTimeNanos;
        isPinotDataFetched = true;
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
    }

    /**
     * Generates the {@link com.facebook.presto.spi.block.Block} for the specific column from the {@link #currentDataTable}.
     *
     * <p>Based on the original Pinot column types, write as Presto-supported values to {@link com.facebook.presto.spi.block.BlockBuilder}, e.g.
     * FLOAT -> Double, INT -> Long, String -> Slice.
     *
     * @param blockBuilder blockBuilder for the current column
     * @param columnType   type of the column
     * @param columnIdx    column index
     */

    private void writeBlock(BlockBuilder blockBuilder, Type columnType, int columnIdx)
    {
        Class<?> javaType = columnType.getJavaType();
        DataType pinotColumnType = currentDataTable.getDataTable().getDataSchema().getColumnType(columnIdx);
        if (javaType.equals(boolean.class)) {
            writeBooleanBlock(blockBuilder, columnType, columnIdx);
        }
        else if (pinotColumnType.isInteger()) {
            writeLongBlock(blockBuilder, columnType, columnIdx, pinotColumnType);
        }
        else if (pinotColumnType.equals(DataType.DOUBLE) || pinotColumnType.equals(DataType.FLOAT)) {
            writeDoubleBlock(blockBuilder, columnType, columnIdx, pinotColumnType);
        }
        else if (javaType.equals(Slice.class)) {
            writeSliceBlock(blockBuilder, columnType, columnIdx);
        }
        else {
            throw new PrestoException(
                    PINOT_UNSUPPORTED_COLUMN_TYPE,
                    String.format(
                            "Failed to write column %s. pinotColumnType %s, javaType %s",
                            columnHandles.get(columnIdx).getColumnName(), pinotColumnType, javaType));
        }
    }

    private void writeBooleanBlock(BlockBuilder blockBuilder, Type columnType, int columnIdx)
    {
        IntStream.rangeClosed(0, currentDataTable.getDataTable().getNumberOfRows() - 1).forEach(i ->
        {
            columnType.writeBoolean(blockBuilder, getBoolean(i, columnIdx));
            completedBytes++;
        });
    }

    private void writeLongBlock(BlockBuilder blockBuilder, Type columnType, int columnIdx, DataType pinotColumnType)
    {
        IntStream.rangeClosed(0, currentDataTable.getDataTable().getNumberOfRows() - 1).forEach(i ->
        {
            columnType.writeLong(blockBuilder, getLong(i, columnIdx));
            completedBytes += pinotColumnType.size();
        });
    }

    private void writeDoubleBlock(BlockBuilder blockBuilder, Type columnType, int columnIdx, DataType pinotColumnType)
    {
        IntStream.rangeClosed(0, currentDataTable.getDataTable().getNumberOfRows() - 1).forEach(i ->
        {
            columnType.writeDouble(blockBuilder, getDouble(i, columnIdx));
            completedBytes += pinotColumnType.size();
        });
    }

    private void writeSliceBlock(BlockBuilder blockBuilder, Type columnType, int columnIdx)
    {
        IntStream.rangeClosed(0, currentDataTable.getDataTable().getNumberOfRows() - 1).forEach(i ->
        {
            Slice slice = getSlice(i, columnIdx);
            columnType.writeSlice(blockBuilder, slice, 0, slice.length());
            completedBytes += slice.getBytes().length;
        });
    }

    Type getType(int colIdx)
    {
        checkArgument(colIdx < columnHandles.size(), "Invalid field index");
        return columnHandles.get(colIdx).getColumnType();
    }

    boolean getBoolean(int rowIdx, int colIdx)
    {
        return currentDataTable.getDataTable().getBoolean(rowIdx, colIdx);
    }

    long getLong(int rowIdx, int colIdx)
    {
        DataType dataType = currentDataTable.getDataTable().getDataSchema().getColumnType(colIdx);
        // Note columnType in the dataTable could be different from the original columnType in the columnHandle.
        // e.g. when original column type is int/long and aggregation value is requested, the returned dataType from Pinot would be double.
        // So need to cast it back to the original columnType.
        if (dataType.equals(DataType.DOUBLE)) {
            return (long) currentDataTable.getDataTable().getDouble(rowIdx, colIdx);
        }
        if (getType(colIdx).equals(INTEGER)) {
            return currentDataTable.getDataTable().getInt(rowIdx, colIdx);
        }
        else {
            return currentDataTable.getDataTable().getLong(rowIdx, colIdx);
        }
    }

    double getDouble(int rowIdx, int colIdx)
    {
        DataType dataType = currentDataTable.getDataTable().getDataSchema().getColumnType(colIdx);
        if (dataType.equals(DataType.FLOAT)) {
            return currentDataTable.getDataTable().getFloat(rowIdx, colIdx);
        }
        else {
            return currentDataTable.getDataTable().getDouble(rowIdx, colIdx);
        }
    }

    Slice getSlice(int rowIdx, int colIdx)
    {
        checkColumnType(colIdx, VARCHAR);
        DataType columnType = currentDataTable.getDataTable().getDataSchema().getColumnType(colIdx);
        switch (columnType) {
            case INT_ARRAY:
                int[] intArray = currentDataTable.getDataTable().getIntArray(rowIdx, colIdx);
                return utf8Slice(Arrays.toString(intArray));
            case LONG_ARRAY:
                long[] longArray = currentDataTable.getDataTable().getLongArray(rowIdx, colIdx);
                return utf8Slice(Arrays.toString(longArray));
            case FLOAT_ARRAY:
                float[] floatArray = currentDataTable.getDataTable().getFloatArray(rowIdx, colIdx);
                return utf8Slice(Arrays.toString(floatArray));
            case DOUBLE_ARRAY:
                double[] doubleArray = currentDataTable.getDataTable().getDoubleArray(rowIdx, colIdx);
                return utf8Slice(Arrays.toString(doubleArray));
            case STRING_ARRAY:
                String[] stringArray = currentDataTable.getDataTable().getStringArray(rowIdx, colIdx);
                return utf8Slice(Arrays.toString(stringArray));
            case STRING:
                String fieldStr = currentDataTable.getDataTable().getString(rowIdx, colIdx);
                if (fieldStr == null || fieldStr.isEmpty()) {
                    return Slices.EMPTY_SLICE;
                }
                return Slices.utf8Slice(fieldStr);
        }
        return Slices.EMPTY_SLICE;
    }

    /**
     * Get estimated size in bytes for the Pinot column.
     * Deterministic for numeric fields; use estimate for other types to save calculation.
     *
     * @param dataType FieldSpec.dataType for Pinot column.
     * @return estimated size in bytes.
     */
    private int getEstimatedColumnSizeInBytes(DataType dataType)
    {
        if (dataType.isNumber()) {
            return dataType.getStoredType().size();
        }
        else {
            return pinotConfig.getEstimatedSizeInBytesForNonNumericColumn();
        }
    }

    void checkColumnType(int colIdx, Type expected)
    {
        Type actual = getType(colIdx);
        checkArgument(actual.equals(expected), "Expected column %s to be type %s but is %s", colIdx, expected, actual);
    }

    private class PinotDataTableWithSize
    {
        DataTable dataTable;
        int estimatedSizeInBytes;

        PinotDataTableWithSize(DataTable dataTable, int estimatedSizeInBytes)
        {
            this.dataTable = dataTable;
            this.estimatedSizeInBytes = estimatedSizeInBytes;
        }

        DataTable getDataTable()
        {
            return dataTable;
        }

        int getEstimatedSizeInBytes()
        {
            return estimatedSizeInBytes;
        }
    }
}
