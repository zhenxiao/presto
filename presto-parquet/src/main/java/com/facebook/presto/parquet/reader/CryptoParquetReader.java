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
package com.facebook.presto.parquet.reader;

import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.parquet.ParquetDataSource;
import com.facebook.presto.parquet.PrimitiveField;
import com.facebook.presto.spi.PrestoException;
import com.google.common.base.Joiner;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.crypto.InternalFileDecryptor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.MessageColumnIO;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.parquet.ParquetValidationUtils.validateParquet;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_SCHEMA_PROPERTY;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;

/**
 * TODO: Merge this class with ParquetReader once decryption code is baked well.
 */
public class CryptoParquetReader
        extends ParquetReader
{
    private final InternalFileDecryptor fileDecryptor;

    public CryptoParquetReader(MessageColumnIO messageColumnIO,
                               List<BlockMetaData> blocks,
                               ParquetDataSource dataSource,
                               AggregatedMemoryContext systemMemoryContext,
                               InternalFileDecryptor fileDecryptor)
    {
        super(messageColumnIO, blocks, dataSource, systemMemoryContext);
        this.fileDecryptor = fileDecryptor;
    }

    @Override
    protected ColumnChunk readPrimitive(PrimitiveField field)
            throws IOException
    {
        ColumnDescriptor columnDescriptor = field.getDescriptor();
        PrimitiveColumnReader columnReader = columnReaders[field.getId()];
        if (columnReader.getPageReader() == null) {
            validateParquet(currentBlockMetadata.getRowCount() > 0, "Row group has 0 rows");
            ColumnChunkMetaData metadata = getColumnChunkMetaData(columnDescriptor);
            long startingPosition = metadata.getStartingPos();
            int totalSize = toIntExact(metadata.getTotalSize());
            byte[] buffer = allocateBlock(totalSize);
            dataSource.readFully(startingPosition, buffer);
            ColumnChunkDescriptor descriptor = new ColumnChunkDescriptor(columnDescriptor, metadata, totalSize);
            CryptoParquetColumnChunk columnChunk = new CryptoParquetColumnChunk(descriptor, buffer, 0, fileDecryptor);
            if (fileDecryptor == null) {
                columnReader.setPageReader(columnChunk.readAllPages());
            }
            else {
                short columnOrdinal = fileDecryptor.getColumnSetup(descriptor.getColumnChunkMetaData().getPath()).getOrdinal();
                columnReader.setPageReader(columnChunk.readAllPages(currentBlockMetadata.getOrdinal(), columnOrdinal));
            }
        }
        try {
            return columnReader.readPrimitive(field);
        }
        catch (UnsupportedOperationException e) {
            throw new PrestoException(INVALID_SCHEMA_PROPERTY, format("There is a mismatch between file schema and partition schema. " +
                                                                "The column %s in file %s is declared as type %s but parquet file declared column type as %s.",
                                                                Joiner.on(".").join(columnDescriptor.getPath()),
                                                                dataSource.getId(),
                                                                field.getType(),
                                                                columnDescriptor.getType()));
        }
    }
}
