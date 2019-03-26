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

import com.facebook.presto.parquet.DataPage;
import com.facebook.presto.parquet.DictionaryPage;
import com.facebook.presto.parquet.ParquetCorruptionException;
import org.apache.parquet.crypto.AesEncryptor;
import org.apache.parquet.crypto.InternalColumnDecryptionSetup;
import org.apache.parquet.crypto.InternalFileDecryptor;
import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.metadata.ColumnPath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * TODO: Merge this class with ParquetColumnChunk once decryption code is baked well.
 */
public class CryptoParquetColumnChunk
        extends ParquetColumnChunk
{
    private final InternalFileDecryptor fileDecryptor;

    public CryptoParquetColumnChunk(
            ColumnChunkDescriptor descriptor,
            byte[] data,
            int offset,
            InternalFileDecryptor fileDecryptor)
    {
        super(descriptor, data, offset);
        this.fileDecryptor = fileDecryptor;
    }

    protected PageHeader readPageHeader(BlockCipher.Decryptor headerBlockDecryptor, byte[] pageHeaderAAD)
            throws IOException
    {
        PageHeader pageHeader = Util.readPageHeader(this, headerBlockDecryptor, pageHeaderAAD);
        return pageHeader;
    }

    /**
     * Read all pages with starting row group ordinal and column ordinal count. This method is used only when fileDecryptor
     * is set. If fileDecryptor is null, this method will throw exception.
     * @param rowGroupOrdinal
     * @param columnOrdinal
     * @return
     * @throws IOException
     */
    public PageReader readAllPages(short rowGroupOrdinal, short columnOrdinal)
            throws IOException
    {
        if (fileDecryptor == null) {
            throw new IOException("InternalFileDecryptor is not set");
        }
        List<DataPage> pages = new ArrayList<>();
        DictionaryPage dictionaryPage = null;
        long valueCount = 0;
        short pageOrdinal = 0;
        byte[] dataPageHeaderAAD = null;
        ColumnPath columnPath = ColumnPath.get(descriptor.getColumnDescriptor().getPath());
        InternalColumnDecryptionSetup columnDecryptionSetup = fileDecryptor.getColumnSetup(columnPath);
        BlockCipher.Decryptor headerBlockDecryptor = columnDecryptionSetup.getMetaDataDecryptor();
        if (null != headerBlockDecryptor) {
            dataPageHeaderAAD = AesEncryptor.createModuleAAD(fileDecryptor.getFileAAD(), AesEncryptor.DataPageHeader, rowGroupOrdinal, columnOrdinal, pageOrdinal);
        }

        while (valueCount < descriptor.getColumnChunkMetaData().getValueCount()) {
            byte[] pageHeaderAAD = dataPageHeaderAAD;
            if (null != headerBlockDecryptor) {
                // Important: this verifies file integrity (makes sure dictionary page had not been removed)
                if (null == dictionaryPage && descriptor.getColumnChunkMetaData().hasDictionaryPage()) {
                    pageHeaderAAD = AesEncryptor.createModuleAAD(fileDecryptor.getFileAAD(), AesEncryptor.DictionaryPageHeader, rowGroupOrdinal, columnOrdinal, (short) -1);
                }
                else {
                    AesEncryptor.quickUpdatePageAAD(dataPageHeaderAAD, pageOrdinal);
                }
            }
            PageHeader pageHeader = readPageHeader(headerBlockDecryptor, pageHeaderAAD);
            int uncompressedPageSize = pageHeader.getUncompressed_page_size();
            int compressedPageSize = pageHeader.getCompressed_page_size();
            switch (pageHeader.type) {
                case DICTIONARY_PAGE:
                    if (dictionaryPage != null) {
                        throw new ParquetCorruptionException("%s has more than one dictionary page in column chunk", descriptor.getColumnDescriptor());
                    }
                    dictionaryPage = readDictionaryPage(pageHeader, uncompressedPageSize, compressedPageSize);
                    break;
                case DATA_PAGE:
                    pageOrdinal++;
                    valueCount += readDataPageV1(pageHeader, uncompressedPageSize, compressedPageSize, pages);
                    break;
                case DATA_PAGE_V2:
                    pageOrdinal++;
                    valueCount += readDataPageV2(pageHeader, uncompressedPageSize, compressedPageSize, pages);
                    break;
                default:
                    skip(compressedPageSize);
                    break;
            }
        }
        return new CryptoPageReader(descriptor.getColumnChunkMetaData().getCodec(),
                pages,
                dictionaryPage,
                descriptor.getColumnDescriptor().getPath(),
                columnDecryptionSetup.getDataDecryptor(),
                fileDecryptor.getFileAAD(),
                rowGroupOrdinal,
                columnOrdinal);
    }
}
