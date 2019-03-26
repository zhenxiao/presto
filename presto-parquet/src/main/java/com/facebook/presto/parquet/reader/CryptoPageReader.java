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
import com.facebook.presto.parquet.DataPageV1;
import com.facebook.presto.parquet.DataPageV2;
import com.facebook.presto.parquet.DictionaryPage;
import io.airlift.slice.Slice;
import org.apache.parquet.crypto.AesDecryptor;
import org.apache.parquet.crypto.AesEncryptor;
import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.parquet.ParquetCompressionUtils.decompress;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.Math.toIntExact;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

/**
 * TODO: Merge this class with PageReader once decryption code is baked well.
 */
class CryptoPageReader
        extends PageReader
{
    static final int INT_LENGTH = 4;

    private final BlockCipher.Decryptor blockDecryptor;
    private final String[] columnPath;

    private short pageOrdinal; // TODO r[eplace ordinal with pageIndex
    private byte[] dataPageAAD;
    private byte[] dictionaryPageAAD;

    public CryptoPageReader(CompressionCodecName codec,
                            List<DataPage> compressedPages,
                            DictionaryPage compressedDictionaryPage,
                            String[] columnPath,
                            BlockCipher.Decryptor blockDecryptor,
                            byte[] fileAAD,
                            short rowGroupOrdinal,
                            short columnOrdinal)
    {
        super(codec, compressedPages, compressedDictionaryPage);
        this.blockDecryptor = blockDecryptor;
        this.columnPath = columnPath;
        this.pageOrdinal = -1;
        if (null != blockDecryptor) {
            dataPageAAD = AesEncryptor.createModuleAAD(fileAAD, AesEncryptor.DataPage, rowGroupOrdinal, columnOrdinal, pageOrdinal);
            dictionaryPageAAD = AesEncryptor.createModuleAAD(fileAAD, AesEncryptor.DictionaryPage, rowGroupOrdinal, columnOrdinal, (short) -1);
        }
    }

    @Override
    public DataPage readPage()
    {
        pageOrdinal++;

        if (compressedPages.isEmpty()) {
            return null;
        }
        if (null != blockDecryptor) {
            AesEncryptor.quickUpdatePageAAD(dataPageAAD, pageOrdinal);
        }
        DataPage compressedPage = compressedPages.remove(0);
        try {
            Slice slice = decryptSlice(compressedPage.getSlice(), dataPageAAD);
            if (compressedPage instanceof DataPageV1) {
                DataPageV1 dataPageV1 = (DataPageV1) compressedPage;
                return new DataPageV1(
                        decompress(codec, slice, dataPageV1.getUncompressedSize()),
                        dataPageV1.getValueCount(),
                        dataPageV1.getUncompressedSize(),
                        dataPageV1.getStatistics(),
                        dataPageV1.getRepetitionLevelEncoding(),
                        dataPageV1.getDefinitionLevelEncoding(),
                        dataPageV1.getValueEncoding());
            }
            else {
                DataPageV2 dataPageV2 = (DataPageV2) compressedPage;
                if (!dataPageV2.isCompressed()) {
                    return dataPageV2;
                }
                int uncompressedSize = toIntExact(dataPageV2.getUncompressedSize()
                        - dataPageV2.getDefinitionLevels().length()
                        - dataPageV2.getRepetitionLevels().length());
                return new DataPageV2(
                        dataPageV2.getRowCount(),
                        dataPageV2.getNullCount(),
                        dataPageV2.getValueCount(),
                        dataPageV2.getRepetitionLevels(),
                        dataPageV2.getDefinitionLevels(),
                        dataPageV2.getDataEncoding(),
                        decompress(codec, slice, uncompressedSize),
                        dataPageV2.getUncompressedSize(),
                        dataPageV2.getStatistics(),
                        false);
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Could not decompress page", e);
        }
    }

    @Override
    public DictionaryPage readDictionaryPage()
    {
        if (compressedDictionaryPage == null) {
            return null;
        }
        try {
            Slice slice = decryptSlice(compressedDictionaryPage.getSlice(), dictionaryPageAAD);
            return new DictionaryPage(
                    decompress(codec, slice, compressedDictionaryPage.getUncompressedSize()),
                    compressedDictionaryPage.getDictionarySize(),
                    compressedDictionaryPage.getEncoding());
        }
        catch (IOException e) {
            throw new RuntimeException("Error reading dictionary page", e);
        }
    }

    private Slice decryptSlice(Slice slice, byte[] aad) throws IOException
    {
        if (null != blockDecryptor) {
            int offset = (int) slice.getAddress() - ARRAY_BYTE_BASE_OFFSET + INT_LENGTH;
            int length = slice.length() - INT_LENGTH;
            byte[] plainText = ((AesDecryptor) blockDecryptor).decrypt((byte[]) slice.getBase(), offset, length, aad);
            return wrappedBuffer(plainText);
        }
        else {
            return slice;
        }
    }
}
