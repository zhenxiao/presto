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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.pipeline.PipelineNode;
import com.facebook.presto.spi.pipeline.TableScanPipeline;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.PushdownTestUtils;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Locale.ENGLISH;

public class AresDbTestUtils
{
    private AresDbTestUtils()
    {
    }

    public static PipelineNode scan(ConnectorTableHandle tableHandle, List<ColumnHandle> inputColumns)
    {
        List<String> columnNames = inputColumns.stream().map(c -> ((AresDbColumnHandle) c).getColumnName().toLowerCase(ENGLISH)).collect(Collectors.toList());
        return scan(tableHandle, inputColumns, columnNames);
    }

    public static PipelineNode scan(ConnectorTableHandle tableHandle, List<ColumnHandle> inputColumns, List<String> outputColumnNames)
    {
        List<Type> rowType = inputColumns.stream().map(c -> ((AresDbColumnHandle) c).getDataType()).collect(Collectors.toList());
        return PushdownTestUtils.scan(tableHandle, inputColumns, outputColumnNames, rowType);
    }

    public static TableScanPipeline pipeline(PipelineNode... nodes)
    {
        return PushdownTestUtils.pipeline(AresDbMetadata::createDerivedColumnHandles, nodes);
    }
}
