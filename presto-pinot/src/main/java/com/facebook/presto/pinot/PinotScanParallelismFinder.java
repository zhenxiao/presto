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

import com.facebook.presto.spi.pipeline.AggregationPipelineNode;
import com.facebook.presto.spi.pipeline.LimitPipelineNode;
import com.facebook.presto.spi.pipeline.PipelineNode;
import com.facebook.presto.spi.pipeline.SortPipelineNode;
import com.facebook.presto.spi.pipeline.TableScanPipeline;
import com.facebook.presto.spi.pipeline.TableScanPipelineVisitor;

class PinotScanParallelismFinder
        extends TableScanPipelineVisitor<Boolean, Boolean>
{
    // go through the pipeline operations and see if we parallelize the scan
    public static boolean canParallelize(boolean canParallelize, TableScanPipeline scanPipeline)
    {
        PinotScanParallelismFinder scanParallelismFinder = new PinotScanParallelismFinder();
        for (PipelineNode pipelineNode : scanPipeline.getPipelineNodes()) {
            canParallelize = pipelineNode.accept(scanParallelismFinder, canParallelize);
        }

        return canParallelize;
    }

    @Override
    public Boolean visitNode(PipelineNode node, Boolean canParallelize)
    {
        return canParallelize;
    }

    @Override
    public Boolean visitAggregationNode(AggregationPipelineNode aggregation, Boolean canParallelize)
    {
        return false;
    }

    @Override
    public Boolean visitLimitNode(LimitPipelineNode limit, Boolean canParallelize)
    {
        // we can only parallelize if the limit pushdown is split level (aka partial limit)
        return canParallelize && limit.isPartial();
    }

    @Override
    public Boolean visitSortNode(SortPipelineNode limit, Boolean canParallelize)
    {
        return false;
    }
}
