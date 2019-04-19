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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.pipeline.AggregationPipelineNode;
import com.facebook.presto.spi.pipeline.TableScanPipeline;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.SymbolReference;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toMap;

public class PushDownUtils
{
    private PushDownUtils()
    {
    }

    static boolean areInputsSymbolReferences(FunctionCall function)
    {
        // make sure all function inputs are null or symbol references
        return function.getArguments().stream()
                .allMatch(arg -> arg instanceof SymbolReference);
    }

    /**
     * Extract inputs column references from given function arguments
     */
    static List<String> getInputColumns(FunctionCall function)
    {
        checkArgument(function.getArguments().stream()
                .allMatch(argument -> argument != null &&
                        argument instanceof SymbolReference));

        return function.getArguments().stream()
                .filter(argument -> argument != null)
                .map(argument -> ((SymbolReference) argument).getName())
                .collect(Collectors.toList());
    }

    /**
     * Convert planner based objects into connector format
     */
    static Optional<AggregationPipelineNode> convertAggregationToPushDownFormat(boolean isPartial, List<Symbol> aggOutputSymbols, Map<Symbol, AggregationNode.Aggregation> aggregations,
            List<Symbol> groupByKeys, List<Symbol> finalOutputSymbols, TypeProvider typeProvider)
    {
        int groupByKeyIndex = 0;
        AggregationPipelineNode aggPipelineNode = new AggregationPipelineNode(isPartial);
        for (int fieldId = 0; fieldId < finalOutputSymbols.size(); fieldId++) {
            Symbol aggOutputSymbol = aggOutputSymbols.get(fieldId);
            Symbol finalOutputSymbol = finalOutputSymbols.get(fieldId);
            AggregationNode.Aggregation agg = aggregations.get(aggOutputSymbol);

            if (agg != null) {
                FunctionCall aggFunction = agg.getCall();
                if (!areInputsSymbolReferences(aggFunction)) {
                    return Optional.empty();
                }
                // aggregation output
                aggPipelineNode.addAggregation(
                        getInputColumns(aggFunction),
                        agg.getCall().getName().toString(),
                        finalOutputSymbol.getName(),
                        typeProvider.get(finalOutputSymbol));
            }
            else {
                // group by output
                Symbol inputSymbol = groupByKeys.get(groupByKeyIndex);
                aggPipelineNode.addGroupBy(inputSymbol.getName(), finalOutputSymbol.getName(), typeProvider.get(aggOutputSymbol));
                groupByKeyIndex++;
            }
        }

        return Optional.of(aggPipelineNode);
    }

    static TableScanNode newTableScanWithPipeline(TableScanNode existingNode, PlanNodeId newPlanNodeId, List<Symbol> newOutputSymbols, TableScanPipeline scanPipeline)
    {
        // Check the newOutputSymbols size and outputColumnHandles list size in scanPipeline are the same
        checkArgument(scanPipeline.getOutputColumnHandles().size() == newOutputSymbols.size(), "Mismatch in row size");
        Map<Symbol, ColumnHandle> newAssignments = IntStream.range(0, newOutputSymbols.size())
                .boxed()
                .collect(toMap(newOutputSymbols::get, scanPipeline.getOutputColumnHandles()::get));

        return new TableScanNode(
                newPlanNodeId,
                existingNode.getTable(),
                newOutputSymbols,
                newAssignments,
                existingNode.getLayout(),
                existingNode.getCurrentConstraint(),
                existingNode.getEnforcedConstraint(),
                Optional.of(scanPipeline));
    }
}
