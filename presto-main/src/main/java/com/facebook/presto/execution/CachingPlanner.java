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
package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.Plan;

import java.util.List;
import java.util.Optional;

public interface CachingPlanner
{
    class PlanDetails
    {
        private final Plan plan;
        private final List<Input> inputs;
        private final Optional<Output> output;

        public PlanDetails(Plan plan, List<Input> inputs, Optional<Output> output)
        {
            this.plan = plan;
            this.inputs = inputs;
            this.output = output;
        }

        public Plan getPlan()
        {
            return plan;
        }

        public List<Input> getInputs()
        {
            return inputs;
        }

        public Optional<Output> getOutput()
        {
            return output;
        }
    }

    PlanDetails getPlanAndStuff(Analysis analysis, Session session, WarningCollector warningCollector, Metadata metadata, LogicalPlanner.Stage stage);
}
