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
package com.facebook.presto.server;

import com.google.common.base.Ticker;
import io.airlift.stats.TimeStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.concurrent.TimeUnit;

public class StatementProgressRecorder
{
    private final Ticker ticker = Ticker.systemTicker();

    private final TimeStat startToSchedulingQueryCreate = new TimeStat(TimeUnit.MICROSECONDS);
    private final TimeStat schedulingToQueryCreated = new TimeStat(TimeUnit.MICROSECONDS);
    private final TimeStat queryCreatedToLastResult = new TimeStat(TimeUnit.MICROSECONDS);
    private final TimeStat lastResultToResponseBuilt = new TimeStat(TimeUnit.MICROSECONDS);

    public class Instance
    {
        private final long start = ticker.read();
        private long schedulingQueryCreate;
        private long queryCreateTime;
        private long gotLastResultTime;
        private long responseBuiltTime;

        private void flush()
        {
            if (schedulingQueryCreate > 0) {
                startToSchedulingQueryCreate.add(schedulingQueryCreate - start, TimeUnit.NANOSECONDS);
            }
            else {
                return;
            }

            if (queryCreateTime > 0) {
                schedulingToQueryCreated.add(queryCreateTime - schedulingQueryCreate, TimeUnit.NANOSECONDS);
            }
            else {
                return;
            }

            if (gotLastResultTime > 0) {
                queryCreatedToLastResult.add(gotLastResultTime - queryCreateTime, TimeUnit.NANOSECONDS);
            }
            else {
                return;
            }

            if (responseBuiltTime > 0) {
                lastResultToResponseBuilt.add(responseBuiltTime - gotLastResultTime, TimeUnit.NANOSECONDS);
            }
        }

        public void recordResponseBuilt()
        {
            responseBuiltTime = ticker.read();
            flush();
        }

        public void schedulingQueryCreate()
        {
            schedulingQueryCreate = ticker.read();
        }

        public void creatingQuery()
        {
            queryCreateTime = ticker.read();
        }

        public void gotLastResult()
        {
            gotLastResultTime = ticker.read();
        }
    }

    public Instance create()
    {
        return new Instance();
    }

    @Managed
    @Nested
    public TimeStat getStartToSchedulingQueryCreate()
    {
        return startToSchedulingQueryCreate;
    }

    @Managed
    @Nested
    public TimeStat getSchedulingToQueryCreated()
    {
        return schedulingToQueryCreated;
    }

    @Managed
    @Nested
    public TimeStat getQueryCreatedToLastResult()
    {
        return queryCreatedToLastResult;
    }

    @Managed
    @Nested
    public TimeStat getLastResultToResponseBuilt()
    {
        return lastResultToResponseBuilt;
    }
}
