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
package com.facebook.presto.eventlistener;

import io.airlift.stats.TimeStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.concurrent.TimeUnit;

@ThreadSafe
public class EventListenerStats
{
    private final TimeStat queryCreateEventProcessTime = new TimeStat(TimeUnit.MILLISECONDS);
    private final TimeStat queryCompleteEventProcessTime = new TimeStat(TimeUnit.MILLISECONDS);
    private final TimeStat splitCompleteEventProcessTime = new TimeStat(TimeUnit.MILLISECONDS);

    @Inject
    public EventListenerStats()
    {
    }

    @Managed
    @Nested
    public TimeStat getQueryCreateEventProcessTime()
    {
        return queryCreateEventProcessTime;
    }

    @Managed
    @Nested
    public TimeStat getQueryCompleteEventProcessTime()
    {
        return queryCompleteEventProcessTime;
    }

    @Managed
    @Nested
    public TimeStat getSplitCompleteEventProcessTime()
    {
        return splitCompleteEventProcessTime;
    }
}
