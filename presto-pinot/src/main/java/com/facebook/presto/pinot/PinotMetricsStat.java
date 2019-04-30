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

import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler;
import io.airlift.stats.CounterStat;
import io.airlift.stats.DistributionStat;
import io.airlift.stats.TimeStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.TimeUnit;

import static com.facebook.presto.pinot.PinotUtils.isValidPinotHttpResponseCode;

@ThreadSafe
public class PinotMetricsStat
{
    private final TimeStat time = new TimeStat(TimeUnit.MILLISECONDS);
    private final CounterStat numRequests = new CounterStat();
    private final CounterStat numErrorRequests = new CounterStat();
    private DistributionStat responseSize;

    public PinotMetricsStat(boolean withResponse)
    {
        if (withResponse) {
            responseSize = new DistributionStat();
        }
    }

    void record(long timeTaken, TimeUnit timeUnit, boolean encounteredError)
    {
        time.add(timeTaken, timeUnit);
        numRequests.update(1);
        if (encounteredError) {
            numErrorRequests.update(1);
        }
    }

    void record(Request request, StringResponseHandler.StringResponse response, long duration, TimeUnit timeUnit)
    {
        time.add(duration, timeUnit);
        numRequests.update(1);
        if (isValidPinotHttpResponseCode(response.getStatusCode())) {
            responseSize.add(response.getBody().length());
        }
        else {
            numErrorRequests.update(1);
        }
    }

    @Managed
    @Nested
    public TimeStat getTime()
    {
        return time;
    }

    @Managed
    @Nested
    public CounterStat getNumRequests()
    {
        return numRequests;
    }

    @Managed
    @Nested
    public CounterStat getNumErrorRequests()
    {
        return numErrorRequests;
    }

    @Managed
    @Nested
    public DistributionStat getResponseSize()
    {
        return responseSize;
    }
}
