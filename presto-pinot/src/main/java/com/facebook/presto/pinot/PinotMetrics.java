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
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.TimeUnit;

import static java.util.Locale.ENGLISH;

@ThreadSafe
public class PinotMetrics
{
    // TODO: Is there a way I can have a map<String, Stat> and have that be properly exposed still via jmx with keys as names ?
    private final PinotMetricsStat getStats = new PinotMetricsStat(true);
    private final PinotMetricsStat queryStats = new PinotMetricsStat(true);
    private final PinotMetricsStat tablesStats = new PinotMetricsStat(true);
    private final PinotMetricsStat schemaStats = new PinotMetricsStat(true);
    private final PinotMetricsStat brokerLookupStats = new PinotMetricsStat(false);

    @Managed
    @Nested
    public PinotMetricsStat getQueryStats()
    {
        return queryStats;
    }

    @Managed
    @Nested
    public PinotMetricsStat getGetStats()
    {
        return getStats;
    }

    @Managed
    @Nested
    public PinotMetricsStat getTablesStats()
    {
        return tablesStats;
    }

    @Managed
    @Nested
    public PinotMetricsStat getSchemaStats()
    {
        return schemaStats;
    }

    @Managed
    @Nested
    public PinotMetricsStat getBrokerLookupStats()
    {
        return brokerLookupStats;
    }

    public void recordTableToBroker(long timeTaken, TimeUnit timeUnit, boolean encounteredError)
    {
        brokerLookupStats.record(timeTaken, timeUnit, encounteredError);
    }

    public void monitorRequest(Request request, StringResponseHandler.StringResponse response, long duration, TimeUnit timeUnit)
    {
        String[] split = request.getUri().getPath().split("/");
        String last = split[split.length - 1].toLowerCase(ENGLISH);
        if ("post".equalsIgnoreCase(request.getMethod()) && "query".equalsIgnoreCase(last)) {
            queryStats.record(request, response, duration, timeUnit);
        }
        else if ("get".equalsIgnoreCase(request.getMethod())) {
            switch (last) {
                case "tables":
                    tablesStats.record(request, response, duration, timeUnit);
                    break;
                case "schema":
                    schemaStats.record(request, response, duration, timeUnit);
            }
            getStats.record(request, response, duration, timeUnit);
        }
    }
}
