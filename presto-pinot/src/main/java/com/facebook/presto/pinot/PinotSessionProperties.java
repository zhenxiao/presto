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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;

public class PinotSessionProperties
{
    private static final String FORCE_SINGLE_NODE_PLAN = "force_single_node_plan";
    private static final String NUM_SEGMENTS_PER_SPLIT = "num_segments_per_split";
    private static final String CONNECTION_TIMEOUT = "connection_timeout";
    private static final String SCAN_PARALLELISM_ENABLED = "scan_parallelism_enabled";

    private final List<PropertyMetadata<?>> sessionProperties;

    public static boolean isForceSingleNodePlan(ConnectorSession session)
    {
        return session.getProperty(FORCE_SINGLE_NODE_PLAN, Boolean.class);
    }

    public static int getNumSegmentsPerSplit(ConnectorSession session)
    {
        return session.getProperty(NUM_SEGMENTS_PER_SPLIT, Integer.class);
    }

    public static boolean isScanParallelismEnabled(ConnectorSession session)
    {
        return session.getProperty(SCAN_PARALLELISM_ENABLED, Boolean.class);
    }

    public static Duration getConnectionTimeout(ConnectorSession session)
    {
        return session.getProperty(CONNECTION_TIMEOUT, Duration.class);
    }

    @Inject
    public PinotSessionProperties(PinotConfig pinotConfig)
    {
        sessionProperties = ImmutableList.of(
                booleanProperty(
                        FORCE_SINGLE_NODE_PLAN,
                        "Force single node plan",
                        pinotConfig.isForceSingleNodePlan(),
                        true),
                booleanProperty(
                        SCAN_PARALLELISM_ENABLED,
                        "Scan Parallelism enabled",
                        pinotConfig.isScanParallelismEnabled(),
                        false),
                new PropertyMetadata<>(
                        CONNECTION_TIMEOUT,
                        "Connection Timeout to talk to Pinot servers",
                        createUnboundedVarcharType(),
                        Duration.class,
                        pinotConfig.getConnectionTimeout(),
                        false,
                        value -> Duration.valueOf((String) value),
                        Duration::toString),
                new PropertyMetadata<>(
                        NUM_SEGMENTS_PER_SPLIT,
                        "Number of segments of the same host per split",
                        INTEGER,
                        Integer.class,
                        pinotConfig.getNumSegmentsPerSplit(),
                        false,
                        value -> {
                            int ret = ((Number) value).intValue();
                            if (ret <= 0) {
                                throw new IllegalArgumentException("Number of segments per split must be more than zero");
                            }
                            return ret;
                        },
                        object -> object));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }
}
