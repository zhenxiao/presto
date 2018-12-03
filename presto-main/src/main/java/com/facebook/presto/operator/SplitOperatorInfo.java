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
package com.facebook.presto.operator;

import com.facebook.presto.util.Mergeable;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class SplitOperatorInfo
        implements Mergeable<SplitOperatorInfo>, OperatorInfo
{
    // NOTE: this deserializes to a map instead of the expected type
    private final Object splitInfo;

    @JsonCreator
    public SplitOperatorInfo(
            @JsonProperty("splitInfo") Object splitInfo)
    {
        this.splitInfo = splitInfo;
    }

    @Override
    public boolean isFinal()
    {
        return true;
    }

    @JsonProperty
    public Object getSplitInfo()
    {
        return splitInfo;
    }

    @Override
    public SplitOperatorInfo mergeWith(SplitOperatorInfo other)
    {
        if (splitInfo instanceof Map && other.splitInfo instanceof Map) {
            Map splitInfoMap = (Map) splitInfo;
            Map otherSplitInfoMap = (Map) other.splitInfo;

            if (checkEqual(splitInfoMap, otherSplitInfoMap, "database") && checkEqual(splitInfoMap, otherSplitInfoMap, "table")) {
                return new SplitOperatorInfo(ImmutableMap.of("database", splitInfoMap.get("database"), "table", splitInfoMap.get("table")));
            }
        }
        return null;
    }

    private boolean checkEqual(Map map1, Map map2, String key)
    {
        return map1.containsKey(key) && map2.containsKey(key) && map1.get(key).equals(map2.get(key));
    }
}
