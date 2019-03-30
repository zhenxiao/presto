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
package com.facebook.presto.server.redirect;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;

import javax.annotation.Nullable;

import java.util.List;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static java.util.Objects.requireNonNull;

public class RedirectRulesSpec
{
    public static final JsonCodec<RedirectRulesSpec> CODEC = new JsonCodecFactory(
            () -> new ObjectMapperProvider().get().enable(FAIL_ON_UNKNOWN_PROPERTIES))
            .jsonCodec(RedirectRulesSpec.class);

    private final List<RedirectRule> redirectRules;
    private final MaxTasksRule clusterMaxTaskRule;

    @JsonCreator
    public RedirectRulesSpec(@JsonProperty("redirectRules") List<RedirectRule> rules, @JsonProperty("clusterMaxTaskRule") @Nullable MaxTasksRule maxTasksRule)
    {
        this.redirectRules = ImmutableList.copyOf(requireNonNull(rules, "rules is null"));
        this.clusterMaxTaskRule = maxTasksRule;
    }

    @JsonProperty
    public List<RedirectRule> getRedirectRules()
    {
        return redirectRules;
    }

    @JsonProperty
    public MaxTasksRule getClusterMaxTaskRule()
    {
        return clusterMaxTaskRule;
    }
}
