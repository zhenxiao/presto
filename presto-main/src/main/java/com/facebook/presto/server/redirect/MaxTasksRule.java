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
import com.google.common.base.Preconditions;

import java.net.URI;

import static java.util.Objects.requireNonNull;

public class MaxTasksRule
{
    private final URI hostname;
    private final long tasks;

    @JsonCreator
    public MaxTasksRule(@JsonProperty("hostname") String hostname, @JsonProperty("tasks") long tasks)
    {
        this.hostname = URI.create(requireNonNull(hostname, "hostname is null"));
        Preconditions.checkArgument(tasks > 0, "tasks must larger than 0");
        this.tasks = tasks;
    }

    @JsonProperty
    public URI getHostname()
    {
        return hostname;
    }

    @JsonProperty
    public long getTasks()
    {
        return tasks;
    }
}
