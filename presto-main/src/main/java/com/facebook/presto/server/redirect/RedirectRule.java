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

import java.net.URI;

import static java.util.Objects.requireNonNull;

public class RedirectRule
{
    private final URI hostname;
    private final String user;

    @JsonCreator
    public RedirectRule(@JsonProperty("hostname") String hostname, @JsonProperty("user") String user)
    {
        this.hostname = URI.create(requireNonNull(hostname, "hostname is null"));
        this.user = requireNonNull(user, "user is null");
    }

    public URI getHostname()
    {
        return hostname;
    }

    public String getUser()
    {
        return user;
    }
}
