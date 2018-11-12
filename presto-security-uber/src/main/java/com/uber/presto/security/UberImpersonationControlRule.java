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
package com.uber.presto.security;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

public class UberImpersonationControlRule
{
    private final Optional<Pattern> principalRegex;
    private final Optional<Pattern> userRegex;
    private final boolean allow;

    @JsonCreator
    public UberImpersonationControlRule(
            @JsonProperty("principal") Optional<Pattern> principal,
            @JsonProperty("user") Optional<Pattern> user,
            @JsonProperty("allow") boolean allow)
    {
        this.principalRegex = requireNonNull(principal, "principalRegex is null");
        this.userRegex = requireNonNull(user, "userRegex is null");
        this.allow = allow;
    }

    public Optional<Boolean> match(String principal, String user)
    {
        if (principalRegex.map(regex -> regex.matcher(principal).matches()).orElse(true) &&
                userRegex.map(regex -> regex.matcher(user).matches()).orElse(true)) {
            return Optional.of(allow);
        }
        return Optional.empty();
    }
}
