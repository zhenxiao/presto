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
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;

import javax.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RedirectManager
{
    public static final JsonCodec<RedirectRulesSpec> CODEC = new JsonCodecFactory(
            () -> new ObjectMapperProvider().get().enable(FAIL_ON_UNKNOWN_PROPERTIES))
            .jsonCodec(RedirectRulesSpec.class);

    private final Map<String, URI> redirectRules;

    @Inject
    public RedirectManager(RedirectConfig config)
    {
        String filename = config.getConfigFile();
        RedirectRulesSpec redirectRulesSpec;
        if (!filename.isEmpty()) {
            try {
                redirectRulesSpec = CODEC.fromJson(Files.readAllBytes(Paths.get(filename)));
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            catch (IllegalArgumentException e) {
                Throwable cause = e.getCause();
                if (cause instanceof UnrecognizedPropertyException) {
                    UnrecognizedPropertyException ex = (UnrecognizedPropertyException) cause;
                    String message = format("Unknown property at line %s:%s: %s",
                            ex.getLocation().getLineNr(),
                            ex.getLocation().getColumnNr(),
                            ex.getPropertyName());
                    throw new IllegalArgumentException(message, e);
                }
                if (cause instanceof JsonMappingException) {
                    // remove the extra "through reference chain" message
                    if (cause.getCause() != null) {
                        cause = cause.getCause();
                    }
                    throw new IllegalArgumentException(cause.getMessage(), e);
                }
                throw e;
            }
        }
        else {
            redirectRulesSpec = new RedirectRulesSpec(ImmutableList.of());
        }
        redirectRules = redirectRulesSpec.getRedirectRules().stream().collect(Collectors.toMap(RedirectRule::getUser, RedirectRule::getHostname));
    }

    public Optional<URI> getMatch(String user)
    {
        return Optional.ofNullable(redirectRules.get(user));
    }

    public static class RedirectRulesSpec
    {
        private final List<RedirectRule> redirectRules;

        @JsonCreator
        public RedirectRulesSpec(@JsonProperty("redirectRules") List<RedirectRule> rules)
        {
            this.redirectRules = ImmutableList.copyOf(requireNonNull(rules, "rules is null"));
        }

        @JsonProperty
        public List<RedirectRule> getRedirectRules()
        {
            return redirectRules;
        }
    }
}
