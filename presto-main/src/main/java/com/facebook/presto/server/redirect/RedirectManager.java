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

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.String.format;

@ThreadSafe
public class RedirectManager
{
    private AtomicReference<List<RedirectRule>> redirectRules = new AtomicReference<>();
    private AtomicReference<Optional<MaxTasksRule>> maxTasksRule = new AtomicReference<>();

    @Inject
    public RedirectManager(RedirectConfig config)
    {
        String filename = config.getConfigFile();
        RedirectRulesSpec redirectRulesSpec;
        if (!filename.isEmpty()) {
            try {
                redirectRulesSpec = RedirectRulesSpec.CODEC.fromJson(Files.readAllBytes(Paths.get(filename)));
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
            redirectRulesSpec = new RedirectRulesSpec(ImmutableList.of(), null);
        }
        reset(redirectRulesSpec);
    }

    public void reset(RedirectRulesSpec redirectRulesSpec)
    {
        List<RedirectRule> redirectRules = ImmutableList.copyOf(redirectRulesSpec.getRedirectRules());
        Optional<MaxTasksRule> clusterMaxTaskRule = Optional.ofNullable(redirectRulesSpec.getClusterMaxTaskRule());

        this.redirectRules.set(redirectRules);
        this.maxTasksRule.set(clusterMaxTaskRule);
    }

    public Optional<URI> getMatch(String user)
    {
        if (user == null) {
            return Optional.empty();
        }
        return redirectRules.get().stream().filter(redirectRule -> redirectRule.matches(user)).map(RedirectRule::getHostname).findFirst();
    }

    public Optional<URI> redirectByMaxTasks(long maxTasks)
    {
        return maxTasksRule.get().filter(maxTasksRule -> maxTasksRule.getTasks() < maxTasks).map(MaxTasksRule::getHostname);
    }
}
