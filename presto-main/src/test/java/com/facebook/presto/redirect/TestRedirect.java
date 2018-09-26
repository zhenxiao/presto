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
package com.facebook.presto.redirect;

import com.facebook.presto.client.QueryResults;
import com.facebook.presto.server.redirect.MaxTasksRule;
import com.facebook.presto.server.redirect.RedirectManager;
import com.facebook.presto.server.redirect.RedirectRule;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.jetty.JettyHttpClient;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.net.URI;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.SystemSessionProperties.QUERY_SUBMIT_USER;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SOURCE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;

public class TestRedirect
{
    private final HttpClient client = new JettyHttpClient();
    private TestingPrestoServer primaryServer;
    private TestingPrestoServer secondaryServer;

    private final String dummy = "dummy";
    private final String userName = "test";

    private final int maxTasks = 100;

    public TestRedirect()
            throws Exception
    {
        secondaryServer = new TestingPrestoServer();
        String hostname = secondaryServer.getBaseUrl().toString();
        primaryServer = createRedirectPrestoServer(ImmutableList.of(new RedirectRule(hostname, userName)), new MaxTasksRule(hostname, maxTasks));
    }

    @AfterClass(alwaysRun = true)
    public void tearDownServer()
            throws Exception
    {
        primaryServer.close();
        secondaryServer.close();
    }

    @Test
    public void testDefault()
            throws Exception
    {
        TestingPrestoServer redirectPrestoServer = createRedirectPrestoServer(ImmutableList.of(), null);
        assertQueryDestination(ImmutableMap.of(PRESTO_USER, dummy), redirectPrestoServer, redirectPrestoServer);
        redirectPrestoServer.close();
    }

    @Test
    public void testRedirectOnHeaders()
    {
        assertQueryDestination(ImmutableMap.of(PRESTO_USER, dummy), primaryServer);
        assertQueryDestination(ImmutableMap.of(PRESTO_USER, userName), secondaryServer);
        assertQueryDestination(ImmutableMap.of(PRESTO_USER, dummy, PRESTO_SESSION, QUERY_SUBMIT_USER + "=" + userName), secondaryServer);
        assertQueryDestination(ImmutableMap.of(PRESTO_USER, dummy, PRESTO_SOURCE, userName), secondaryServer);
    }

    @Test
    public void testRedirectOnTasks()
    {
        assertEquals(primaryServer.getQueryManager().getStats().getTotalTasks(), 0);
        assertQueryDestination(ImmutableMap.of(PRESTO_USER, dummy), primaryServer);

        primaryServer.getQueryManager().getStats().updateTasks(maxTasks + 1);

        assertQueryDestination(ImmutableMap.of(PRESTO_USER, dummy), secondaryServer);
    }

    private void assertQueryDestination(Map<String, String> headers, TestingPrestoServer testingPrestoServer)
    {
        assertQueryDestination(headers, primaryServer, testingPrestoServer);
    }

    private void assertQueryDestination(Map<String, String> headers, TestingPrestoServer startServer, TestingPrestoServer destinationServer)
    {
        // Query will always go to primary server first
        Request request = createRequest(startServer.getBaseUrl(), headers);
        QueryResults queryResults = client.execute(request, createJsonResponseHandler(jsonCodec(QueryResults.class)));
        assertEquals(HostAndPort.fromParts(queryResults.getNextUri().getHost(), queryResults.getNextUri().getPort()), destinationServer.getAddress());
    }

    private Request createRequest(URI serveruri, Map<String, String> headers)
    {
        URI uri = uriBuilderFrom(serveruri.resolve("/v1/statement")).build();
        Request.Builder builder = preparePost()
                .setUri(uri)
                .setBodyGenerator(createStaticBodyGenerator("select 1", UTF_8));
        headers.forEach(builder::setHeader);
        return builder.build();
    }

    private TestingPrestoServer createRedirectPrestoServer(List<RedirectRule> rules, MaxTasksRule maxTasksRule)
            throws Exception
    {
        File tempFile = File.createTempFile("pattern", ".suffix");
        BufferedWriter out = new BufferedWriter(new FileWriter(tempFile));
        out.write(RedirectManager.CODEC.toJson(new RedirectManager.RedirectRulesSpec(rules, maxTasksRule)));
        out.close();
        return new TestingPrestoServer(true, ImmutableMap.of("redirect.config-file", tempFile.getPath()), null, null, new SqlParserOptions(), ImmutableList.of());
    }
}
