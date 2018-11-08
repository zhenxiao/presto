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
import com.facebook.presto.server.redirect.RedirectRule;
import com.facebook.presto.server.redirect.RedirectRulesSpec;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.google.common.net.MediaType;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler;
import io.airlift.http.client.jetty.JettyHttpClient;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.ws.rs.core.HttpHeaders;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.net.URI;
import java.util.Map;

import static com.facebook.presto.SystemSessionProperties.QUERY_SUBMIT_USER;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SOURCE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.facebook.presto.server.redirect.RedirectRulesSpec.CODEC;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.nio.charset.StandardCharsets.UTF_8;
import static javax.ws.rs.core.Response.Status.OK;
import static org.testng.Assert.assertEquals;

public class TestRedirect
{
    private final HttpClient client = new JettyHttpClient();
    private TestingPrestoServer primaryServer;
    private TestingPrestoServer secondaryServer;

    private final String dummy = "dummy";
    private final String userName = "test";

    private final int maxTasks = 100;

    private final RedirectRulesSpec defaultRules;
    private final RedirectRulesSpec redirectNothing;

    public TestRedirect()
            throws Exception
    {
        secondaryServer = new TestingPrestoServer();
        String hostname = secondaryServer.getBaseUrl().toString();
        redirectNothing = new RedirectRulesSpec(ImmutableList.of(), null);
        defaultRules = new RedirectRulesSpec(ImmutableList.of(new RedirectRule(hostname, userName)), new MaxTasksRule(hostname, maxTasks));
        primaryServer = createRedirectPrestoServer(defaultRules);
    }

    @AfterClass(alwaysRun = true)
    public void tearDownServer()
            throws Exception
    {
        primaryServer.close();
        secondaryServer.close();
    }

    @BeforeMethod
    public void resetState()
    {
        updateRedirectRule(primaryServer, redirectNothing);
    }

    @Test
    public void testDefault()
    {
        assertQueryDestination(ImmutableMap.of(PRESTO_USER, dummy), primaryServer);
    }

    @Test
    public void testRedirectOnHeaders()
    {
        updateRedirectRule(primaryServer, defaultRules);
        assertQueryDestination(ImmutableMap.of(PRESTO_USER, userName), secondaryServer);
        assertQueryDestination(ImmutableMap.of(PRESTO_USER, dummy, PRESTO_SESSION, QUERY_SUBMIT_USER + "=" + userName), secondaryServer);
        assertQueryDestination(ImmutableMap.of(PRESTO_USER, dummy, PRESTO_SOURCE, userName), secondaryServer);
    }

    @Test
    public void testRedirectOnTasks()
    {
        updateRedirectRule(primaryServer, defaultRules);
        assertEquals(primaryServer.getQueryManager().getStats().getTotalTasks(), 0);
        assertQueryDestination(ImmutableMap.of(PRESTO_USER, dummy), primaryServer);

        primaryServer.getQueryManager().getStats().updateTasks(maxTasks + 1);

        assertQueryDestination(ImmutableMap.of(PRESTO_USER, dummy), secondaryServer);
    }

    @Test
    public void testRedirectBasedOnRegex()
    {
        updateRedirectRule(primaryServer, redirectNothing);
        assertQueryDestination(ImmutableMap.of(PRESTO_USER, userName), primaryServer);

        // test redirect all but user foo
        RedirectRulesSpec redirectEverythingButFoo = new RedirectRulesSpec(ImmutableList.of(new RedirectRule(secondaryServer.getBaseUrl().toString(), "^(?!foo).*$")), null);
        updateRedirectRule(primaryServer, redirectEverythingButFoo);
        assertQueryDestination(ImmutableMap.of(PRESTO_USER, dummy), secondaryServer);
        assertQueryDestination(ImmutableMap.of(PRESTO_USER, userName), secondaryServer);
        assertQueryDestination(ImmutableMap.of(PRESTO_USER, "foo"), primaryServer);
        assertQueryDestination(ImmutableMap.of(PRESTO_USER, dummy, PRESTO_SOURCE, userName), secondaryServer);
    }

    private void updateRedirectRule(TestingPrestoServer server, RedirectRulesSpec spec)
    {
        URI uri = uriBuilderFrom(server.resolve("/v1/statement/redirect")).build();
        Request request = preparePost()
                .setUri(uri)
                .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.JSON_UTF_8.toString())
                .setBodyGenerator(jsonBodyGenerator(CODEC, spec))
                .build();
        StringResponseHandler.StringResponse response = client.execute(request, createStringResponseHandler());
        assertEquals(OK.getStatusCode(), response.getStatusCode());
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

    private TestingPrestoServer createRedirectPrestoServer(RedirectRulesSpec spec)
            throws Exception
    {
        File tempFile = File.createTempFile("pattern", ".suffix");
        BufferedWriter out = new BufferedWriter(new FileWriter(tempFile));
        out.write(CODEC.toJson(spec));
        out.close();
        return new TestingPrestoServer(true, ImmutableMap.of("redirect.config-file", tempFile.getPath()), null, null, new SqlParserOptions(), ImmutableList.of());
    }
}
