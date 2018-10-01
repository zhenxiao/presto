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
package com.facebook.presto.uber.jdbc;

import com.facebook.presto.client.ClientTypeSignature;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StatementStats;
import com.facebook.presto.client.TestMockWebServerFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.QueueDispatcher;
import okhttp3.mockwebserver.RecordedRequest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.facebook.presto.client.OkHttpUtil.userAgent;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_DELEGATION_TOKEN;
import static com.facebook.presto.client.TestMockWebServerFactory.MockWebServerInfo;
import static com.facebook.presto.uber.jdbc.ConnectionProperties.DELEGATION_TOKEN;
import static com.facebook.presto.uber.jdbc.ConnectionProperties.KERBEROS_CONFIG_PATH;
import static com.facebook.presto.uber.jdbc.ConnectionProperties.KERBEROS_KEYTAB_PATH;
import static com.facebook.presto.uber.jdbc.ConnectionProperties.KERBEROS_PRINCIPAL;
import static com.facebook.presto.uber.jdbc.ConnectionProperties.KERBEROS_REMOTE_SERICE_NAME;
import static com.facebook.presto.uber.jdbc.ConnectionProperties.KERBEROS_USE_CANONICAL_HOSTNAME;
import static com.facebook.presto.uber.jdbc.ConnectionProperties.SSL;
import static com.facebook.presto.uber.jdbc.ConnectionProperties.SSL_TRUST_STORE_PASSWORD;
import static com.facebook.presto.uber.jdbc.ConnectionProperties.SSL_TRUST_STORE_PATH;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * Test authenticating with server using delegation token. The test server here is a mock web server.
 */
public class TestPrestoDriverDelegationTokenAuth
{
    private static final String VALID_DELEGATION_TOKEN = "This is valid token!";
    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);

    private MockWebServerInfo serverInfo;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        serverInfo = TestMockWebServerFactory.getMockWebServerWithSSL();
        serverInfo.server.start();

        setupDispatcher();
    }

    @Test
    public void testValidDelegationToken()
            throws SQLException
    {
        Map<String, String> props = ImmutableMap.of(DELEGATION_TOKEN.getKey(), VALID_DELEGATION_TOKEN);
        testQuery(props);
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Authentication failed: Not Authorized")
    public void testInvalidDelegationToken()
            throws SQLException
    {
        Map<String, String> props = ImmutableMap.of(DELEGATION_TOKEN.getKey(), "invalid token");
        testQuery(props);
    }

    /**
     * Test connection with both Kerberos and delegation token. Expect the JDBC client to ignore delegation token (i.e
     * no interceptor for delegation token). The test here tries to send a request and it looks at the headers added
     * to request and expects no delegation token. This is kind of crude, but good enough given the complexities of
     * Kerberos test setup.
     */
    @Test
    public void testKerberosAndDelegationToken()
            throws SQLException, IOException
    {
        Properties props = new Properties();
        props.setProperty(DELEGATION_TOKEN.getKey(), VALID_DELEGATION_TOKEN);
        props.setProperty(KERBEROS_REMOTE_SERICE_NAME.getKey(), "service");
        props.setProperty(KERBEROS_USE_CANONICAL_HOSTNAME.getKey(), "true");
        props.setProperty(KERBEROS_PRINCIPAL.getKey(), "test/host@realm.com");
        props.setProperty(KERBEROS_CONFIG_PATH.getKey(), File.createTempFile("temp", "krb").getPath());
        props.setProperty(KERBEROS_KEYTAB_PATH.getKey(), File.createTempFile("temp", "krb").getPath());
        props.setProperty(SSL.getKey(), "true");
        props.setProperty(SSL_TRUST_STORE_PATH.getKey(), serverInfo.trustStorePath);
        props.setProperty(SSL_TRUST_STORE_PASSWORD.getKey(), serverInfo.trustStorePassword);
        props.setProperty("user", "test");

        String url = format("jdbc:presto://localhost:%s", serverInfo.server.getPort());

        OkHttpClient httpClient = new OkHttpClient().newBuilder()
                .addInterceptor(userAgent("Presto JDBC driver"))
                .build();

        PrestoDriverUri uri = new PrestoDriverUri(url, props);

        OkHttpClient.Builder builder = httpClient.newBuilder();
        uri.setupClient(builder);
        OkHttpClient client = builder.build();

        Call call = client.newCall(new Request.Builder().url(serverInfo.server.url("/v1/info")).build());
        Response response = call.execute();
        assertNull(response.request().header(PRESTO_DELEGATION_TOKEN), "expected no delegation token in request");
    }

    @AfterMethod
    public void teardown()
            throws IOException
    {
        serverInfo.server.close();
    }

    /**
     * Helper method that setups the dispatcher to authenticate requests and send valid responses.
     */
    private void setupDispatcher()
    {
        QueueDispatcherWithAuth dispatcherWithAuth = new QueueDispatcherWithAuth();
        for (String response : createResults()) {
            dispatcherWithAuth.enqueueResponse(new MockResponse()
                    .addHeader(CONTENT_TYPE, "application/json")
                    .setBody(response));
        }
        serverInfo.server.setDispatcher(dispatcherWithAuth);
    }

    /**
     * Extends {@link QueueDispatcher} to allow validation of the delegation token in request header
     */
    private static class QueueDispatcherWithAuth
            extends QueueDispatcher
    {
        @Override
        public MockResponse dispatch(RecordedRequest request) throws InterruptedException
        {
            final String delegationToken = request.getHeaders().get(PRESTO_DELEGATION_TOKEN);
            if (!VALID_DELEGATION_TOKEN.equals(delegationToken)) {
                return new MockResponse().setStatus("HTTP/1.1 401 Not Authorized");
            }

            return super.dispatch(request);
        }
    }

    private List<String> createResults()
    {
        List<Column> columns = ImmutableList.of(new Column("_col0", "bigint", new ClientTypeSignature("bigint", ImmutableList.of())));
        return ImmutableList.<String>builder()
                .add(newQueryResults(1, null, null, "QUEUED"))
                .add(newQueryResults(2, columns, null, "RUNNING"))
                .add(newQueryResults(3, columns, null, "RUNNING"))
                .add(newQueryResults(4, columns, ImmutableList.of(ImmutableList.of(253161)), "RUNNING"))
                .add(newQueryResults(null, columns, null, "FINISHED"))
                .build();
    }

    private String newQueryResults(Integer nextUriId, List<Column> responseColumns, List<List<Object>> data, String state)
    {
        String queryId = "20190911_214710_00012_rk69b";

        QueryResults queryResults = new QueryResults(
                queryId,
                serverInfo.server.url("/query.html?" + queryId).uri(),
                null,
                nextUriId == null ? null : serverInfo.server.url(format("/v1/statement/%s/%s", queryId, nextUriId)).uri(),
                responseColumns,
                data,
                new StatementStats(state, state.equals("QUEUED"), true, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, null),
                null,
                null,
                null);

        return QUERY_RESULTS_CODEC.toJson(queryResults);
    }

    private void testQuery(Map<String, String> connectionProps)
            throws SQLException
    {
        try (Connection connection = createConnection(connectionProps)) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet rs = statement.executeQuery("bogus query for testing")) {
                    ResultSetMetaData metadata = rs.getMetaData();
                    assertEquals(metadata.getColumnCount(), 1);
                    assertEquals(metadata.getColumnName(1), "_col0");

                    assertTrue(rs.next());
                    assertEquals(rs.getLong(1), 253161L);
                    assertEquals(rs.getLong("_col0"), 253161L);

                    assertFalse(rs.next());
                }
            }
        }
    }

    private Connection createConnection(Map<String, String> additionalProperties)
            throws SQLException
    {
        String url = format("jdbc:presto://localhost:%s", serverInfo.server.getPort());
        Properties properties = new Properties();
        properties.setProperty("user", "test");
        properties.setProperty("SSL", "true");
        properties.setProperty("SSLTrustStorePath", serverInfo.trustStorePath);
        properties.setProperty("SSLTrustStorePassword", serverInfo.trustStorePassword);
        properties.putAll(additionalProperties);
        return DriverManager.getConnection(url, properties);
    }
}
