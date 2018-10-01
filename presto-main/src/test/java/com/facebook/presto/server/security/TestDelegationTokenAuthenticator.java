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
package com.facebook.presto.server.security;

import io.airlift.http.client.HttpClient;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import okhttp3.MediaType;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.QueueDispatcher;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.Principal;

import static com.facebook.presto.server.security.DelegationTokenAuthenticator.HomeDirectory;
import static com.facebook.presto.server.security.DelegationTokenAuthenticator.NNException;
import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestDelegationTokenAuthenticator
{
    private static final JsonCodec<HomeDirectory> HOME_DIRECTORY_CODEC = jsonCodec(HomeDirectory.class);
    private static final JsonCodec<NNException> NN_EXCEPTION_CODEC = jsonCodec(NNException.class);
    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
    private final HttpClient client = new JettyHttpClient();
    private DelegationTokenAuthenticator authenticator;
    private MockWebServer server;

    @BeforeTest
    public void setup()
            throws Exception
    {
        server = new MockWebServer();
        server.start();
        setupDispatcher();
        DTVerificationNNConfig config = new DTVerificationNNConfig();
        config.setNameNodes("http://" + server.getHostName() + ":" + server.getPort());
        authenticator = new DelegationTokenAuthenticator(config, client, HOME_DIRECTORY_CODEC, NN_EXCEPTION_CODEC);
    }

    @AfterTest
    public void teardown()
            throws IOException
    {
        server.close();
    }

    private void setupDispatcher()
    {
        QueueDispatcherWithAuth dispatcherWithAuth = new QueueDispatcherWithAuth();
        server.setDispatcher(dispatcherWithAuth);
    }

    @Test
    public void testValidDelegationToken()
            throws Exception
    {
        Principal principal = authenticator.authenticateWithDelegationToken(buildValidDelegationToken("testValidUser"));
        assertTrue(principal instanceof DelegationTokenAuthenticator.TokenUserPrincipal);
        assertEquals("testValidUser", principal.getName());
    }

    @Test(expectedExceptions = AuthenticationException.class, expectedExceptionsMessageRegExp = "Authentication error with delegation token")
    public void testInValidDelegationToken()
            throws Exception
    {
        authenticator.authenticateWithDelegationToken("Some Bad Tokens");
    }

    private String buildValidDelegationToken(String owner) throws Exception
    {
        DelegationTokenIdentifier id = new DelegationTokenIdentifier();
        id.setOwner(new Text(owner));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(outputStream);
        id.write(output);
        Token<DelegationTokenIdentifier> token = new Token<DelegationTokenIdentifier>(id.getBytes(), null, DelegationTokenIdentifier.HDFS_DELEGATION_KIND, null);
        return token.encodeToUrlString();
    }

    private static class QueueDispatcherWithAuth
            extends QueueDispatcher
    {
        @Override
        public MockResponse dispatch(RecordedRequest request) throws InterruptedException
        {
            try {
                String delegationToken = request.getRequestUrl().queryParameter("delegation");
                Token<DelegationTokenIdentifier> token = new Token<>();
                token.decodeFromUrlString(delegationToken);
                token.setKind(DelegationTokenIdentifier.HDFS_DELEGATION_KIND);

                ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
                DataInputStream in = new DataInputStream(buf);
                DelegationTokenIdentifier id = new DelegationTokenIdentifier();
                id.readFields(in);
                String authUser = id.getOwner().toString();
                if (authUser.equals("testValidUser")) {
                    return new MockResponse().setResponseCode(200).setHeader("Content-Type", JSON).setBody("{\"Path\": \"/user/testValidUser\"}");
                }
                return super.dispatch(request);
            }
            catch (Exception e) {
                return new MockResponse().setStatus("HTTP/1.1 401 Not Authorized");
            }
        }
    }
}
