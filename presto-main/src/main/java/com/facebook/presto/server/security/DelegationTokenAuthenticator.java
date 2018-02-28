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

import com.facebook.presto.operator.ForDelegationTokenValidation;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Splitter;
import com.google.inject.Inject;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.token.Token;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.net.URI;
import java.security.Principal;
import java.util.List;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpStatus.OK;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;

/**
 * This class is used to do the delegation token authentication,
 * when user submits the requests by specifying the delegation token
 * header, we shall use the header to do authentication instead of kerberos
 * ticket. We shall send the token into webHDFS request to let name node
 * validates the token, and then parse the token, finally set the user info
 * into the request.
 */
public class DelegationTokenAuthenticator
{
    private static final Logger LOG = Logger.get(DelegationTokenAuthenticator.class);
    private static final String HDFS_VALIDATION_PATH = "/webhdfs/v1/?op=GETHOMEDIRECTORY&delegation=";
    private final List<String> nameNodes;
    private final HttpClient httpClient;
    private final JsonCodec<HomeDirectory> codec;
    private final JsonCodec<NNException> exceptionCodec;
    private AtomicReference<String> activeNameNode;

    @Inject
    public DelegationTokenAuthenticator(DTVerificationNNConfig config, @ForDelegationTokenValidation HttpClient httpClient, JsonCodec<HomeDirectory> codec, JsonCodec<NNException> exceptionCodec)
    {
        this.httpClient = httpClient;
        this.codec = codec;
        this.exceptionCodec = exceptionCodec;
        Spliterator<String> split = Splitter.on(',').omitEmptyStrings().trimResults().split(config.getNameNodes()).spliterator();
        nameNodes = StreamSupport.stream(split, false).collect(Collectors.toList());
        if (nameNodes.isEmpty()) {
            throw new RuntimeException("Must be at least one namenode in delegation token auth config");
        }
        activeNameNode = new AtomicReference<>(nameNodes.iterator().next());
    }

    private boolean isWebHDFSAuthSuccessful(String token)
            throws Exception
    {
        Request request = prepareGet()
                .setUri(new URI(activeNameNode.get() + HDFS_VALIDATION_PATH + token))
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .build();
        JsonResponse<HomeDirectory> result = httpClient.execute(request, createFullJsonResponseHandler(codec));

        if (result != null) {
            if (result.getValue().path == null) {
                NNException nnException = exceptionCodec.fromJson(result.getJsonBytes());
                RemoteException re = nnException.getRemoteException();
                if (re.getException().equals("StandbyException")) {
                    for (String nameNode : nameNodes) {
                        if (!nameNode.equals(activeNameNode.get())) {
                            activeNameNode.set(nameNode);
                            LOG.info("Switch the active name node to " + activeNameNode.get());
                            return isWebHDFSAuthSuccessful(token);
                        }
                    }
                }
                if (re.getException().equals("RemoteException")) {
                    LOG.debug("Delegation token validation failed %s", re.toString());
                    return false;
                }
            }
            if (result.getStatusCode() == OK.code()) {
                return true;
            }
        }
        LOG.info("Unexpected Delegation token validation result %s", result == null ? "null" : result.toString());
        return false;
    }

    public Principal authenticateWithDelegationToken(String delegationToken)
            throws AuthenticationException
    {
        try {
            Token<DelegationTokenIdentifier> token = new Token<>();
            token.decodeFromUrlString(delegationToken);
            token.setKind(DelegationTokenIdentifier.HDFS_DELEGATION_KIND);

            ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
            DataInputStream in = new DataInputStream(buf);
            DelegationTokenIdentifier id = new DelegationTokenIdentifier();
            id.readFields(in);
            String authUser = id.getOwner().toString();

            if (isWebHDFSAuthSuccessful(delegationToken)) {
                return new TokenUserPrincipal(authUser);
            }

            throw new AuthenticationException("Authentication failed with delegation token");
        }
        catch (Exception e) {
            throw new RuntimeException("Authentication error with delegation token", e);
        }
    }

    public static class HomeDirectory
    {
        private final String path;

        @JsonCreator
        public HomeDirectory(@JsonProperty("Path") String path)
        {
            this.path = path;
        }

        @JsonProperty("Path")
        public String getPath()
        {
            return path;
        }
    }

    public static class NNException
    {
        private final RemoteException exception;

        @JsonCreator
        public NNException(@JsonProperty("RemoteException") RemoteException exception)
        {
            this.exception = exception;
        }

        @JsonProperty("RemoteException")
        public RemoteException getRemoteException()
        {
            return exception;
        }
    }

    public static class RemoteException
    {
        private final String exception;
        private final String javaClassName;
        private final String message;

        @JsonCreator
        public RemoteException(@JsonProperty("exception") String exception, @JsonProperty("javaClassName") String javaClassName, @JsonProperty("message") String message)
        {
            this.exception = requireNonNull(exception, "exception is null");
            this.javaClassName = requireNonNull(javaClassName, "javaClassName is null");
            this.message = requireNonNull(message, "message is null");
        }

        @JsonProperty("exception")
        public String getException()
        {
            return exception;
        }

        @JsonProperty("javaClassName")
        public String getJavaClassName()
        {
            return javaClassName;
        }

        @JsonProperty
        String getMessage()
        {
            return message;
        }
    }

    public final class TokenUserPrincipal
            implements Principal
    {
        private final String username;

        public TokenUserPrincipal(final String username)
        {
            super();
            this.username = requireNonNull(username, "username is null");
        }

        public String getName()
        {
            return this.username;
        }

        @Override
        public String toString()
        {
            StringBuilder buffer = new StringBuilder();
            buffer.append("[principal: ");
            buffer.append(this.username);
            buffer.append("]");
            return buffer.toString();
        }
    }
}
