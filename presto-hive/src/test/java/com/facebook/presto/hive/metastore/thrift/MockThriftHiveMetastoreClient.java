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
package com.facebook.presto.hive.metastore.thrift;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Client;
import org.apache.hadoop.hive.thrift.DelegationTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TTransportException;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hive.thrift.DelegationTokenIdentifier.HIVE_DELEGATION_KIND;

public class MockThriftHiveMetastoreClient
        extends Client
{
    private final AtomicInteger accessCount = new AtomicInteger();
    private boolean throwException;
    private Token<DelegationTokenIdentifier> tokenCreated;

    public MockThriftHiveMetastoreClient()
            throws TTransportException
    {
        super(new TBinaryProtocol(new THttpClient("http://localhost:54321")));
    }

    public Token<DelegationTokenIdentifier> getTokenCreated()
    {
        return tokenCreated;
    }
    public void setThrowException(boolean throwException)
    {
        this.throwException = throwException;
    }

    public int getAccessCount()
    {
        return accessCount.get();
    }

    public String get_delegation_token(String owner, String renewer)
            throws MetaException, org.apache.thrift.TException
    {
        try {
            accessCount.incrementAndGet();
            if (throwException) {
                throw new MetaException();
            }
            DelegationTokenIdentifier id = new DelegationTokenIdentifier();
            id.setOwner(new Text(owner));
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            DataOutputStream output = new DataOutputStream(outputStream);
            id.write(output);
            Token<DelegationTokenIdentifier> hmsToken = new Token<DelegationTokenIdentifier>(id.getBytes(), null, HIVE_DELEGATION_KIND, null);
            this.tokenCreated = hmsToken;
            return hmsToken.encodeToUrlString();
        }
        catch (IOException ioe) {
            throwException = true;
            throw new MetaException("Not able to create delegation token");
        }
    }
}
