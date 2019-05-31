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
package com.facebook.presto.rta;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class RtaSplit
        implements ConnectorSplit
{
    private final RtaConnectorId connectorId;
    private final RtaStorageKey key;
    private final ConnectorSplit split;

    @JsonCreator
    public RtaSplit(@JsonProperty("connectorId") RtaConnectorId connectorId, @JsonProperty("key") RtaStorageKey key, @JsonProperty("split") ConnectorSplit split)
    {
        this.connectorId = connectorId;
        this.key = key;
        this.split = split;
    }

    @JsonProperty
    public ConnectorSplit getSplit()
    {
        return split;
    }

    @JsonProperty
    public RtaStorageKey getKey()
    {
        return key;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("key", key)
                .add("split", split)
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RtaSplit rtaSplit = (RtaSplit) o;
        return Objects.equals(connectorId, rtaSplit.connectorId) &&
                Objects.equals(key, rtaSplit.key) &&
                Objects.equals(split, rtaSplit.split);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, key, split);
    }

    @JsonProperty
    public RtaConnectorId getConnectorId()
    {
        return connectorId;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return split.isRemotelyAccessible();
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return split.getAddresses();
    }

    @Override
    public Object getInfo()
    {
        return split.getInfo();
    }
}
