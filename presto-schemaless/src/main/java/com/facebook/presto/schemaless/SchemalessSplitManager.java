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
package com.facebook.presto.schemaless;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.type.NestedField;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.HostAddress.fromParts;
import static java.util.stream.Collectors.toList;

/**
 * Schemaless specific implementation of {@link ConnectorSplitManager}.
 * TODO Currently considers a single Schemaless Table as one split based on the assumption that the routing of index requests happen within Schemaless client. May need to change in future based on following questions
 * - scan based queries possible? Where is rate limiting implemented. We do not want to accidentally flood the system with high load of requests.
 * - Is routing possible outside Schemaless Client?
 * - Can the Presto Connector decide which Schemaless hosts to route the request to.
 * - Is there any API to get which Schemlaess hosts have the given shards. Are Schemaless hosts directly pingable? (It seems the current APis only provide the Db names instead of hostnames)
 */
public class SchemalessSplitManager
        implements ConnectorSplitManager
{
    private final List<HostAddress> addresses;

    @Inject
    public SchemalessSplitManager(SchemalessTableDescriptionSupplier tableDescriptionSupplier)
    {
        Map<SchemaTableName, SchemalessTableDescription> tabledescriptionsMap = tableDescriptionSupplier.get();

        //TODO ensure all schemaless instances (like mezzanine) have common host and port properties in the parent config file.
        // SO this will fetch from SchemalessConnectorConfig instead of individual table description file
        this.addresses = tabledescriptionsMap.values().stream().map(s -> fromParts(s.getHostAddress(), s.getPort()))
                .collect(toList());
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingStrategy splitSchedulingStrategy, Optional<Map<String, NestedField>> nestedFields, Optional<Map<String, String>> jsonPaths, Optional<Long> limit)
    {
        SchemalessTableLayoutHandle tableLayout = (SchemalessTableLayoutHandle) layout;
        SchemalessTableHandle tableHandle = tableLayout.getTable();

        SchemalessSplit split = new SchemalessSplit(
                tableHandle.toSchemaTableName(),
                addresses);

        return new FixedSplitSource(ImmutableList.of(split));
    }
}
