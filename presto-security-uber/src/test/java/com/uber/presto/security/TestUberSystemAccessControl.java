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
package com.uber.presto.security;

import com.facebook.presto.spi.CatalogSchemaTableName;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.SystemAccessControl;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.Assert.ThrowingRunnable;
import org.testng.annotations.Test;

import javax.security.auth.kerberos.KerberosPrincipal;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.uber.presto.security.UberSystemAccessControl.CONFIG_FILE_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class TestUberSystemAccessControl
{
    private static final Identity alice = new Identity("alice", Optional.of(new KerberosPrincipal("alice/example.com@EXAMPLE.COM")));
    private static final Identity bob = new Identity("bob", Optional.of(new KerberosPrincipal("bob/example.com@EXAMPLE.COM")));
    private static final Identity cathy = new Identity("cathy", Optional.of(new KerberosPrincipal("cathy/example.com@EXAMPLE.COM")));
    private static final Identity janus = new Identity("janus", Optional.of(new KerberosPrincipal("janus/example.com@EXAMPLE.COM")));
    private static final Identity presto = new Identity("presto", Optional.of(new KerberosPrincipal("presto/example.com@EXAMPLE.COM")));
    private static final Set<String> allCatalogs = ImmutableSet.of("system", "hive", "jmx", "elastic");
    private static final Set<String> systemSchemas = ImmutableSet.of("jdbc", "runtime", "information_schema");
    private static final Set<String> elasticSchemas = ImmutableSet.of("aliceschema", "bobschema", "information_schema");
    private static final Set<String> jmxSchemas = ImmutableSet.of("jmxschema1", "jmxschema2", "information_schema");
    private static final Set<String> hiveSchemas = ImmutableSet.of("db1", "db2", "information_schema");

    private static void assertAllowedTableAccess(SystemAccessControl accessControl, Identity identity, CatalogSchemaTableName table, Set<String> columns)
    {
        accessControl.checkCanSelectFromTable(identity, table);
        accessControl.checkCanSelectFromColumns(identity, table, columns);
        accessControl.checkCanCreateViewWithSelectFromTable(identity, table);
        accessControl.checkCanCreateViewWithSelectFromColumns(identity, table, columns);
    }

    private static void assertDeniedTableAccess(SystemAccessControl accessControl, Identity identity, CatalogSchemaTableName table, Set<String> columns)
    {
        assertDenied(() -> accessControl.checkCanSelectFromTable(identity, table));
        assertDenied(() -> accessControl.checkCanSelectFromColumns(identity, table, columns));
        assertDenied(() -> accessControl.checkCanCreateViewWithSelectFromTable(identity, table));
        assertDenied(() -> accessControl.checkCanCreateViewWithSelectFromColumns(identity, table, columns));
    }

    private static void assertDenied(ThrowingRunnable runnable)
    {
        assertThrows(AccessDeniedException.class, runnable);
    }

    private static CatalogSchemaTableName newCST(String catalog, String schema, String table)
    {
        return new CatalogSchemaTableName(catalog, schema, table);
    }

    @Test
    public void testPrestoUserAccess()
    {
        SystemAccessControl accessControl = newAccessControl();

        // catalog access
        accessControl.checkCanAccessCatalog(presto, "system");
        accessControl.checkCanAccessCatalog(presto, "jmx");
        accessControl.checkCanAccessCatalog(presto, "hive");
        assertDenied(() -> accessControl.checkCanAccessCatalog(presto, "elastic"));

        assertEquals(accessControl.filterCatalogs(presto, allCatalogs), ImmutableSet.of("system", "hive", "jmx"));

        // schema access
        assertEquals(accessControl.filterSchemas(presto, "system", systemSchemas), systemSchemas);
        assertEquals(accessControl.filterSchemas(presto, "hive", hiveSchemas), hiveSchemas);
        assertEquals(accessControl.filterSchemas(presto, "jmx", jmxSchemas), jmxSchemas);
        assertEquals(accessControl.filterSchemas(presto, "elastic", elasticSchemas), ImmutableSet.of());

        // table access
        assertAllowedTableAccess(accessControl, presto, newCST("system", "runtime", "nodes"), ImmutableSet.of("col1"));
        assertAllowedTableAccess(accessControl, presto, newCST("jmx", "jmxschema1", "jmxTable1"), ImmutableSet.of("col1"));
        assertAllowedTableAccess(accessControl, presto, newCST("hive", "db1", "hiveTable1"), ImmutableSet.of("col1"));
        assertAllowedTableAccess(accessControl, alice, newCST("hive", "information_schema", "hiveTable1"), ImmutableSet.of("col1"));
        assertDeniedTableAccess(accessControl, presto, newCST("elastic", "aliceschema", "aliceTable"), ImmutableSet.of("col1"));
    }

    @Test
    public void testAliceUserAccess()
    {
        SystemAccessControl accessControl = newAccessControl();

        // catalog access
        accessControl.checkCanAccessCatalog(alice, "system");
        assertDenied(() -> accessControl.checkCanAccessCatalog(alice, "jvm"));
        accessControl.checkCanAccessCatalog(alice, "hive");
        accessControl.checkCanAccessCatalog(alice, "elastic");

        assertEquals(accessControl.filterCatalogs(alice, allCatalogs), ImmutableSet.of("system", "hive", "elastic"));

        // schema access
        assertEquals(accessControl.filterSchemas(alice, "system", systemSchemas), ImmutableList.of("jdbc", "information_schema"));
        assertEquals(accessControl.filterSchemas(alice, "hive", hiveSchemas), hiveSchemas);
        assertEquals(accessControl.filterSchemas(alice, "jmx", jmxSchemas), ImmutableList.of());
        assertEquals(accessControl.filterSchemas(alice, "elastic", elasticSchemas), ImmutableSet.of("aliceschema", "information_schema"));

        // table access
        assertAllowedTableAccess(accessControl, alice, newCST("system", "jdbc", "procedure"), ImmutableSet.of("col1"));
        assertDeniedTableAccess(accessControl, alice, newCST("system", "runtime", "nodes"), ImmutableSet.of("col1"));
        assertDeniedTableAccess(accessControl, alice, newCST("jmx", "jmxschema1", "jmxTable1"), ImmutableSet.of("col1"));
        assertAllowedTableAccess(accessControl, alice, newCST("hive", "db1", "hiveTable1"), ImmutableSet.of("col1"));
        assertAllowedTableAccess(accessControl, alice, newCST("hive", "information_schema", "hiveTable1"), ImmutableSet.of("col1"));
        assertAllowedTableAccess(accessControl, alice, newCST("elastic", "aliceschema", "aliceTable"), ImmutableSet.of("col1"));
        assertDeniedTableAccess(accessControl, alice, newCST("elastic", "bobschema", "bobTable"), ImmutableSet.of("col1"));
        assertAllowedTableAccess(accessControl, alice, newCST("elastic", "information_schema", "tables"), ImmutableSet.of("col1"));
    }

    @Test
    public void testBobUserAccess()
    {
        SystemAccessControl accessControl = newAccessControl();

        // catalog access
        accessControl.checkCanAccessCatalog(bob, "system");
        assertDenied(() -> accessControl.checkCanAccessCatalog(bob, "jvm"));
        accessControl.checkCanAccessCatalog(bob, "hive");
        accessControl.checkCanAccessCatalog(bob, "elastic");

        assertEquals(accessControl.filterCatalogs(bob, allCatalogs), ImmutableSet.of("system", "hive", "elastic"));

        // schema access
        assertEquals(accessControl.filterSchemas(bob, "system", systemSchemas), ImmutableList.of("jdbc", "information_schema"));
        assertEquals(accessControl.filterSchemas(bob, "hive", hiveSchemas), hiveSchemas);
        assertEquals(accessControl.filterSchemas(bob, "jmx", jmxSchemas), ImmutableList.of());
        assertEquals(accessControl.filterSchemas(bob, "elastic", elasticSchemas), ImmutableSet.of("bobschema", "information_schema"));

        // table access
        assertAllowedTableAccess(accessControl, bob, newCST("system", "jdbc", "procedure"), ImmutableSet.of("col1"));
        assertDeniedTableAccess(accessControl, bob, newCST("system", "runtime", "nodes"), ImmutableSet.of("col1"));
        assertDeniedTableAccess(accessControl, bob, newCST("jmx", "jmxschema1", "jmxTable1"), ImmutableSet.of("col1"));
        assertAllowedTableAccess(accessControl, bob, newCST("hive", "db1", "hiveTable1"), ImmutableSet.of("col1"));
        assertAllowedTableAccess(accessControl, alice, newCST("hive", "information_schema", "hiveTable1"), ImmutableSet.of("col1"));
        assertAllowedTableAccess(accessControl, bob, newCST("elastic", "bobschema", "bobTable"), ImmutableSet.of("col1"));
        assertDeniedTableAccess(accessControl, bob, newCST("elastic", "aliceschema", "aliceTable"), ImmutableSet.of("col1"));
        assertAllowedTableAccess(accessControl, alice, newCST("elastic", "information_schema", "tables"), ImmutableSet.of("col1"));
    }

    @Test
    public void testCathyUserAccess()
    {
        SystemAccessControl accessControl = newAccessControl();

        // catalog access
        accessControl.checkCanAccessCatalog(cathy, "system");
        assertDenied(() -> accessControl.checkCanAccessCatalog(cathy, "jvm"));
        accessControl.checkCanAccessCatalog(cathy, "hive");
        assertDenied(() -> accessControl.checkCanAccessCatalog(cathy, "elastic"));

        assertEquals(accessControl.filterCatalogs(cathy, allCatalogs), ImmutableSet.of("system", "hive"));

        // schema access
        assertEquals(accessControl.filterSchemas(cathy, "system", systemSchemas), ImmutableList.of("jdbc", "information_schema"));
        assertEquals(accessControl.filterSchemas(cathy, "hive", hiveSchemas), hiveSchemas);
        assertEquals(accessControl.filterSchemas(cathy, "jmx", jmxSchemas), ImmutableList.of());
        assertEquals(accessControl.filterSchemas(cathy, "elastic", elasticSchemas), ImmutableSet.of());

        // table access
        assertAllowedTableAccess(accessControl, cathy, newCST("system", "jdbc", "procedure"), ImmutableSet.of("col1"));
        assertDeniedTableAccess(accessControl, cathy, newCST("system", "runtime", "nodes"), ImmutableSet.of("col1"));
        assertDeniedTableAccess(accessControl, cathy, newCST("jmx", "jmxschema1", "jmxTable1"), ImmutableSet.of("col1"));
        assertAllowedTableAccess(accessControl, cathy, newCST("hive", "db1", "hiveTable1"), ImmutableSet.of("col1"));
        assertDeniedTableAccess(accessControl, cathy, newCST("elastic", "bobschema", "cathyTable"), ImmutableSet.of("col1"));
        assertDeniedTableAccess(accessControl, cathy, newCST("elastic", "aliceschema", "aliceTable"), ImmutableSet.of("col1"));
    }

    @Test
    public void testImpersonations()
    {
        SystemAccessControl accessControl = newAccessControl();

        // Janus should be able to impersonate all users
        accessControl.checkCanSetUser(janus.getPrincipal().get(), "alice");
        accessControl.checkCanSetUser(janus.getPrincipal().get(), "bob");
        accessControl.checkCanSetUser(janus.getPrincipal().get(), "cathy");

        // Presto should be able to impersonate all users
        accessControl.checkCanSetUser(presto.getPrincipal().get(), "alice");
        accessControl.checkCanSetUser(presto.getPrincipal().get(), "bob");
        accessControl.checkCanSetUser(presto.getPrincipal().get(), "cathy");

        // Alice shouldn't be able to impersonate any user
        assertDenied(() -> accessControl.checkCanSetUser(alice.getPrincipal().get(), "bob"));
        assertDenied(() -> accessControl.checkCanSetUser(alice.getPrincipal().get(), "cathy"));

        // Bob should be able to impersonate only alice
        accessControl.checkCanSetUser(bob.getPrincipal().get(), "alice");
        assertDenied(() -> accessControl.checkCanSetUser(bob.getPrincipal().get(), "cathy"));
    }

    private SystemAccessControl newAccessControl()
    {
        String configFilePath = this.getClass().getClassLoader().getResource("test-uber-access-control.json").getPath();
        Map<String, String> config = ImmutableMap.of(CONFIG_FILE_NAME, configFilePath);
        return new UberSystemAccessControl.Factory().create(config);
    }
}
