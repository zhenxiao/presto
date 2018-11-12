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

import com.facebook.presto.spi.CatalogSchemaName;
import com.facebook.presto.spi.CatalogSchemaTableName;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.Privilege;
import com.facebook.presto.spi.security.SystemAccessControl;
import com.facebook.presto.spi.security.SystemAccessControlFactory;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import org.apache.hadoop.security.authentication.util.KerberosName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Principal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.security.AccessDeniedException.denyCatalogAccess;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateView;
import static com.facebook.presto.spi.security.AccessDeniedException.denyCreateViewWithSelect;
import static com.facebook.presto.spi.security.AccessDeniedException.denySelectColumns;
import static com.facebook.presto.spi.security.AccessDeniedException.denySelectTable;
import static com.facebook.presto.spi.security.AccessDeniedException.denySetUser;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Uber's implementation of Presto's {@link SystemAccessControl}. Supported access controls are:
 *
 * <ul>
 * <li>Catalog level access control</li>
 * <li>Schema level access control within catalogs. Catalog level access control rules are evaluated first before the schema level access rules.</li>
 * <li>Impersonation control: who can impersonate whom</li>
 * <li>Kerberos rules: this rules are set up in {@link KerberosName}</li>
 * </ul>
 */
public class UberSystemAccessControl
        implements SystemAccessControl
{
    static final String NAME = "uberAccessControl";
    static final String CONFIG_FILE_NAME = "uber.security.config-file";
    private static final String INFORMATION_SCHEMA = "information_schema";

    private final Optional<List<UberCatalogAccessControlRule>> catalogRules;
    private final Optional<List<UberSchemaAccessControlRule>> schemaRules;
    private final Optional<List<UberImpersonationControlRule>> impersonationRules;

    private UberSystemAccessControl(Optional<List<UberCatalogAccessControlRule>> catalogRules, Optional<List<UberSchemaAccessControlRule>> schemaRules,
            Optional<List<UberImpersonationControlRule>> impersonationRules)
    {
        this.catalogRules = catalogRules;
        this.schemaRules = schemaRules;
        this.impersonationRules = impersonationRules;
    }

    private boolean canAccessCatalog(Identity identity, String catalogName)
    {
        if (!catalogRules.isPresent()) {
            return true;
        }

        for (UberCatalogAccessControlRule rule : catalogRules.get()) {
            Optional<Boolean> allowed = rule.match(identity.getUser(), catalogName);
            if (allowed.isPresent()) {
                return allowed.get();
            }
        }
        return false;
    }

    private boolean canAccessSchema(Identity identity, String catalogName, String schemaName)
    {
        if (!canAccessCatalog(identity, catalogName)) {
            return false;
        }

        if (!schemaRules.isPresent()) {
            return true;
        }

        if (schemaName.equalsIgnoreCase(INFORMATION_SCHEMA)) {
            // information_schema schema is accessible to everyone as long as they have the catalog access
            return true;
        }

        for (UberSchemaAccessControlRule rule : schemaRules.get()) {
            Optional<Boolean> allowed = rule.match(identity.getUser(), catalogName, schemaName);
            if (allowed.isPresent()) {
                return allowed.get();
            }
        }

        return false;
    }

    @Override
    public void checkCanSetUser(Principal principal, String userName)
    {
        if (!impersonationRules.isPresent()) {
            return;
        }

        if (principal == null) {
            denySetUser(principal, userName);
        }

        try {
            String principalName = new KerberosName(principal.getName()).getShortName();

            if (principalName.equals(userName)) {
                return;
            }

            for (UberImpersonationControlRule rule : impersonationRules.get()) {
                Optional<Boolean> allowed = rule.match(principalName, userName);
                if (allowed.isPresent()) {
                    if (allowed.get()) {
                        return;
                    }
                    denySetUser(principal, userName);
                }
            }

            denySetUser(principal, userName);
        }
        catch (IOException e) {
            denySetUser(principal, userName);
        }
    }

    @Override
    public void checkCanSetSystemSessionProperty(Identity identity, String propertyName)
    {
    }

    @Override
    public void checkCanAccessCatalog(Identity identity, String catalogName)
    {
        if (!canAccessCatalog(identity, catalogName)) {
            denyCatalogAccess(catalogName);
        }
    }

    @Override
    public Set<String> filterCatalogs(Identity identity, Set<String> catalogs)
    {
        ImmutableSet.Builder<String> filteredCatalogs = ImmutableSet.builder();
        for (String catalog : catalogs) {
            if (canAccessCatalog(identity, catalog)) {
                filteredCatalogs.add(catalog);
            }
        }
        return filteredCatalogs.build();
    }

    @Override
    public void checkCanCreateSchema(Identity identity, CatalogSchemaName schema)
    {
    }

    @Override
    public void checkCanDropSchema(Identity identity, CatalogSchemaName schema)
    {
    }

    @Override
    public void checkCanRenameSchema(Identity identity, CatalogSchemaName schema, String newSchemaName)
    {
    }

    @Override
    public void checkCanShowSchemas(Identity identity, String catalogName)
    {
    }

    @Override
    public Set<String> filterSchemas(Identity identity, String catalogName, Set<String> schemaNames)
    {
        ImmutableSet.Builder<String> filteredSchemas = ImmutableSet.builder();
        for (String schemaName : schemaNames) {
            if (canAccessSchema(identity, catalogName, schemaName)) {
                filteredSchemas.add(schemaName);
            }
        }
        return filteredSchemas.build();
    }

    @Override
    public void checkCanCreateTable(Identity identity, CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanDropTable(Identity identity, CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanRenameTable(Identity identity, CatalogSchemaTableName table, CatalogSchemaTableName newTable)
    {
    }

    @Override
    public void checkCanShowTablesMetadata(Identity identity, CatalogSchemaName schema)
    {
    }

    @Override
    public Set<SchemaTableName> filterTables(Identity identity, String catalogName, Set<SchemaTableName> tableNames)
    {
        ImmutableSet.Builder<SchemaTableName> filteredTables = ImmutableSet.builder();
        for (SchemaTableName tableName : tableNames) {
            if (canAccessSchema(identity, catalogName, tableName.getSchemaName())) {
                filteredTables.add(tableName);
            }
        }

        return filteredTables.build();
    }

    @Override
    public void checkCanAddColumn(Identity identity, CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanDropColumn(Identity identity, CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanRenameColumn(Identity identity, CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanSelectFromColumns(Identity identity, CatalogSchemaTableName table, Set<String> columns)
    {
        if (!canAccessSchema(identity, table.getCatalogName(), table.getSchemaTableName().getSchemaName())) {
            denySelectColumns(table.getSchemaTableName().getTableName(), columns);
        }
    }

    @Override
    public void checkCanSelectFromTable(Identity identity, CatalogSchemaTableName table)
    {
        if (!canAccessSchema(identity, table.getCatalogName(), table.getSchemaTableName().getSchemaName())) {
            denySelectTable(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanInsertIntoTable(Identity identity, CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanDeleteFromTable(Identity identity, CatalogSchemaTableName table)
    {
    }

    @Override
    public void checkCanCreateView(Identity identity, CatalogSchemaTableName view)
    {
    }

    @Override
    public void checkCanDropView(Identity identity, CatalogSchemaTableName view)
    {
    }

    @Override
    public void checkCanSelectFromView(Identity identity, CatalogSchemaTableName view)
    {
    }

    @Override
    public void checkCanCreateViewWithSelectFromTable(Identity identity, CatalogSchemaTableName table)
    {
        if (!canAccessSchema(identity, table.getCatalogName(), table.getSchemaTableName().getSchemaName())) {
            denyCreateViewWithSelect(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromView(Identity identity, CatalogSchemaTableName view)
    {
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(Identity identity, CatalogSchemaTableName table, Set<String> columns)
    {
        if (!canAccessSchema(identity, table.getCatalogName(), table.getSchemaTableName().getSchemaName())) {
            denyCreateView(table.getSchemaTableName().getTableName());
        }
    }

    @Override
    public void checkCanSetCatalogSessionProperty(Identity identity, String catalogName, String propertyName)
    {
    }

    @Override
    public void checkCanGrantTablePrivilege(Identity identity, Privilege privilege, CatalogSchemaTableName table, String grantee, boolean withGrantOption)
    {
    }

    @Override
    public void checkCanRevokeTablePrivilege(Identity identity, Privilege privilege, CatalogSchemaTableName table, String revokee, boolean grantOptionFor)
    {
    }

    public static class Factory
            implements SystemAccessControlFactory
    {
        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public SystemAccessControl create(Map<String, String> config)
        {
            requireNonNull(config, "config is null");

            String configFileName = config.get(CONFIG_FILE_NAME);
            checkState(
                    configFileName != null,
                    "Security configuration must contain the '%s' property", CONFIG_FILE_NAME);

            try {
                Path path = Paths.get(configFileName);
                if (!path.isAbsolute()) {
                    path = path.toAbsolutePath();
                }
                path.toFile().canRead();

                UberSystemAccessControlConfig rules = JsonCodec.jsonCodec(UberSystemAccessControlConfig.class)
                        .fromJson(Files.readAllBytes(path));

                rules.getKerberosRules().ifPresent(kerberosRules -> KerberosName.setRules(kerberosRules));

                return new UberSystemAccessControl(rules.getCatalogs(), rules.getSchemas(), rules.getImpersonations());
            }
            catch (SecurityException | IOException | InvalidPathException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
