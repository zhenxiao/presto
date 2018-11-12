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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Optional;

public class UberSystemAccessControlConfig
{
    private final Optional<List<UberCatalogAccessControlRule>> catalogs;
    private final Optional<List<UberSchemaAccessControlRule>> schemas;
    private final Optional<List<UberImpersonationControlRule>> impersonations;
    private final Optional<String> kerberosRules;

    @JsonCreator
    public UberSystemAccessControlConfig(
            @JsonProperty("catalogs") Optional<List<UberCatalogAccessControlRule>> catalogs,
            @JsonProperty("schemas") Optional<List<UberSchemaAccessControlRule>> schemas,
            @JsonProperty("impersonations") Optional<List<UberImpersonationControlRule>> impersonations,
            @JsonProperty("kerberosRules") Optional<String> kerberosRules)
    {
        this.catalogs = catalogs;
        this.schemas = schemas;
        this.impersonations = impersonations;
        this.kerberosRules = kerberosRules;
    }

    public Optional<List<UberCatalogAccessControlRule>> getCatalogs()
    {
        return catalogs;
    }

    public Optional<List<UberSchemaAccessControlRule>> getSchemas()
    {
        return schemas;
    }

    public Optional<List<UberImpersonationControlRule>> getImpersonations()
    {
        return impersonations;
    }

    public Optional<String> getKerberosRules()
    {
        return kerberosRules;
    }
}
