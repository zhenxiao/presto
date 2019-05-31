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

import com.facebook.presto.rta.schema.RTADeployment;
import com.facebook.presto.rta.schema.RTATableEntity;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

@ThreadSafe
public class RtaPropertyManager
{
    private final AtomicReference<Map<RtaStorageKey, Map<String, String>>> properties = new AtomicReference<>();
    private final Optional<DataCenterSpec> dataCenter;

    public Map<RtaStorageKey, Map<String, String>> getProperties()
    {
        return properties.get();
    }

    private static class DataCenterSpec
    {
        private static final Pattern PATTERN = Pattern.compile("([a-z]+)([0-9]+)");
        private final String dcName;
        private final int dcNumber;

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("dcName", dcName)
                    .add("dcNumber", dcNumber)
                    .toString();
        }

        private DataCenterSpec(String dcName, int dcNumber)
        {
            this.dcName = dcName.toLowerCase(ENGLISH);
            this.dcNumber = dcNumber;
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
            DataCenterSpec that = (DataCenterSpec) o;
            return dcNumber == that.dcNumber &&
                    dcName.equals(that.dcName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(dcName, dcNumber);
        }

        public static Optional<DataCenterSpec> parse(String dataCenter)
        {
            Matcher matcher = PATTERN.matcher(dataCenter.toLowerCase(ENGLISH));
            if (matcher.matches() && matcher.groupCount() == 2) {
                try {
                    return Optional.of(new DataCenterSpec(matcher.group(1), Integer.parseInt(matcher.group(2))));
                }
                catch (NumberFormatException ne) {
                }
            }
            return Optional.empty();
        }

        public String getDcName()
        {
            return dcName;
        }
    }

    public static class PerEnvironmentSpec
    {
        private final Optional<String> baseConfigFile;
        private final Map<String, String> configs;

        @JsonCreator
        public PerEnvironmentSpec(@JsonProperty("baseConfigFile") @Nullable String baseConfigFile, @JsonProperty("config") @Nullable Map<String, String> configs)
        {
            this.baseConfigFile = Optional.ofNullable(baseConfigFile);
            this.configs = configs == null ? ImmutableMap.of() : ImmutableMap.copyOf(configs);
        }

        public Optional<String> getBaseConfigFile()
        {
            return baseConfigFile;
        }

        public Map<String, String> getConfigs()
        {
            return configs;
        }

        public Map<String, String> getResolvedConfigs(Map<String, String> configsForEnvironment)
                throws IOException
        {
            Map<String, String> properties = new HashMap<>();
            if (baseConfigFile.isPresent()) {
                properties.putAll(RtaUtil.loadProperties(new File(baseConfigFile.get())));
            }
            properties.putAll(configsForEnvironment);
            properties.putAll(configs);
            properties.remove("connector.name");
            return properties;
        }
    }

    public static class PerTypeSpec
    {
        private final Map<String, String> config;
        private final Map<String, PerEnvironmentSpec> environments;

        @JsonCreator
        public PerTypeSpec(@JsonProperty("config") @Nullable Map<String, String> config, @JsonProperty("environments") Map<String, PerEnvironmentSpec> environments)
        {
            this.config = config == null ? ImmutableMap.of() : ImmutableMap.copyOf(config);
            this.environments = environments == null ? ImmutableMap.of() : ImmutableMap.copyOf(environments);
        }

        public Map<String, String> getConfig()
        {
            return config;
        }

        public Map<String, PerEnvironmentSpec> getEnvironments()
        {
            return environments;
        }
    }

    public static class PropertySpec
    {
        public static final JsonCodec<PropertySpec> CODEC = new JsonCodecFactory(
                () -> new ObjectMapperProvider().get().enable(FAIL_ON_UNKNOWN_PROPERTIES))
                .jsonCodec(PropertySpec.class);
        private final Map<String, String> config;
        private final Map<String, PerTypeSpec> types;

        @JsonCreator
        public PropertySpec(@JsonProperty("config") @Nullable Map<String, String> config, @JsonProperty("types") Map<String, PerTypeSpec> types)
        {
            this.config = config == null ? ImmutableMap.of() : ImmutableMap.copyOf(config);
            this.types = types == null ? ImmutableMap.of() : ImmutableMap.copyOf(types);
        }

        public Map<String, String> getConfig()
        {
            return config;
        }

        public Map<String, PerTypeSpec> getTypes()
        {
            return types;
        }

        public Map<RtaStorageKey, Map<String, String>> build()
                throws IOException
        {
            ImmutableMap.Builder<RtaStorageKey, Map<String, String>> ret = ImmutableMap.builder();
            for (Map.Entry<String, PerTypeSpec> type : types.entrySet()) {
                String typeStr = type.getKey();
                PerTypeSpec spec = type.getValue();
                Map<String, String> configsForType = new HashMap<>(config);
                configsForType.putAll(spec.config);
                RtaStorageType rtaType = RtaStorageType.valueOf(typeStr.toUpperCase(ENGLISH));
                for (Map.Entry<String, PerEnvironmentSpec> env : spec.getEnvironments().entrySet()) {
                    String environment = env.getKey();
                    PerEnvironmentSpec perEnvironmentSpec = env.getValue();
                    Map<String, String> configsForEnvironment = new HashMap<>(configsForType);
                    ret.put(new RtaStorageKey(environment, rtaType), ImmutableMap.copyOf(perEnvironmentSpec.getResolvedConfigs(configsForEnvironment)));
                }
            }
            return ret.build();
        }
    }

    @Inject
    public RtaPropertyManager(RtaConfig config)
            throws IOException
    {
        this.dataCenter = getUberDataCenterName(config).flatMap(DataCenterSpec::parse);
        String filename = config.getConfigFile();
        PropertySpec propertySpec;
        if (!filename.isEmpty()) {
            try {
                propertySpec = PropertySpec.CODEC.fromJson(Files.readAllBytes(Paths.get(filename)));
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            catch (IllegalArgumentException e) {
                Throwable cause = e.getCause();
                if (cause instanceof UnrecognizedPropertyException) {
                    UnrecognizedPropertyException ex = (UnrecognizedPropertyException) cause;
                    String message = format("Unknown property at line %s:%s: %s",
                            ex.getLocation().getLineNr(),
                            ex.getLocation().getColumnNr(),
                            ex.getPropertyName());
                    throw new IllegalArgumentException(message, e);
                }
                if (cause instanceof JsonMappingException) {
                    // remove the extra "through reference chain" message
                    if (cause.getCause() != null) {
                        cause = cause.getCause();
                    }
                    throw new IllegalArgumentException(cause.getMessage(), e);
                }
                throw e;
            }
        }
        else {
            propertySpec = new PropertySpec(ImmutableMap.of(), ImmutableMap.of());
        }
        properties.set(propertySpec.build());
    }

    private static Optional<String> getUberDataCenterName(RtaConfig config)
    {
        String configDataCenter = config.getDataCenterOverride();
        if (configDataCenter != null) {
            return Optional.of(configDataCenter);
        }
        try {
            return Files.lines(Paths.get("/etc/uber/datacenter")).map(String::trim).map(s -> s.toLowerCase(ENGLISH)).filter(s -> !s.isEmpty()).findFirst();
        }
        catch (IOException e) {
            return Optional.empty();
        }
    }

    private int getDeploymentScore(RTADeployment deployment)
    {
        // First rank by the same DC, or the DC with the same prefix
        // Break ties by the storage type
        Optional<DataCenterSpec> deploymentDc = DataCenterSpec.parse(deployment.getDataCenter());
        Preconditions.checkState(RtaStorageType.values().length < 1000, "Assuming that there are no more than 1000 storage types for comparison");
        if (dataCenter.equals(deploymentDc)) {
            return 0 + deployment.getStorageType().ordinal();
        }
        else if (dataCenter.isPresent() && deploymentDc.isPresent() && dataCenter.get().getDcName().equals(deploymentDc.get().getDcName())) {
            return 1000 + deployment.getStorageType().ordinal();
        }
        else {
            return 2000 + deployment.getStorageType().ordinal();
        }
    }

    @VisibleForTesting
    RTADeployment pickDeployment(List<RTADeployment> deployments)
    {
        Preconditions.checkState(!deployments.isEmpty());
        ArrayList<RTADeployment> deploymentsMutable = new ArrayList<>(deployments);
        deploymentsMutable.sort(Comparator.comparingInt(this::getDeploymentScore));
        int winningScore = this.getDeploymentScore(deploymentsMutable.get(0));
        int winners = 0;
        while (winners < deploymentsMutable.size() && winningScore == getDeploymentScore(deploymentsMutable.get(winners))) {
            ++winners;
        }
        Preconditions.checkState(winners > 0);
        List<RTADeployment> candidates = deploymentsMutable.subList(0, winners);
        Collections.shuffle(candidates);
        return candidates.get(0);
    }

    public RTADeployment getDefaultDeployment(RTATableEntity entity)
    {
        Map<RtaStorageKey, Map<String, String>> properties = getProperties();
        ArrayList<RTADeployment> candidateDeployments = new ArrayList<>();
        for (RTADeployment deployment : entity.getDeployments()) {
            if (properties.containsKey(RtaStorageKey.fromDeployment(deployment))) {
                candidateDeployments.add(deployment);
            }
        }
        if (candidateDeployments.isEmpty()) {
            throw new NoSuchElementException("Cannot find a default deployment for table " + entity + " with known storages " + ImmutableList.copyOf(properties.keySet()));
        }
        else {
            return pickDeployment(candidateDeployments);
        }
    }
}
