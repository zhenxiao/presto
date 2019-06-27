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

import com.facebook.presto.spi.session.PropertyMetadata;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class RtaSessionProperties
{
    private final List<PropertyMetadata<?>> sessionProperties;

    private static class PropertyAndType
    {
        private final PropertyMetadata<?> propertyMetadata;
        private final RtaStorageType type;

        public PropertyAndType(PropertyMetadata<?> propertyMetadata, RtaStorageType type)
        {
            this.propertyMetadata = propertyMetadata;
            this.type = type;
        }

        public PropertyMetadata<?> getPropertyMetadata()
        {
            return propertyMetadata;
        }

        public RtaStorageType getType()
        {
            return type;
        }

        @Override
        public String toString()
        {
            return new StringJoiner(", ", PropertyAndType.class.getSimpleName() + "[", "]")
                    .add("propertyMetadataName=" + propertyMetadata.getName())
                    .add("propertyMetadataDescription=" + propertyMetadata.getDescription())
                    .add("type=" + type)
                    .toString();
        }
    }

    @Inject
    public RtaSessionProperties(RtaConfig rtaConfig, RtaConnectorProvider connectorProvider)
    {
        Map<String, PropertyAndType> uniqueProperties = new HashMap<>();
        connectorProvider.getConnectors().forEach((key, connector) -> {
            RtaStorageType type = key.getType();
            connector.getSessionProperties().forEach(property -> {
                PropertyAndType existing = uniqueProperties.get(property.getName());
                if (existing != null && !type.equals(existing.getType())) {
                    throw new IllegalStateException(String.format("Found a duplicate property %s, clashing b/w %s and %s", property.getName(), existing, type));
                }
                uniqueProperties.put(property.getName(), new PropertyAndType(property, type));
            });
        });
        sessionProperties = uniqueProperties.values().stream().map(PropertyAndType::getPropertyMetadata).collect(toImmutableList());
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }
}
