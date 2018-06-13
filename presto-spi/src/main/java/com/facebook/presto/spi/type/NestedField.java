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
package com.facebook.presto.spi.type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class NestedField
{
    private final String name;
    private final Map<String, NestedField> fields;

    @JsonCreator
    public NestedField(@JsonProperty("name") String name, @JsonProperty("fields") Map<String, NestedField> fields)
    {
        this.name = name;
        this.fields = fields;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Map<String, NestedField> getFields()
    {
        return fields;
    }

    private void addField(NestedField field)
    {
        if (fields.containsKey(field.getName().toLowerCase())) {
            mergeFields(fields.get(field.getName().toLowerCase()), field);
        }
        else {
            fields.put(field.getName().toLowerCase(), field);
        }
    }

    public static NestedField mergeFields(NestedField left, NestedField right)
    {
        if (left.getFields().isEmpty()) {
            return left;
        }

        if (right.getFields().isEmpty()) {
            return right;
        }

        for (NestedField field : right.getFields().values()) {
            if (left.getFields().containsKey(field.getName().toLowerCase())) {
                left.getFields().put(field.getName().toLowerCase(), mergeFields(left.getFields().get(field.getName().toLowerCase()), field));
            }
            else {
                left.getFields().put(field.getName().toLowerCase(), field);
            }
        }
        return left;
    }
}
