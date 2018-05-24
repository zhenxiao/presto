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
package com.facebook.presto.elasticsearch2;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.NestedField;
import com.facebook.presto.spi.type.Type;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.StandardTypes.ARRAY;
import static com.facebook.presto.spi.type.StandardTypes.MAP;
import static com.facebook.presto.spi.type.StandardTypes.ROW;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;

public final class ElasticsearchUtils
{
    private ElasticsearchUtils() {}

    public static Block serializeObject(Type type, BlockBuilder builder, Object object, NestedField nestedField)
    {
        if (ROW.equals(type.getTypeSignature().getBase())) {
            return serializeStruct(type, builder, object, nestedField);
        }
        else if (MAP.equals(type.getTypeSignature().getBase()) || ARRAY.equals(type.getTypeSignature().getBase())) {
            throw new RuntimeException("Type not supported in Elasticsearch: " + type.getDisplayName());
        }
        else {
            serializePrimitive(type, builder, object);
            return null;
        }
    }

    private static Block serializeStruct(Type type, BlockBuilder builder, Object object, NestedField nestedField)
    {
        if (object == null) {
            requireNonNull(builder, "parent builder is null").appendNull();
            return null;
        }

        List<Type> typeParameters = type.getTypeParameters();
        BlockBuilder currentBuilder;

        boolean builderSynthesized = false;
        if (builder == null) {
            builderSynthesized = true;
            builder = type.createBlockBuilder(null, 1);
        }
        currentBuilder = builder.beginBlockEntry();

        for (int i = 0; i < typeParameters.size(); i++) {
            Optional<String> fieldName = type.getTypeSignature().getParameters().get(i).getNamedTypeSignature().getName();
            String name = "";
            if (fieldName.isPresent()) {
                name = fieldName.get().toLowerCase();
            }
            if (nestedField == null) {
                Object value = ((Map) object).get(name);
                serializeObject(typeParameters.get(i), currentBuilder, value, null);
            }
            else {
                if (nestedField.getFields().containsKey(name)) {
                    Map<String, Object> map = new HashMap<>((Map) object);
                    Object value = null;
                    for (String key : map.keySet()) {
                        if (key.equalsIgnoreCase(name)) {
                            value = map.get(key);
                            break;
                        }
                    }
                    serializeObject(typeParameters.get(i), currentBuilder, value, nestedField.getFields().get(name));
                }
                else {
                    currentBuilder.appendNull();
                }
            }
        }

        builder.closeEntry();
        if (builderSynthesized) {
            return (Block) type.getObject(builder, 0);
        }
        else {
            return null;
        }
    }

    private static void serializePrimitive(Type type, BlockBuilder builder, Object object)
    {
        requireNonNull(builder, "parent builder is null");

        if (object == null) {
            builder.appendNull();
            return;
        }

        if (type.equals(BOOLEAN)) {
            type.writeBoolean(builder, (Boolean) object);
            return;
        }
        else if (type.equals(BIGINT)) {
            type.writeLong(builder, Long.valueOf(String.valueOf(object)));
            return;
        }
        else if (type.equals(DOUBLE)) {
            type.writeDouble(builder, (Double) object);
            return;
        }
        else if (type.equals(INTEGER)) {
            type.writeLong(builder, Long.valueOf(String.valueOf(object)));
            return;
        }
        else if (type.equals(VARCHAR)) {
            type.writeSlice(builder, utf8Slice(String.valueOf(object)));
            return;
        }
        else if (type.equals(VARBINARY)) {
            type.writeSlice(builder, utf8Slice(String.valueOf(object)));
            return;
        }
        throw new RuntimeException("Unknown primitive type: " + type.getDisplayName());
    }
}
