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
package com.facebook.presto.spi.pipeline;

import com.facebook.presto.spi.type.TypeSignature;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

public class PushDownInputColumn
        extends PushDownExpression
{
    private final String name;

    @JsonCreator
    public PushDownInputColumn(@JsonProperty("type") TypeSignature type, @JsonProperty("name") String name)
    {
        super(type);
        this.name = requireNonNull(name, "name is null");
    }

    public <R, C> R accept(PushDownExpressionVisitor<R, C> visitor, C context)
    {
        return visitor.visitInputColumn(this, context);
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @Override
    public String toString()
    {
        return name;
    }
}
