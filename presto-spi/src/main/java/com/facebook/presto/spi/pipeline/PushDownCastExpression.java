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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

public class PushDownCastExpression
        extends PushDownExpression
{
    private final PushDownExpression input;
    private final String type;

    @JsonCreator
    public PushDownCastExpression(@JsonProperty("input") PushDownExpression input, @JsonProperty("type") String type)
    {
        this.input = requireNonNull(input, "input is null");
        this.type = requireNonNull(type, "type is null");
    }

    @JsonProperty
    public PushDownExpression getInput()
    {
        return input;
    }

    @JsonProperty
    public String getType()
    {
        return type;
    }

    @Override
    public <R, C> R accept(PushDownExpressionVisitor<R, C> visitor, C context)
    {
        return visitor.visitCastExpression(this, context);
    }

    @Override
    public String toString()
    {
        return "cast(" + input.toString() + " as " + type + ")";
    }
}
