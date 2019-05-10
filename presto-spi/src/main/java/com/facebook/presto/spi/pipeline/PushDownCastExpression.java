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

import java.util.Optional;
import java.util.function.BiFunction;

import static java.util.Objects.requireNonNull;

public class PushDownCastExpression
        extends PushDownExpression
{
    private final PushDownExpression input;
    private final String resultType;
    private final boolean implicit;

    @JsonCreator
    public PushDownCastExpression(@JsonProperty("type") TypeSignature type, @JsonProperty("input") PushDownExpression input, @JsonProperty("resultType") String resultType, @JsonProperty("implicit") boolean implicit)
    {
        super(type);
        this.input = requireNonNull(input, "input is null");
        this.resultType = requireNonNull(resultType, "type is null");
        this.implicit = implicit;
    }

    @JsonProperty
    public PushDownExpression getInput()
    {
        return input;
    }

    @JsonProperty
    public String getResultType()
    {
        return resultType;
    }

    @JsonProperty
    public boolean isImplicit()
    {
        return implicit;
    }

    @Override
    public <R, C> R accept(PushDownExpressionVisitor<R, C> visitor, C context)
    {
        return visitor.visitCastExpression(this, context);
    }

    @Override
    public String toString()
    {
        return "cast(" + input.toString() + " as " + resultType + ")";
    }

    public boolean isImplicitCast(Optional<BiFunction<String, TypeSignature, Boolean>> otherCheck)
    {
        return implicit || otherCheck.orElse((x, y) -> false).apply(getResultType(), input.getType());
    }
}
