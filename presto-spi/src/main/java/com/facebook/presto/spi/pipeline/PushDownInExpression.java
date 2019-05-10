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

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class PushDownInExpression
        extends PushDownExpression
{
    private final boolean isWhiteList;
    private final PushDownExpression value;
    private final List<PushDownExpression> arguments;

    @JsonCreator
    public PushDownInExpression(
            @JsonProperty("type") TypeSignature type,
            @JsonProperty("isWhiteList") boolean isWhiteList,
            @JsonProperty("value") PushDownExpression value,
            @JsonProperty("arguments") List<PushDownExpression> arguments)
    {
        super(type);
        this.isWhiteList = isWhiteList;
        this.value = requireNonNull(value, "value is null");
        this.arguments = requireNonNull(arguments, "arguments is null");
    }

    @JsonProperty
    public boolean isWhiteList()
    {
        return isWhiteList;
    }

    @JsonProperty
    public PushDownExpression getValue()
    {
        return value;
    }

    @JsonProperty
    public List<PushDownExpression> getArguments()
    {
        return arguments;
    }

    @Override
    public String toString()
    {
        return value + (isWhiteList ? " IN " : " NOT IN ") + "(" + arguments.stream().map(a -> a.toString()).collect(Collectors.joining(", ")) + ")";
    }

    @Override
    public <R, C> R accept(PushDownExpressionVisitor<R, C> visitor, C context)
    {
        return visitor.visitInExpression(this, context);
    }
}
