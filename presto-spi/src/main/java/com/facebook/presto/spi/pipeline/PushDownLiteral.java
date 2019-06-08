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

import static java.lang.String.format;

public class PushDownLiteral
        extends PushDownExpression
{
    private final String strValue;
    private final Long longValue;
    private final Double doubleValue;
    private final Boolean booleanValue;

    @JsonCreator
    public PushDownLiteral(
            @JsonProperty("type") TypeSignature type,
            @JsonProperty("strValue") String strValue,
            @JsonProperty("longValue") Long longValue,
            @JsonProperty("doubleValue") Double doubleValue,
            @JsonProperty("booleanValue") Boolean booleanValue)
    {
        super(type);
        // TODO: check only one of them is non-null
        this.strValue = strValue;
        this.longValue = longValue;
        this.doubleValue = doubleValue;
        this.booleanValue = booleanValue;
    }

    @JsonProperty
    public String getStrValue()
    {
        return strValue;
    }

    @JsonProperty
    public Long getLongValue()
    {
        return longValue;
    }

    @JsonProperty
    public Double getDoubleValue()
    {
        return doubleValue;
    }

    public Boolean getBooleanValue()
    {
        return booleanValue;
    }

    @Override
    public String toString()
    {
        if (strValue != null) {
            return "'" + strValue + "'";
        }

        if (longValue != null) {
            return format("%d", longValue);
        }

        if (doubleValue != null) {
            return format("%f", doubleValue);
        }

        if (booleanValue != null) {
            return String.valueOf(booleanValue);
        }

        return null;
    }

    @Override
    public <R, C> R accept(PushDownExpressionVisitor<R, C> visitor, C context)
    {
        return visitor.visitLiteral(this, context);
    }
}
