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
package com.facebook.presto.geospatial;

import com.facebook.presto.geospatial.serde.GeometrySerde;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;

import static com.facebook.presto.geospatial.serde.GeometrySerde.geometryFromBinary;
import static com.facebook.presto.geospatial.serde.GeometrySerde.geometryFromText;
import static com.facebook.presto.geospatial.serde.GeometryType.GEOMETRY_TYPE_NAME;
import static com.facebook.presto.spi.function.OperatorType.CAST;

public final class GeometryCastOperators
{
    private GeometryCastOperators()
    {
    }

    @ScalarOperator(CAST)
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice castTVarchar(@SqlType(StandardTypes.VARCHAR) Slice value)
    {
        return GeometrySerde.serialize(geometryFromText(value));
    }

    @ScalarOperator(CAST)
    @LiteralParameters("x")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice castToChar(@SqlType("char(x)") Slice value)
    {
        return GeometrySerde.serialize(geometryFromText(value));
    }

    @ScalarOperator(CAST)
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice castToVarBinary(@SqlType(StandardTypes.VARBINARY) Slice value)
    {
        return GeometrySerde.serialize(geometryFromBinary(value));
    }
}
