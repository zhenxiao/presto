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
package com.facebook.presto.plugin.geospatial.geoindex;

import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.geospatial.serde.GeometryType.GEOMETRY_TYPE_NAME;
import static com.facebook.presto.spi.type.StandardTypes.VARCHAR;

@AggregationFunction(value = "build_geo_index", decomposable = false)
public final class AggregateGeoData
{
    private AggregateGeoData() {}

    @InputFunction
    public static void putGeoVarchar(GeoData geoData, @SqlType(VARCHAR) Slice id, @SqlType(VARCHAR) Slice geoShape)
    {
        geoData.addVarcharShape(id, geoShape);
    }

    @InputFunction
    public static void putGeoBinary(GeoData geoData, @SqlType(VARCHAR) Slice id, @SqlType(GEOMETRY_TYPE_NAME) Slice geoShape)
    {
        geoData.addBinaryShape(id, geoShape);
    }

    @InputFunction
    public static void putVarcharGeometry(GeoData geoData, @SqlType(VARCHAR) Slice geoShape)
    {
        geoData.addVarcharShape(Slices.EMPTY_SLICE, geoShape);
    }

    @InputFunction
    public static void putVarbinaryGeometry(GeoData geoData, @SqlType(GEOMETRY_TYPE_NAME) Slice geoShape)
    {
        geoData.addBinaryShape(Slices.EMPTY_SLICE, geoShape);
    }

    @CombineFunction
    public static void combine(GeoData state, GeoData otherState)
    {
    }

    @OutputFunction(StandardTypes.VARCHAR)
    public static void output(GeoData state, BlockBuilder out)
    {
        state.output(out);
    }
}
