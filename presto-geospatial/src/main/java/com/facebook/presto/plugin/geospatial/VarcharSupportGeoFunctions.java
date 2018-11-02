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
package com.facebook.presto.plugin.geospatial;

import com.esri.core.geometry.ogc.OGCGeometry;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;

import static com.facebook.presto.geospatial.serde.GeometryType.GEOMETRY_TYPE_NAME;
import static com.facebook.presto.plugin.geospatial.GeoFunctions.geometryFromText;
import static com.facebook.presto.plugin.geospatial.GeoFunctions.stGeometryFromText;
import static com.facebook.presto.plugin.geospatial.GeometryUtils.serializeGeometry;
import static com.facebook.presto.spi.type.StandardTypes.VARBINARY;
import static com.facebook.presto.spi.type.StandardTypes.VARCHAR;

// VarcharSupportGeoFunctions add varchar parameter support for all GeoFunctions
public final class VarcharSupportGeoFunctions
{
    private VarcharSupportGeoFunctions() {}

    @Description("Returns binary of geometry")
    @ScalarFunction("st_geometry_from_text")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stGeometryFromTextLagacyName(@SqlType(VARCHAR) Slice input)
    {
        return stGeometryFromText(input);
    }

    @ScalarFunction("st_geometry_to_varbinary")
    @SqlType(VARBINARY)
    public static Slice stGeometryToBinary(@SqlType(StandardTypes.VARCHAR) Slice input)
    {
        OGCGeometry ogcGeometry = geometryFromText(input);
        return serializeGeometry(ogcGeometry);
    }
}
