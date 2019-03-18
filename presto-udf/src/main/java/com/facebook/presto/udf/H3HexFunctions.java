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
package com.facebook.presto.udf;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import com.uber.h3core.H3Core;
import com.uber.h3core.util.Vector;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public final class H3HexFunctions
{
    private static final H3Core h3;
    private H3HexFunctions() {}

    static {
        try {
            h3 = new H3Core();
        }
        catch (IOException ex) {
            throw new RuntimeException("Failed to load H3Core", ex);
        }
    }

    @Description("Returns hexagon address calculated from H3 library.\\nlat(double): latitude of the coordinate\\nlng(double): longitude of the coordinate\\nres(int): resolution of the address, between 0 and 15 inclusive")
    @ScalarFunction("get_hexagon_addr")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice getHexagonAddr(@SqlType(StandardTypes.DOUBLE) @SqlNullable Double lat,
                                       @SqlType(StandardTypes.DOUBLE) @SqlNullable Double lng,
                                       @SqlType(StandardTypes.BIGINT) @SqlNullable Long res)
    {
        return Slices.utf8Slice(h3.geoToH3Address(lat.doubleValue(), lng.doubleValue(), res.intValue()));
    }

    @Description("Returns wkt from hex address from H3 library.")
    @ScalarFunction("get_hexagon_addr_wkt")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice getHexagonAddrWkt(@SqlType(StandardTypes.VARCHAR) @SqlNullable Slice hexAddr)
    {
        String hexAddress = hexAddr.toStringUtf8();
        if (hexAddress.length() == 0) {
            return null;
        }
        List<Vector> boundaries = h3.h3ToGeoBoundary(hexAddress);
        String result = boundaries.stream().map(latLng -> new String(String.valueOf(latLng.y) + " " + String.valueOf(latLng.x))).collect(Collectors.joining(","));

        if (result.length() > 0) {
            return Slices.utf8Slice("POLYGON ((" + result + "))");
        }
        else {
            return null;
        }
    }
}
