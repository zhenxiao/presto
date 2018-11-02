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

import com.esri.core.geometry.OperatorContains;
import com.esri.core.geometry.OperatorIntersects;
import com.esri.core.geometry.OperatorSimpleRelation;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.facebook.presto.geospatial.serde.GeometrySerde;
import com.facebook.presto.geospatial.serde.GeometryType;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.hash.HashCode;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.facebook.presto.geospatial.serde.GeometryType.GEOMETRY_TYPE_NAME;
import static com.facebook.presto.plugin.geospatial.GeoFunctions.stAsText;
import static com.facebook.presto.plugin.geospatial.geoindex.QuadTreeUtils.queryIndexValue;
import static com.facebook.presto.spi.StandardErrorCode.GEOSPATIAL_CREATE_INDEX_ERROR;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;

public class GeoIndexFunctions
{
    private static final long MAX_INDICES_SIZE = 20;
    private static final Cache<HashCode, QuadTreeUtils.GeoIndex> QuardTreeCache = CacheBuilder.newBuilder().maximumSize(MAX_INDICES_SIZE).build();

    private GeoIndexFunctions() {}

    @SqlNullable
    @Description("Returns the first key whose corresponding geometry contains geo shape")
    @ScalarFunction("geo_contains")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice geoContainsGeometryType(@SqlType(GEOMETRY_TYPE_NAME) Slice shape, @SqlType(StandardTypes.VARCHAR) Slice geoShapesBlock)
    {
        List<String> results = processGeoString(stAsText(shape), geoShapesBlock, OperatorContains.local(), true);
        if (!results.isEmpty()) {
            return Slices.utf8Slice(results.get(0));
        }
        return null;
    }

    @SqlNullable
    @Description("Returns the first key whose corresponding geometry intersects with geo shape")
    @ScalarFunction("geo_intersects")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice geoIntersectsGeometryType(@SqlType(GEOMETRY_TYPE_NAME) Slice shape, @SqlType(StandardTypes.VARCHAR) Slice geoShapesBlock)
    {
        List<String> results = processGeoString(stAsText(shape), geoShapesBlock, OperatorIntersects.local(), true);
        if (!results.isEmpty()) {
            return Slices.utf8Slice(results.get(0));
        }
        return null;
    }

    @SqlNullable
    @Description("Returns all keys whose corresponding geometry contains geo shape")
    @ScalarFunction("geo_contains_all")
    @SqlType("array(varchar)")
    public static Block geoContainsAllGeometryType(@SqlType(GeometryType.GEOMETRY_TYPE_NAME) Slice shape, @SqlType(StandardTypes.VARCHAR) Slice geoShapesBlock)
    {
        List<String> results = processGeoString(stAsText(shape), geoShapesBlock, OperatorContains.local(), false);
        if (!results.isEmpty()) {
            BlockBuilder blockBuilder = VarcharType.VARCHAR.createBlockBuilder(null, results.size());
            for (String str : results) {
                VarcharType.VARCHAR.writeString(blockBuilder, str);
            }
            return blockBuilder.build();
        }
        return null;
    }

    @SqlNullable
    @Description("Returns all keys whose corresponding geometry intersects with geo shape")
    @ScalarFunction("geo_intersects_all")
    @SqlType("array(varchar)")
    public static Block geoIntersectsAllGeometryType(@SqlType(GeometryType.GEOMETRY_TYPE_NAME) Slice shape, @SqlType(StandardTypes.VARCHAR) Slice geoShapesBlock)
    {
        List<String> results = processGeoString(stAsText(shape), geoShapesBlock, OperatorIntersects.local(), false);
        if (!results.isEmpty()) {
            BlockBuilder blockBuilder = VarcharType.VARCHAR.createBlockBuilder(null, results.size());
            for (String str : results) {
                VarcharType.VARCHAR.writeString(blockBuilder, str);
            }
            return blockBuilder.build();
        }
        return null;
    }

    private static List<String> processGeoString(Slice shape, Slice geoShapesBlock, OperatorSimpleRelation relationOperator, boolean getFirst)
    {
        return queryIndexValue(OGCGeometry.fromText(shape.toStringUtf8()), getGeoIndex(geoShapesBlock), relationOperator, getFirst);
    }

    private static QuadTreeUtils.GeoIndex getGeoIndex(Slice slice)
    {
        HashCode hashCode = HashCode.fromInt(slice.getInt(0));
        try {
            return QuardTreeCache.get(hashCode, () -> createGeoIndex(slice));
        }
        catch (ExecutionException e) {
            throw new PrestoException(GEOSPATIAL_CREATE_INDEX_ERROR, "not able to create geo index", e);
        }
    }

    private static QuadTreeUtils.GeoIndex createGeoIndex(Slice slice)
    {
        int geoShapeDataSize = slice.getInt(SIZE_OF_INT);
        byte[] bytes = slice.getBytes(SIZE_OF_INT + SIZE_OF_INT, geoShapeDataSize);
        List<QuadTreeUtils.GeoItem> geoShapes = GeoShape.deserialize(Slices.wrappedBuffer(bytes)).stream().map(geoShape -> new
                QuadTreeUtils.GeoItem(geoShape.id.toStringUtf8(), GeometrySerde.deserialize(geoShape.geoShape))).collect
                (Collectors.toList());
        return QuadTreeUtils.init(geoShapes);
    }
}
