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
package com.facebook.presto.geo;

import ch.hsr.geohash.GeoHash;
import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.MultiPath;
import com.esri.core.geometry.MultiPoint;
import com.esri.core.geometry.OperatorContains;
import com.esri.core.geometry.OperatorCrosses;
import com.esri.core.geometry.OperatorDisjoint;
import com.esri.core.geometry.OperatorEquals;
import com.esri.core.geometry.OperatorIntersects;
import com.esri.core.geometry.OperatorOverlaps;
import com.esri.core.geometry.OperatorSimpleRelation;
import com.esri.core.geometry.OperatorTouches;
import com.esri.core.geometry.OperatorWithin;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCLineString;
import com.esri.core.geometry.ogc.OGCMultiLineString;
import com.esri.core.geometry.ogc.OGCPoint;
import com.esri.core.geometry.ogc.OGCPolygon;
import com.facebook.presto.geo.QuadTreeUtils.GeoIndex;
import com.facebook.presto.geo.QuadTreeUtils.GeoItem;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashCode;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.esri.core.geometry.ogc.OGCGeometry.createFromEsriGeometry;
import static com.esri.core.geometry.ogc.OGCGeometry.fromText;
import static com.facebook.presto.geo.GeometryUtils.OGCType.ST_MULTIPOLYGON;
import static com.facebook.presto.geo.GeometryUtils.OGCType.ST_POLYGON;
import static com.facebook.presto.geo.GeometryUtils.WKID_UNKNOWN;
import static com.facebook.presto.geo.GeometryUtils.geometryFromBinary;
import static com.facebook.presto.geo.GeometryUtils.serializeGeometry;
import static com.facebook.presto.geo.QuadTreeUtils.queryGeoItem;
import static com.facebook.presto.geo.QuadTreeUtils.queryGeometry;
import static com.facebook.presto.geo.QuadTreeUtils.queryIndexValue;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.GEOSPATIAL_CREATE_INDEX_ERROR;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;

public final class GeoFunctions
{
    private static final String LINE = "LineString";
    private static final String MULTILINE = "MultiLineString";
    private static final String MULTIPOINT = "MultiPoint";
    private static final String POLYGON = "Polygon";
    private static final String POINT = "Point";
    private static final long MAX_INDICES_SIZE = 20;
    private static final Cache<HashCode, GeoIndex> QuardTreeCache = CacheBuilder.newBuilder().maximumSize(MAX_INDICES_SIZE).build();

    private GeoFunctions() {}

    @Description("Returns binary representation of a Line")
    @ScalarFunction
    @SqlType(StandardTypes.VARBINARY)
    public static Slice stLine(@SqlType(StandardTypes.VARCHAR) Slice input)
    {
        OGCGeometry line = fromText(input.toStringUtf8());
        line.setSpatialReference(null);
        return serializeGeometry(line);
    }

    @Description("Returns binary representation of a Point")
    @ScalarFunction
    @SqlType(StandardTypes.VARBINARY)
    public static Slice stPoint(@SqlType(StandardTypes.DOUBLE) double longitude, @SqlType(StandardTypes.DOUBLE) double latitude)
    {
        OGCGeometry geometry = createFromEsriGeometry(new Point(longitude, latitude), null);
        return serializeGeometry(geometry);
    }

    @Description("Returns binary representation of a Polygon")
    @ScalarFunction
    @SqlType(StandardTypes.VARBINARY)
    public static Slice stPolygon(@SqlType(StandardTypes.VARCHAR) Slice input)
    {
        OGCGeometry polygon = fromText(input.toStringUtf8());
        polygon.setSpatialReference(null);
        return serializeGeometry(polygon);
    }

    @Description("Returns area of input polygon")
    @ScalarFunction("st_area")
    @SqlType(StandardTypes.DOUBLE)
    public static double stAreaWithText(@SqlType(StandardTypes.VARCHAR) Slice input)
    {
        OGCGeometry geometry = fromText(input.toStringUtf8());
        return geometry.getEsriGeometry().calculateArea2D();
    }

    @Description("Returns area of input polygon")
    @ScalarFunction("st_area")
    @SqlType(StandardTypes.DOUBLE)
    public static double stAreaWithBinary(@SqlType(StandardTypes.VARBINARY) Slice input)
    {
        OGCGeometry geometry = GeometryUtils.geometryFromBinary(input);
        return geometry.getEsriGeometry().calculateArea2D();
    }

    @Description("Returns point that is the center of the polygon's envelope")
    @ScalarFunction
    @SqlType(StandardTypes.VARBINARY)
    public static Slice stCentroid(@SqlType(StandardTypes.VARBINARY) Slice input)
    {
        OGCGeometry geometry = GeometryUtils.geometryFromBinary(input);
        GeometryUtils.OGCType geomType = GeometryUtils.getGeomType(input);
        if (geomType == ST_POLYGON || geomType == ST_MULTIPOLYGON) {
            int id = GeometryUtils.getWkid(input);
            SpatialReference reference = (id != WKID_UNKNOWN) ? SpatialReference.create(id) : null;
            Envelope envelope = new Envelope();
            geometry.getEsriGeometry().queryEnvelope(envelope);
            Point centroid = new Point((envelope.getXMin() + envelope.getXMax()) / 2, (envelope.getYMin() + envelope.getYMax()) / 2);
            return serializeGeometry(createFromEsriGeometry(centroid, reference));
        }
        throw new IllegalArgumentException("st_centroid only applies to polygon");
    }

    @Description("Returns count of coordinate components")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long stCoordinateDimension(@SqlType(StandardTypes.VARBINARY) Slice input)
    {
        OGCGeometry geometry = GeometryUtils.geometryFromBinary(input);
        return geometry.coordinateDimension();
    }

    @Description("Returns spatial dimension of geometry")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long stDimension(@SqlType(StandardTypes.VARBINARY) Slice input)
    {
        OGCGeometry geometry = GeometryUtils.geometryFromBinary(input);
        return geometry.dimension();
    }

    @SqlNullable
    @Description("Returns true if and only if the line is closed")
    @ScalarFunction("st_is_closed")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stIsClosedStrInput(@SqlType(StandardTypes.VARCHAR) Slice input)
    {
        OGCGeometry geometry = fromText(input.toStringUtf8());
        return stIsClosedImpl(geometry);
    }

    @SqlNullable
    @Description("Returns true if and only if the line is closed")
    @ScalarFunction("st_is_closed")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stIsClosedBinaryInput(@SqlType(StandardTypes.VARBINARY) Slice input)
    {
        return stIsClosedImpl(GeometryUtils.geometryFromBinary(input));
    }

    private static Boolean stIsClosedImpl(OGCGeometry geometry)
    {
        if (geometry.geometryType().equals(LINE) || geometry.geometryType().equals(MULTILINE)) {
            MultiPath lines = (MultiPath) geometry.getEsriGeometry();
            int pathCount = lines.getPathCount();
            boolean result = true;
            for (int i = 0; result && i < pathCount; i++) {
                Point start = lines.getPoint(lines.getPathStart(i));
                Point end = lines.getPoint(lines.getPathEnd(i) - 1);
                result = result && end.equals(start);
            }
            return result;
        }
        throw new IllegalArgumentException("st_is_closed only support line and multiline");
    }

    @SqlNullable
    @Description("Returns true if and only if the geometry is empty")
    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stIsEmpty(@SqlType(StandardTypes.VARBINARY) Slice input)
    {
        OGCGeometry geometry = GeometryUtils.geometryFromBinary(input);
        return geometry.isEmpty();
    }

    @Description("Returns the length of line")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double stLength(@SqlType(StandardTypes.VARBINARY) Slice input)
    {
        OGCGeometry geometry = GeometryUtils.geometryFromBinary(input);
        return geometry.getEsriGeometry().calculateLength2D();
    }

    @Description("Returns the maximum X coordinate of geometry")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double stMaxX(@SqlType(StandardTypes.VARBINARY) Slice input)
    {
        OGCGeometry geometry = GeometryUtils.geometryFromBinary(input);
        Envelope envelope = new Envelope();
        geometry.getEsriGeometry().queryEnvelope(envelope);
        return envelope.getXMax();
    }

    @Description("Returns the maximum Y coordinate of geometry")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double stMaxY(@SqlType(StandardTypes.VARBINARY) Slice input)
    {
        OGCGeometry geometry = GeometryUtils.geometryFromBinary(input);
        Envelope envelope = new Envelope();
        geometry.getEsriGeometry().queryEnvelope(envelope);
        return envelope.getYMax();
    }

    @Description("Returns the minimum X coordinate of geometry")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double stMinX(@SqlType(StandardTypes.VARBINARY) Slice input)
    {
        OGCGeometry geometry = GeometryUtils.geometryFromBinary(input);
        Envelope envelope = new Envelope();
        geometry.getEsriGeometry().queryEnvelope(envelope);
        return envelope.getXMin();
    }

    @Description("Returns the minimum Y coordinate of geometry")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double stMinY(@SqlType(StandardTypes.VARBINARY) Slice input)
    {
        OGCGeometry geometry = GeometryUtils.geometryFromBinary(input);
        Envelope envelope = new Envelope();
        geometry.getEsriGeometry().queryEnvelope(envelope);
        return envelope.getYMin();
    }

    @Description("Returns the number of interior rings in the polygon")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long stInteriorRingNumber(@SqlType(StandardTypes.VARBINARY) Slice input)
    {
        OGCGeometry geometry = GeometryUtils.geometryFromBinary(input);
        if (geometry.geometryType().equals(POLYGON)) {
            return ((OGCPolygon) geometry).numInteriorRing();
        }
        throw new IllegalArgumentException("st_interior_ring_number only applies to polygon");
    }

    @Description("Returns the number of points in the geometry")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long stPointNumber(@SqlType(StandardTypes.VARBINARY) Slice input)
    {
        OGCGeometry geometry = GeometryUtils.geometryFromBinary(input);
        if (geometry.geometryType().equals(POLYGON)) {
            Polygon polygon = (Polygon) geometry.getEsriGeometry();
            return polygon.getPointCount() + polygon.getPathCount();
        }
        else if (geometry.geometryType().equals(POINT)) {
            return geometry.getEsriGeometry().isEmpty() ? 0 : 1;
        }
        else if (geometry.geometryType().equals(MULTIPOINT)) {
            return ((MultiPoint) geometry.getEsriGeometry()).getPointCount();
        }
        return ((MultiPath) geometry.getEsriGeometry()).getPointCount();
    }

    @SqlNullable
    @Description("Returns true if and only if the line is closed and simple")
    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stIsRing(@SqlType(StandardTypes.VARBINARY) Slice input)
    {
        OGCGeometry geometry = GeometryUtils.geometryFromBinary(input);
        if (geometry.geometryType().equals(LINE)) {
            OGCLineString line = (OGCLineString) geometry;
            return line.isClosed() && line.isSimple();
        }
        throw new IllegalArgumentException("st_is_ring only applies to linestring");
    }

    @Description("Returns the first point of an line")
    @ScalarFunction
    @SqlType(StandardTypes.VARBINARY)
    public static Slice stStartPoint(@SqlType(StandardTypes.VARBINARY) Slice input)
    {
        OGCGeometry geometry = GeometryUtils.geometryFromBinary(input);
        if (geometry.geometryType().equals(LINE)) {
            MultiPath lines = (MultiPath) geometry.getEsriGeometry();
            int id = GeometryUtils.getWkid(input);
            SpatialReference reference = (id != WKID_UNKNOWN) ? SpatialReference.create(id) : null;
            return serializeGeometry(createFromEsriGeometry(lines.getPoint(0), reference));
        }
        throw new IllegalArgumentException("st_start_point only applies to linestring");
    }

    @Description("Returns the last point of an line")
    @ScalarFunction
    @SqlType(StandardTypes.VARBINARY)
    public static Slice stEndPoint(@SqlType(StandardTypes.VARBINARY) Slice input)
    {
        OGCGeometry geometry = GeometryUtils.geometryFromBinary(input);
        if (geometry.geometryType().equals(LINE)) {
            MultiPath lines = (MultiPath) geometry.getEsriGeometry();
            int id = GeometryUtils.getWkid(input);
            SpatialReference reference = (id != WKID_UNKNOWN) ? SpatialReference.create(id) : null;
            return serializeGeometry(createFromEsriGeometry(lines.getPoint(lines.getPointCount() - 1), reference));
        }
        throw new IllegalArgumentException("st_end_point only applies to linestring");
    }

    @Description("Returns the X coordinate of point")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double stX(@SqlType(StandardTypes.VARBINARY) Slice input)
    {
        OGCGeometry geometry = GeometryUtils.geometryFromBinary(input);
        if (geometry.geometryType().equals(POINT)) {
            return ((OGCPoint) geometry).X();
        }
        throw new IllegalArgumentException("st_x only applies to point");
    }

    @Description("Returns the Y coordinate of point")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double stY(@SqlType(StandardTypes.VARBINARY) Slice input)
    {
        OGCGeometry geometry = GeometryUtils.geometryFromBinary(input);
        if (geometry.geometryType().equals(POINT)) {
            return ((OGCPoint) geometry).Y();
        }
        throw new IllegalArgumentException("st_y only applies to point");
    }

    @Description("Returns string representation of the boundary geometry of input geometry")
    @ScalarFunction
    @SqlType(StandardTypes.VARBINARY)
    public static Slice stBoundary(@SqlType(StandardTypes.VARBINARY) Slice input)
    {
        OGCGeometry geometry = GeometryUtils.geometryFromBinary(input);
        OGCGeometry bound = geometry.boundary();
        if (bound.geometryType().equals(MULTILINE) && ((OGCMultiLineString) bound).numGeometries() == 1) {
            bound = ((OGCMultiLineString) bound).geometryN(0);
        }
        return serializeGeometry(bound);
    }

    @Description("Returns string representation of envelope of the input geometry")
    @ScalarFunction
    @SqlType(StandardTypes.VARBINARY)
    public static Slice stEnvelope(@SqlType(StandardTypes.VARBINARY) Slice input)
    {
        OGCGeometry geometry = GeometryUtils.geometryFromBinary(input);
        int id = GeometryUtils.getWkid(input);
        SpatialReference reference = (id != WKID_UNKNOWN) ? SpatialReference.create(id) : null;
        Envelope envelope = new Envelope();
        geometry.getEsriGeometry().queryEnvelope(envelope);
        return serializeGeometry(createFromEsriGeometry(envelope, reference));
    }

    @Description("Returns string representation of the geometry buffered by distance")
    @ScalarFunction
    @SqlType(StandardTypes.VARBINARY)
    public static Slice stBuffer(@SqlType(StandardTypes.VARBINARY) Slice input, @SqlType(StandardTypes.DOUBLE) double distance)
    {
        OGCGeometry geometry = GeometryUtils.geometryFromBinary(input);
        return serializeGeometry(geometry.buffer(distance));
    }

    @Description("Returns string representation of the geometry difference of left geometry and right geometry")
    @ScalarFunction
    @SqlType(StandardTypes.VARBINARY)
    public static Slice stDifference(@SqlType(StandardTypes.VARBINARY) Slice left, @SqlType(StandardTypes.VARBINARY) Slice right)
    {
        OGCGeometry leftGeometry = GeometryUtils.geometryFromBinary(left);
        OGCGeometry rightGeometry = GeometryUtils.geometryFromBinary(right);
        return serializeGeometry(leftGeometry.difference(rightGeometry));
    }

    @Description("Returns distance between left geometry and right geometry")
    @ScalarFunction("st_distance")
    @SqlType(StandardTypes.DOUBLE)
    public static double stDistanceVarchar(@SqlType(StandardTypes.VARCHAR) Slice left, @SqlType(StandardTypes.VARCHAR) Slice right)
    {
        OGCGeometry leftGeometry = fromText(left.toStringUtf8());
        OGCGeometry rightGeometry = fromText(right.toStringUtf8());
        return leftGeometry.distance(rightGeometry);
    }

    @Description("Returns distance between left geometry and right geometry")
    @ScalarFunction("st_distance")
    @SqlType(StandardTypes.DOUBLE)
    public static double stDistanceBinary(@SqlType(StandardTypes.VARBINARY) Slice left, @SqlType(StandardTypes.VARBINARY) Slice right)
    {
        OGCGeometry leftGeometry = geometryFromBinary(left);
        OGCGeometry rightGeometry = geometryFromBinary(right);
        return leftGeometry.distance(rightGeometry);
    }

    @Description("Returns string representation of the exterior ring of the polygon")
    @ScalarFunction
    @SqlType(StandardTypes.VARBINARY)
    public static Slice stExteriorRing(@SqlType(StandardTypes.VARBINARY) Slice input)
    {
        OGCGeometry geometry = GeometryUtils.geometryFromBinary(input);
        if (geometry.geometryType().equals(POLYGON)) {
            try {
                Method method = OGCPolygon.class.getMethod("exteriorRing");
                OGCLineString exteriorRing = (OGCLineString) method.invoke(geometry);
                return serializeGeometry(exteriorRing);
            }
            catch (Exception e) {
                throw new IllegalArgumentException("st_exterior_ring only applies to polygon");
            }
        }
        throw new IllegalArgumentException("st_exterior_ring only applies to polygon");
    }

    @Description("Returns string representation of the geometry intersection of left geometry and right geometry")
    @ScalarFunction
    @SqlType(StandardTypes.VARBINARY)
    public static Slice stIntersection(@SqlType(StandardTypes.VARBINARY) Slice left, @SqlType(StandardTypes.VARBINARY) Slice right)
    {
        OGCGeometry leftGeometry = GeometryUtils.geometryFromBinary(left);
        OGCGeometry rightGeometry = GeometryUtils.geometryFromBinary(right);
        return serializeGeometry(leftGeometry.intersection(rightGeometry));
    }

    @Description("Returns string representation of the geometry symmetric difference of left geometry and right geometry")
    @ScalarFunction
    @SqlType(StandardTypes.VARBINARY)
    public static Slice stSymmetricDifference(@SqlType(StandardTypes.VARBINARY) Slice left, @SqlType(StandardTypes.VARBINARY) Slice right)
    {
        OGCGeometry leftGeometry = GeometryUtils.geometryFromBinary(left);
        OGCGeometry rightGeometry = GeometryUtils.geometryFromBinary(right);
        if (GeometryUtils.getWkid(left) != GeometryUtils.getWkid(right)) {
            return Slices.EMPTY_SLICE;
        }
        return serializeGeometry(leftGeometry.symDifference(rightGeometry));
    }

    @SqlNullable
    @Description("Returns true if and only if left geometry contains right geometry")
    @ScalarFunction("st_contains")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stContainsVarchar(@SqlType(StandardTypes.VARCHAR) Slice left, @SqlType(StandardTypes.VARCHAR) Slice right)
    {
        OGCGeometry leftGeometry = fromText(left.toStringUtf8());
        OGCGeometry rightGeometry = fromText(right.toStringUtf8());
        OperatorSimpleRelation containsOperator = OperatorContains.local();
        return containsOperator.execute(leftGeometry.getEsriGeometry(), rightGeometry.getEsriGeometry(), leftGeometry.getEsriSpatialReference(), null);
    }

    @SqlNullable
    @Description("Returns true if and only if left geometry contains right geometry")
    @ScalarFunction("st_contains")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stContainsBinary(@SqlType(StandardTypes.VARBINARY) Slice left, @SqlType(StandardTypes.VARBINARY) Slice right)
    {
        OGCGeometry leftGeometry = geometryFromBinary(left);
        OGCGeometry rightGeometry = geometryFromBinary(right);
        OperatorSimpleRelation containsOperator = OperatorContains.local();
        return containsOperator.execute(leftGeometry.getEsriGeometry(), rightGeometry.getEsriGeometry(), leftGeometry.getEsriSpatialReference(), null);
    }

    @SqlNullable
    @Description("Returns true if and only if left geometry contains right geometry")
    @ScalarFunction("st_contains")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stContainsVarcharBinary(@SqlType(StandardTypes.VARCHAR) Slice left, @SqlType(StandardTypes.VARBINARY) Slice right)
    {
        OGCGeometry leftGeometry = fromText(left.toStringUtf8());
        OGCGeometry rightGeometry = geometryFromBinary(right);
        OperatorSimpleRelation containsOperator = OperatorContains.local();
        return containsOperator.execute(leftGeometry.getEsriGeometry(), rightGeometry.getEsriGeometry(), leftGeometry.getEsriSpatialReference(), null);
    }

    @SqlNullable
    @Description("Returns true if and only if left geometry contains right geometry")
    @ScalarFunction("st_contains")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stContainsBinaryVarchar(@SqlType(StandardTypes.VARBINARY) Slice left, @SqlType(StandardTypes.VARCHAR) Slice right)
    {
        OGCGeometry leftGeometry = geometryFromBinary(left);
        OGCGeometry rightGeometry = fromText(right.toStringUtf8());
        OperatorSimpleRelation containsOperator = OperatorContains.local();
        return containsOperator.execute(leftGeometry.getEsriGeometry(), rightGeometry.getEsriGeometry(), leftGeometry.getEsriSpatialReference(), null);
    }

    @SqlNullable
    @Description("Returns binary of geometry")
    @ScalarFunction("st_geometry_from_text")
    @SqlType(StandardTypes.VARBINARY)
    public static Slice stGeometryFromText(@SqlType(StandardTypes.VARCHAR) Slice shape)
    {
        try {
            OGCGeometry geometry = fromText(shape.toStringUtf8());
            return serializeGeometry(geometry);
        }
        catch (Exception e) {
            throw new PrestoException(GENERIC_USER_ERROR, "Cannot convert to geometry: " + shape.toStringUtf8());
        }
    }

    @SqlNullable
    @Description("Returns text of geometry")
    @ScalarFunction("st_geometry_to_text")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice stGeometryToText(@SqlType(StandardTypes.VARBINARY) Slice shape)
    {
        OGCGeometry geometry = geometryFromBinary(shape);
        return Slices.utf8Slice(geometry.asText());
    }

    @SqlNullable
    @Description("Returns true if and only if left geometry crosses right geometry")
    @ScalarFunction("st_crosses")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stCrossesVarchar(@SqlType(StandardTypes.VARCHAR) Slice left, @SqlType(StandardTypes.VARCHAR) Slice right)
    {
        OGCGeometry leftGeometry = fromText(left.toStringUtf8());
        OGCGeometry rightGeometry = fromText(right.toStringUtf8());
        OperatorSimpleRelation crossesOperator = OperatorCrosses.local();
        return crossesOperator.execute(leftGeometry.getEsriGeometry(), rightGeometry.getEsriGeometry(), leftGeometry.getEsriSpatialReference(), null);
    }

    @SqlNullable
    @Description("Returns true if and only if left geometry crosses right geometry")
    @ScalarFunction("st_crosses")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stCrossesBinary(@SqlType(StandardTypes.VARBINARY) Slice left, @SqlType(StandardTypes.VARBINARY) Slice right)
    {
        OGCGeometry leftGeometry = GeometryUtils.geometryFromBinary(left);
        OGCGeometry rightGeometry = GeometryUtils.geometryFromBinary(right);
        OperatorSimpleRelation crossesOperator = OperatorCrosses.local();
        return crossesOperator.execute(leftGeometry.getEsriGeometry(), rightGeometry.getEsriGeometry(), leftGeometry.getEsriSpatialReference(), null);
    }

    @SqlNullable
    @Description("Returns true if and only if the intersection of left geometry and right geometry is empty")
    @ScalarFunction("st_disjoint")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stDisjointVarchar(@SqlType(StandardTypes.VARCHAR) Slice left, @SqlType(StandardTypes.VARCHAR) Slice right)
    {
        OGCGeometry leftGeometry = fromText(left.toStringUtf8());
        OGCGeometry rightGeometry = fromText(right.toStringUtf8());
        OperatorSimpleRelation disjointOperator = OperatorDisjoint.local();
        return disjointOperator.execute(leftGeometry.getEsriGeometry(), rightGeometry.getEsriGeometry(), leftGeometry.getEsriSpatialReference(), null);
    }

    @SqlNullable
    @Description("Returns true if and only if the intersection of left geometry and right geometry is empty")
    @ScalarFunction("st_disjoint")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stDisjointBinary(@SqlType(StandardTypes.VARBINARY) Slice left, @SqlType(StandardTypes.VARBINARY) Slice right)
    {
        OGCGeometry leftGeometry = GeometryUtils.geometryFromBinary(left);
        OGCGeometry rightGeometry = GeometryUtils.geometryFromBinary(right);
        OperatorSimpleRelation disjointOperator = OperatorDisjoint.local();
        return disjointOperator.execute(leftGeometry.getEsriGeometry(), rightGeometry.getEsriGeometry(), leftGeometry.getEsriSpatialReference(), null);
    }

    @SqlNullable
    @Description("Returns true if and only if the envelopes of left geometry and right geometry intersect")
    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stEnvelopeIntersect(@SqlType(StandardTypes.VARBINARY) Slice left, @SqlType(StandardTypes.VARBINARY) Slice right)
    {
        OGCGeometry leftGeometry = GeometryUtils.geometryFromBinary(left);
        OGCGeometry rightGeometry = GeometryUtils.geometryFromBinary(right);
        Envelope leftEnvelope = new Envelope();
        Envelope rightEnvelope = new Envelope();
        leftGeometry.getEsriGeometry().queryEnvelope(leftEnvelope);
        rightGeometry.getEsriGeometry().queryEnvelope(rightEnvelope);
        return leftEnvelope.isIntersecting(rightEnvelope);
    }

    @SqlNullable
    @Description("Returns true if and only if left geometry equals right geometry")
    @ScalarFunction("st_equals")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stEqualsVarchar(@SqlType(StandardTypes.VARCHAR) Slice left, @SqlType(StandardTypes.VARCHAR) Slice right)
    {
        OGCGeometry leftGeometry = fromText(left.toStringUtf8());
        OGCGeometry rightGeometry = fromText(right.toStringUtf8());
        OperatorSimpleRelation equalsOperator = OperatorEquals.local();
        return equalsOperator.execute(leftGeometry.getEsriGeometry(), rightGeometry.getEsriGeometry(), leftGeometry.getEsriSpatialReference(), null);
    }

    @SqlNullable
    @Description("Returns true if and only if left geometry equals right geometry")
    @ScalarFunction("st_equals")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stEqualsBinary(@SqlType(StandardTypes.VARBINARY) Slice left, @SqlType(StandardTypes.VARBINARY) Slice right)
    {
        OGCGeometry leftGeometry = GeometryUtils.geometryFromBinary(left);
        OGCGeometry rightGeometry = GeometryUtils.geometryFromBinary(right);
        OperatorSimpleRelation equalsOperator = OperatorEquals.local();
        return equalsOperator.execute(leftGeometry.getEsriGeometry(), rightGeometry.getEsriGeometry(), leftGeometry.getEsriSpatialReference(), null);
    }

    @SqlNullable
    @Description("Returns true if and only if left geometry intersects right geometry")
    @ScalarFunction("st_intersects")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stIntersectsVarchar(@SqlType(StandardTypes.VARCHAR) Slice left, @SqlType(StandardTypes.VARCHAR) Slice right)
    {
        OGCGeometry leftGeometry = fromText(left.toStringUtf8());
        OGCGeometry rightGeometry = fromText(right.toStringUtf8());
        OperatorSimpleRelation intersectsOperator = OperatorIntersects.local();
        return intersectsOperator.execute(leftGeometry.getEsriGeometry(), rightGeometry.getEsriGeometry(), leftGeometry.getEsriSpatialReference(), null);
    }

    @SqlNullable
    @Description("Returns true if and only if left geometry intersects right geometry")
    @ScalarFunction("st_intersects")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stIntersectsBinary(@SqlType(StandardTypes.VARBINARY) Slice left, @SqlType(StandardTypes.VARBINARY) Slice right)
    {
        OGCGeometry leftGeometry = GeometryUtils.geometryFromBinary(left);
        OGCGeometry rightGeometry = GeometryUtils.geometryFromBinary(right);
        OperatorSimpleRelation intersectsOperator = OperatorIntersects.local();
        return intersectsOperator.execute(leftGeometry.getEsriGeometry(), rightGeometry.getEsriGeometry(), leftGeometry.getEsriSpatialReference(), null);
    }

    @SqlNullable
    @Description("Returns true if and only if left geometry intersects right geometry")
    @ScalarFunction("st_intersects")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stIntersectsBinaryVarchar(@SqlType(StandardTypes.VARBINARY) Slice left, @SqlType(StandardTypes.VARCHAR) Slice right)
    {
        OGCGeometry leftGeometry = GeometryUtils.geometryFromBinary(left);
        OGCGeometry rightGeometry = fromText(right.toStringUtf8());
        OperatorSimpleRelation intersectsOperator = OperatorIntersects.local();
        return intersectsOperator.execute(leftGeometry.getEsriGeometry(), rightGeometry.getEsriGeometry(), leftGeometry.getEsriSpatialReference(), null);
    }

    @SqlNullable
    @Description("Returns true if and only if left geometry intersects right geometry")
    @ScalarFunction("st_intersects")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stIntersectsVarcharBinary(@SqlType(StandardTypes.VARCHAR) Slice left, @SqlType(StandardTypes.VARBINARY) Slice right)
    {
        OGCGeometry leftGeometry = fromText(left.toStringUtf8());
        OGCGeometry rightGeometry = GeometryUtils.geometryFromBinary(right);
        OperatorSimpleRelation intersectsOperator = OperatorIntersects.local();
        return intersectsOperator.execute(leftGeometry.getEsriGeometry(), rightGeometry.getEsriGeometry(), leftGeometry.getEsriSpatialReference(), null);
    }

    @SqlNullable
    @Description("Returns true if and only if left geometry overlaps right geometry")
    @ScalarFunction("st_overlaps")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stOverlapsVarchar(@SqlType(StandardTypes.VARCHAR) Slice left, @SqlType(StandardTypes.VARCHAR) Slice right)
    {
        OGCGeometry leftGeometry = fromText(left.toStringUtf8());
        OGCGeometry rightGeometry = fromText(right.toStringUtf8());
        OperatorSimpleRelation overlapsOperator = OperatorOverlaps.local();
        return overlapsOperator.execute(leftGeometry.getEsriGeometry(), rightGeometry.getEsriGeometry(), leftGeometry.getEsriSpatialReference(), null);
    }

    @SqlNullable
    @Description("Returns true if and only if left geometry overlaps right geometry")
    @ScalarFunction("st_overlaps")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stOverlapsBinary(@SqlType(StandardTypes.VARBINARY) Slice left, @SqlType(StandardTypes.VARBINARY) Slice right)
    {
        OGCGeometry leftGeometry = GeometryUtils.geometryFromBinary(left);
        OGCGeometry rightGeometry = GeometryUtils.geometryFromBinary(right);
        OperatorSimpleRelation overlapsOperator = OperatorOverlaps.local();
        return overlapsOperator.execute(leftGeometry.getEsriGeometry(), rightGeometry.getEsriGeometry(), leftGeometry.getEsriSpatialReference(), null);
    }

    @SqlNullable
    @Description("Returns true if and only if left geometry has the specified DE-9IM relationship with right geometry")
    @ScalarFunction("st_relate")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stRelate(@SqlType(StandardTypes.VARBINARY) Slice left, @SqlType(StandardTypes.VARBINARY) Slice right, @SqlType(StandardTypes.VARCHAR) Slice relation)
    {
        OGCGeometry leftGeometry = GeometryUtils.geometryFromBinary(left);
        OGCGeometry rightGeometry = GeometryUtils.geometryFromBinary(right);
        return leftGeometry.relate(rightGeometry, relation.toStringUtf8());
    }

    @SqlNullable
    @Description("Returns true if and only if left geometry touches right geometry")
    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stTouches(@SqlType(StandardTypes.VARBINARY) Slice left, @SqlType(StandardTypes.VARBINARY) Slice right)
    {
        OGCGeometry leftGeometry = GeometryUtils.geometryFromBinary(left);
        OGCGeometry rightGeometry = GeometryUtils.geometryFromBinary(right);
        OperatorSimpleRelation touchesOperator = OperatorTouches.local();
        return touchesOperator.execute(leftGeometry.getEsriGeometry(), rightGeometry.getEsriGeometry(), leftGeometry.getEsriSpatialReference(), null);
    }

    @SqlNullable
    @Description("Returns true if and only if left geometry is within right geometry")
    @ScalarFunction("st_within")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stWithinVarchar(@SqlType(StandardTypes.VARCHAR) Slice left, @SqlType(StandardTypes.VARCHAR) Slice right)
    {
        OGCGeometry leftGeometry = fromText(left.toStringUtf8());
        OGCGeometry rightGeometry = fromText(right.toStringUtf8());
        OperatorSimpleRelation withinOperator = OperatorWithin.local();
        return withinOperator.execute(leftGeometry.getEsriGeometry(), rightGeometry.getEsriGeometry(), leftGeometry.getEsriSpatialReference(), null);
    }

    @SqlNullable
    @Description("Returns geo hash of a point")
    @ScalarFunction("st_geohash")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice geoHash(@SqlType(StandardTypes.DOUBLE) double lng, @SqlType(StandardTypes.DOUBLE) double lat, @SqlType(StandardTypes.INTEGER) long precision)
    {
        return Slices.utf8Slice(GeoHash.withCharacterPrecision(lat, lng, (int) precision).toBase32());
    }

    @SqlNullable
    @Description("Returns geo hash of a point")
    @ScalarFunction("st_geohash")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice geoHashDefault(@SqlType(StandardTypes.DOUBLE) double lng, @SqlType(StandardTypes.DOUBLE) double lat)
    {
        return geoHash(lng, lat, 12);
    }

    @SqlNullable
    @Description("Returns geo hash of a point")
    @ScalarFunction("st_geohash")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice geoHashFromPoint(@SqlType(StandardTypes.VARBINARY) Slice pointSlice, @SqlType(StandardTypes.BIGINT) long precision)
    {
        OGCGeometry geometry;
        try {
            geometry = GeometryUtils.geometryFromBinary(pointSlice);
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Not able to parse point: " + pointSlice.toStringUtf8(), e);
        }
        if (geometry.geometryType().equals(POINT)) {
            Point point = (Point) geometry.getEsriGeometry();
            return geoHash(point.getX(), point.getY(), precision);
        }
        else {
            throw new IllegalArgumentException("st_geohash only supports st point");
        }
    }

    @SqlNullable
    @Description("Returns geo hash of a point")
    @ScalarFunction("st_geohash")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice geoHashFromPointDefault(@SqlType(StandardTypes.VARBINARY) Slice pointSlice)
    {
        return geoHashFromPoint(pointSlice, 12);
    }

    @SqlNullable
    @Description("Returns the first key whose corresponding geometry contains geo shape")
    @ScalarFunction("geo_contains")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice geoContains(@SqlType(StandardTypes.VARCHAR) Slice shape, @SqlType(StandardTypes.VARCHAR) Slice geoShapesBlock)
    {
        List<String> results = processGeoString(shape, geoShapesBlock, OperatorContains.local(), true);
        if (!results.isEmpty()) {
            return Slices.utf8Slice(results.get(0));
        }
        return null;
    }

    @SqlNullable
    @Description("Returns the first key whose corresponding geometry contains geo shape")
    @ScalarFunction("get_geo_shape")
    @SqlType(StandardTypes.VARBINARY)
    public static Slice getGeoShape(@SqlType(StandardTypes.VARCHAR) Slice shape, @SqlType(StandardTypes.VARCHAR) Slice geoShapesBlock)
    {
        List<OGCGeometry> results = getGeoShapeString(shape, geoShapesBlock, OperatorContains.local(), true);
        if (!results.isEmpty()) {
            return serializeGeometry(results.get(0));
        }
        return null;
    }

    @SqlNullable
    @Description("Returns the first key whose corresponding geometry contains geo shape")
    @ScalarFunction("get_geo_shape")
    @SqlType(StandardTypes.VARBINARY)
    public static Slice getGeoShapeBinary(@SqlType(StandardTypes.VARBINARY) Slice shape, @SqlType(StandardTypes.VARCHAR) Slice geoShapesBlock)
    {
        List<OGCGeometry> results = getGeoShapeBinary(shape, geoShapesBlock, OperatorContains.local(), true);
        if (!results.isEmpty()) {
            return serializeGeometry(results.get(0));
        }
        return null;
    }

    @SqlNullable
    @Description("Returns true if and only if left geometry is within right geometry")
    @ScalarFunction("st_within")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stWithinBinary(@SqlType(StandardTypes.VARBINARY) Slice left, @SqlType(StandardTypes.VARBINARY) Slice right)
    {
        OGCGeometry leftGeometry = GeometryUtils.geometryFromBinary(left);
        OGCGeometry rightGeometry = GeometryUtils.geometryFromBinary(right);
        OperatorSimpleRelation withinOperator = OperatorWithin.local();
        return withinOperator.execute(leftGeometry.getEsriGeometry(), rightGeometry.getEsriGeometry(), leftGeometry.getEsriSpatialReference(), null);
    }

    @SqlNullable
    @Description("Returns the first key whose corresponding geometry contains geo shape")
    @ScalarFunction("geo_contains")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice geoContainsBinary(@SqlType(StandardTypes.VARBINARY) Slice shape, @SqlType(StandardTypes.VARCHAR) Slice geoShapesBlock)
    {
        List<String> results = processGeoBinary(shape, geoShapesBlock, OperatorContains.local(), true);
        if (!results.isEmpty()) {
            return Slices.utf8Slice(results.get(0));
        }
        return null;
    }

    @SqlNullable
    @Description("Returns the first key whose corresponding geometry intersects with geo shape")
    @ScalarFunction("geo_intersects")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice geoIntersects(@SqlType(StandardTypes.VARCHAR) Slice shape, @SqlType(StandardTypes.VARCHAR) Slice geoShapesBlock)
    {
        List<String> results = processGeoString(shape, geoShapesBlock, OperatorIntersects.local(), true);
        if (!results.isEmpty()) {
            return Slices.utf8Slice(results.get(0));
        }
        return null;
    }

    @SqlNullable
    @Description("Returns the first key whose corresponding geometry intersects with geo shape")
    @ScalarFunction("geo_intersects")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice geoIntersectsBinary(@SqlType(StandardTypes.VARBINARY) Slice shape, @SqlType(StandardTypes.VARCHAR) Slice geoShapesBlock)
    {
        List<String> results = processGeoBinary(shape, geoShapesBlock, OperatorIntersects.local(), true);
        if (!results.isEmpty()) {
            return Slices.utf8Slice(results.get(0));
        }
        return null;
    }

    @SqlNullable
    @Description("Returns all keys whose corresponding geometry contains geo shape")
    @ScalarFunction("geo_contains_all")
    @SqlType("array(varchar)")
    public static Block geoContainsAll(@SqlType(StandardTypes.VARCHAR) Slice shape, @SqlType(StandardTypes.VARCHAR) Slice geoShapesBlock)
    {
        List<String> results = processGeoString(shape, geoShapesBlock, OperatorContains.local(), false);
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
    @Description("Returns all keys whose corresponding geometry contains geo shape")
    @ScalarFunction("geo_contains_all")
    @SqlType("array(varchar)")
    public static Block geoContainsAllBinary(@SqlType(StandardTypes.VARBINARY) Slice shape, @SqlType(StandardTypes.VARCHAR) Slice geoShapesBlock)
    {
        List<String> results = processGeoBinary(shape, geoShapesBlock, OperatorContains.local(), false);
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
    public static Block geoIntersectsAll(@SqlType(StandardTypes.VARCHAR) Slice shape, @SqlType(StandardTypes.VARCHAR) Slice geoShapesBlock)
    {
        List<String> results = processGeoString(shape, geoShapesBlock, OperatorIntersects.local(), false);
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
    public static Block geoIntersectsAllBinary(@SqlType(StandardTypes.VARBINARY) Slice shape, @SqlType(StandardTypes.VARCHAR) Slice geoShapesBlock)
    {
        List<String> results = processGeoBinary(shape, geoShapesBlock, OperatorIntersects.local(), false);
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
    @Description("Returns all keys whose corresponding geometry contains geo shape")
    @ScalarFunction("geo_intersects_all_with_shape")
    @SqlType("array(row(varchar,varbinary))")
    public static Block geoIntersectsAllShape(@SqlType(StandardTypes.VARCHAR) Slice shape, @SqlType(StandardTypes.VARCHAR) Slice geoShapesBlock)
    {
        return processGeoOperationWithShape(fromText(shape.toStringUtf8()), geoShapesBlock, OperatorIntersects.local());
    }

    @SqlNullable
    @Description("Returns all keys whose corresponding geometry contains geo shape")
    @ScalarFunction("geo_intersects_all_with_shape")
    @SqlType("array(row(varchar,varbinary))")
    public static Block geoIntersectsAllShapeBinary(@SqlType(StandardTypes.VARBINARY) Slice shape, @SqlType(StandardTypes.VARCHAR) Slice geoShapesBlock)
    {
        return processGeoOperationWithShape(geometryFromBinary(shape), geoShapesBlock, OperatorIntersects.local());
    }

    @SqlNullable
    @Description("Returns all keys whose corresponding geometry contains geo shape")
    @ScalarFunction("geo_contains_all_with_shape")
    @SqlType("array(row(varchar,varbinary))")
    public static Block geoContainsAllShape(@SqlType(StandardTypes.VARCHAR) Slice shape, @SqlType(StandardTypes.VARCHAR) Slice geoShapesBlock)
    {
        return processGeoOperationWithShape(fromText(shape.toStringUtf8()), geoShapesBlock, OperatorContains.local());
    }

    @SqlNullable
    @Description("Returns all keys whose corresponding geometry contains geo shape")
    @ScalarFunction("geo_contains_all_with_shape")
    @SqlType("array(row(varchar,varbinary))")
    public static Block geoContainsAllShapeBinary(@SqlType(StandardTypes.VARBINARY) Slice shape, @SqlType(StandardTypes.VARCHAR) Slice geoShapesBlock)
    {
        return processGeoOperationWithShape(geometryFromBinary(shape), geoShapesBlock, OperatorContains.local());
    }

    private static Block processGeoOperationWithShape(OGCGeometry shape, Slice geoShapesBlock, OperatorSimpleRelation relationOperator)
    {
        List<GeoItem> geoItems = queryGeoItem(shape, getGeoIndex(geoShapesBlock), relationOperator, false);
        RowType rowType = RowType.anonymous(ImmutableList.of(VarcharType.VARCHAR, VarbinaryType.VARBINARY));
        if (!geoItems.isEmpty()) {
            BlockBuilder outputBuilder = rowType.createBlockBuilder(null, geoItems.size());
            for (GeoItem geoItem : geoItems) {
                BlockBuilder rowBuilder = outputBuilder.beginBlockEntry();
                VarcharType.VARCHAR.writeString(rowBuilder, geoItem.getIndexField());
                VarbinaryType.VARBINARY.writeSlice(rowBuilder, GeometryUtils.serializeGeometry(geoItem.getGeometry()));
                outputBuilder.closeEntry();
            }
            return outputBuilder.build();
        }
        return null;
    }

    private static List<OGCGeometry> getGeoShapeString(Slice shape, Slice geoShapesBlock, OperatorSimpleRelation relationOperator, boolean getFirst)
    {
        return getGeoShape(fromText(shape.toStringUtf8()), geoShapesBlock, relationOperator, getFirst);
    }

    private static List<OGCGeometry> getGeoShapeBinary(Slice shape, Slice geoShapesBlock, OperatorSimpleRelation relationOperator, boolean getFirst)
    {
        return getGeoShape(geometryFromBinary(shape), geoShapesBlock, relationOperator, getFirst);
    }

    private static List<String> processGeoString(Slice shape, Slice geoShapesBlock, OperatorSimpleRelation relationOperator, boolean getFirst)
    {
        return getGeoIndexValue(fromText(shape.toStringUtf8()), geoShapesBlock, relationOperator, getFirst);
    }

    private static List<String> processGeoBinary(Slice shape, Slice geoShapesBlock, OperatorSimpleRelation relationOperator, boolean getFirst)
    {
        return getGeoIndexValue(geometryFromBinary(shape), geoShapesBlock, relationOperator, getFirst);
    }

    private static List<OGCGeometry> getGeoShape(OGCGeometry shape, Slice geoShapesBlock, OperatorSimpleRelation relationOperator, boolean getFirst)
    {
        return queryGeometry(shape, getGeoIndex(geoShapesBlock), relationOperator, getFirst);
    }

    private static List<String> getGeoIndexValue(OGCGeometry shape, Slice geoShapesBlock, OperatorSimpleRelation relationOperator, boolean getFirst)
    {
        return queryIndexValue(shape, getGeoIndex(geoShapesBlock), relationOperator, getFirst);
    }

    private static GeoIndex getGeoIndex(Slice slice)
    {
        HashCode hashCode = HashCode.fromInt(slice.getInt(0));
        try {
            return QuardTreeCache.get(hashCode, () -> createGeoIndex(slice));
        }
        catch (ExecutionException e) {
            throw new PrestoException(GEOSPATIAL_CREATE_INDEX_ERROR, "not able to create geo index", e);
        }
    }

    private static GeoIndex createGeoIndex(Slice slice)
    {
        int geoShapeDataSize = slice.getInt(SIZE_OF_INT);
        byte[] bytes = slice.getBytes(SIZE_OF_INT + SIZE_OF_INT, geoShapeDataSize);
        List<GeoItem> geoShapes = GeoShape.deserialize(Slices.wrappedBuffer(bytes)).stream().map(geoShape -> new
                GeoItem(geoShape.id.toStringUtf8(), GeometryUtils.geometryFromBinary(geoShape.geoShape))).collect
                (Collectors.toList());
        return QuadTreeUtils.init(geoShapes);
    }
}
