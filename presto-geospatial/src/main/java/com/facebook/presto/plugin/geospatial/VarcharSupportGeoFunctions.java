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

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;

import static com.facebook.presto.plugin.geospatial.GeoFunctions.stArea;
import static com.facebook.presto.plugin.geospatial.GeoFunctions.stAsText;
import static com.facebook.presto.plugin.geospatial.GeoFunctions.stBuffer;
import static com.facebook.presto.plugin.geospatial.GeoFunctions.stGeometryFromText;
import static com.facebook.presto.plugin.geospatial.GeometryType.GEOMETRY_TYPE_NAME;
import static com.facebook.presto.spi.type.StandardTypes.DOUBLE;
import static com.facebook.presto.spi.type.StandardTypes.INTEGER;
import static com.facebook.presto.spi.type.StandardTypes.VARCHAR;

// VarcharSupportGeoFunctions add varchar parameter support for all GeoFunctions
public final class VarcharSupportGeoFunctions
{
    private VarcharSupportGeoFunctions() {}

    @ScalarFunction("ST_Area")
    @SqlType(DOUBLE)
    public static double stAreaText(@SqlType(VARCHAR) Slice input)
    {
        return stArea(stGeometryFromText(input));
    }

    @Description("Returns binary of geometry")
    @ScalarFunction("st_geometry_from_text")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stGeometryFromTextLagacyName(@SqlType(VARCHAR) Slice input)
    {
        return stGeometryFromText(input);
    }

    @Description("Returns text of geometry")
    @ScalarFunction("st_geometry_to_text")
    @SqlType(VARCHAR)
    public static Slice stGeometryToText(@SqlType(StandardTypes.VARBINARY) Slice input)
    {
        return stAsText(input);
    }

    @ScalarFunction("ST_Buffer")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stBufferVarchar(@SqlType(VARCHAR) Slice input, @SqlType(DOUBLE) double distance)
    {
        return stBuffer(stGeometryFromText(input), distance);
    }

    @ScalarFunction("ST_Centroid")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stCentroid(@SqlType(VARCHAR) Slice input)
    {
        return GeoFunctions.stCentroid(stGeometryFromText(input));
    }

    @ScalarFunction("ST_ConvexHull")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stConvexHull(@SqlType(VARCHAR) Slice input)
    {
        return GeoFunctions.stConvexHull(stGeometryFromText(input));
    }

    @ScalarFunction("ST_CoordDim")
    @SqlType(StandardTypes.TINYINT)
    public static long stCoordinateDimension(@SqlType(VARCHAR) Slice input)
    {
        return GeoFunctions.stCoordinateDimension(stGeometryFromText(input));
    }

    @ScalarFunction("ST_Dimension")
    @SqlType(StandardTypes.TINYINT)
    public static long stDimension(@SqlType(VARCHAR) Slice input)
    {
        return GeoFunctions.stDimension(stGeometryFromText(input));
    }

    @SqlNullable
    @ScalarFunction("ST_IsClosed")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stIsClosed(@SqlType(VARCHAR) Slice input)
    {
        return GeoFunctions.stIsClosed(stGeometryFromText(input));
    }

    @SqlNullable
    @Description("Returns TRUE if this Geometry is an empty geometrycollection, polygon, point etc")
    @ScalarFunction("ST_IsEmpty")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stIsEmpty(@SqlType(VARCHAR) Slice input)
    {
        return GeoFunctions.stIsEmpty(stGeometryFromText(input));
    }

    @Description("Returns TRUE if this Geometry has no anomalous geometric points, such as self intersection or self tangency")
    @ScalarFunction("ST_IsSimple")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean stIsSimple(@SqlType(VARCHAR) Slice input)
    {
        return GeoFunctions.stIsSimple(stGeometryFromText(input));
    }

    @Description("Returns true if the input geometry is well formed")
    @ScalarFunction("ST_IsValid")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean stIsValid(@SqlType(VARCHAR) Slice input)
    {
        return GeoFunctions.stIsValid(stGeometryFromText(input));
    }

    @Description("Returns the reason for why the input geometry is not valid. Returns null if the input is valid.")
    @ScalarFunction("geometry_invalid_reason")
    @SqlType(StandardTypes.VARCHAR)
    @SqlNullable
    public static Slice invalidReason(@SqlType(VARCHAR) Slice input)
    {
        return GeoFunctions.invalidReason(stGeometryFromText(input));
    }

    @Description("Returns the length of a LineString or Multi-LineString using Euclidean measurement on a 2D plane (based on spatial ref) in projected units")
    @ScalarFunction("ST_Length")
    @SqlType(DOUBLE)
    public static double stLength(@SqlType(VARCHAR) Slice input)
    {
        return GeoFunctions.stLength(stGeometryFromText(input));
    }

    @SqlNullable
    @Description("Returns a float between 0 and 1 representing the location of the closest point on the LineString to the given Point, as a fraction of total 2d line length.")
    @ScalarFunction("line_locate_point")
    @SqlType(DOUBLE)
    public static Double lineLocatePoint(@SqlType(VARCHAR) Slice lineSlice, @SqlType(VARCHAR) Slice pointSlice)
    {
        return GeoFunctions.lineLocatePoint(stGeometryFromText(lineSlice), stGeometryFromText(pointSlice));
    }

    @SqlNullable
    @Description("Returns X maxima of a bounding box of a Geometry")
    @ScalarFunction("ST_XMax")
    @SqlType(DOUBLE)
    public static Double stXMax(@SqlType(VARCHAR) Slice input)
    {
        return GeoFunctions.stXMax(stGeometryFromText(input));
    }

    @SqlNullable
    @Description("Returns Y maxima of a bounding box of a Geometry")
    @ScalarFunction("ST_YMax")
    @SqlType(DOUBLE)
    public static Double stYMax(@SqlType(VARCHAR) Slice input)
    {
        return GeoFunctions.stYMax(stGeometryFromText(input));
    }

    @SqlNullable
    @Description("Returns X minima of a bounding box of a Geometry")
    @ScalarFunction("ST_XMin")
    @SqlType(DOUBLE)
    public static Double stXMin(@SqlType(VARCHAR) Slice input)
    {
        return GeoFunctions.stXMin(stGeometryFromText(input));
    }

    @SqlNullable
    @Description("Returns Y minima of a bounding box of a Geometry")
    @ScalarFunction("ST_YMin")
    @SqlType(DOUBLE)
    public static Double stYMin(@SqlType(VARCHAR) Slice input)
    {
        return GeoFunctions.stYMin(stGeometryFromText(input));
    }

    @SqlNullable
    @Description("Returns the cardinality of the collection of interior rings of a polygon")
    @ScalarFunction("ST_NumInteriorRing")
    @SqlType(StandardTypes.BIGINT)
    public static Long stNumInteriorRings(@SqlType(VARCHAR) Slice input)
    {
        return GeoFunctions.stNumInteriorRings(stGeometryFromText(input));
    }

    @Description("Returns the cardinality of the geometry collection")
    @ScalarFunction("ST_NumGeometries")
    @SqlType(INTEGER)
    public static long stNumGeometries(@SqlType(VARCHAR) Slice input)
    {
        return GeoFunctions.stNumGeometries(stGeometryFromText(input));
    }

    @SqlNullable
    @Description("Returns the geometry element at the specified index (indices started with 1)")
    @ScalarFunction("ST_GeometryN")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stGeometryN(@SqlType(VARCHAR) Slice input, @SqlType(INTEGER) long index)
    {
        return GeoFunctions.stGeometryN(stGeometryFromText(input), index);
    }

    @Description("Returns the number of points in a Geometry")
    @ScalarFunction("ST_NumPoints")
    @SqlType(StandardTypes.BIGINT)
    public static long stNumPoints(@SqlType(VARCHAR) Slice input)
    {
        return GeoFunctions.stNumPoints(stGeometryFromText(input));
    }

    @SqlNullable
    @Description("Returns TRUE if and only if the line is closed and simple")
    @ScalarFunction("ST_IsRing")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stIsRing(@SqlType(VARCHAR) Slice input)
    {
        return GeoFunctions.stIsRing(stGeometryFromText(input));
    }

    @SqlNullable
    @Description("Returns the first point of a LINESTRING geometry as a Point")
    @ScalarFunction("ST_StartPoint")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stStartPoint(@SqlType(VARCHAR) Slice input)
    {
        return GeoFunctions.stStartPoint(stGeometryFromText(input));
    }

    @Description("Returns a \"simplified\" version of the given geometry")
    @ScalarFunction("simplify_geometry")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice simplifyGeometry(@SqlType(VARCHAR) Slice input,
            @SqlType(DOUBLE) double distanceTolerance)
    {
        return GeoFunctions.simplifyGeometry(stGeometryFromText(input), distanceTolerance);
    }

    @SqlNullable
    @Description("Returns the last point of a LINESTRING geometry as a Point")
    @ScalarFunction("ST_EndPoint")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stEndPoint(@SqlType(VARCHAR) Slice input)
    {
        return GeoFunctions.stEndPoint(stGeometryFromText(input));
    }

    @SqlNullable
    @Description("Return the X coordinate of the point")
    @ScalarFunction("ST_X")
    @SqlType(DOUBLE)
    public static Double stX(@SqlType(VARCHAR) Slice input)
    {
        return GeoFunctions.stX(stGeometryFromText(input));
    }

    @SqlNullable
    @Description("Return the Y coordinate of the point")
    @ScalarFunction("ST_Y")
    @SqlType(DOUBLE)
    public static Double stY(@SqlType(VARCHAR) Slice input)
    {
        return GeoFunctions.stY(stGeometryFromText(input));
    }

    @Description("Returns the closure of the combinatorial boundary of this Geometry")
    @ScalarFunction("ST_Boundary")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stBoundary(@SqlType(VARCHAR) Slice input)
    {
        return GeoFunctions.stBoundary(stGeometryFromText(input));
    }

    @Description("Returns the bounding rectangular polygon of a Geometry")
    @ScalarFunction("ST_Envelope")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stEnvelope(@SqlType(VARCHAR) Slice input)
    {
        return GeoFunctions.stEnvelope(stGeometryFromText(input));
    }

    @Description("Returns the Geometry value that represents the point set difference of two geometries")
    @ScalarFunction("ST_Difference")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stDifference(@SqlType(VARCHAR) Slice left, @SqlType(VARCHAR) Slice right)
    {
        return GeoFunctions.stDifference(stGeometryFromText(left), stGeometryFromText(right));
    }

    @Description("Returns the 2-dimensional cartesian minimum distance (based on spatial ref) between two geometries in projected units")
    @ScalarFunction("ST_Distance")
    @SqlType(DOUBLE)
    public static double stDistance(@SqlType(VARCHAR) Slice left, @SqlType(VARCHAR) Slice right)
    {
        return GeoFunctions.stDistance(stGeometryFromText(left), stGeometryFromText(right));
    }

    @SqlNullable
    @Description("Returns a line string representing the exterior ring of the POLYGON")
    @ScalarFunction("ST_ExteriorRing")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stExteriorRing(@SqlType(VARCHAR) Slice input)
    {
        return GeoFunctions.stExteriorRing(stGeometryFromText(input));
    }

    @Description("Returns the Geometry value that represents the point set intersection of two Geometries")
    @ScalarFunction("ST_Intersection")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stIntersection(@SqlType(VARCHAR) Slice left, @SqlType(VARCHAR) Slice right)
    {
        return GeoFunctions.stIntersection(stGeometryFromText(left), stGeometryFromText(right));
    }

    @Description("Returns the Geometry value that represents the point set symmetric difference of two Geometries")
    @ScalarFunction("ST_SymDifference")
    @SqlType(GEOMETRY_TYPE_NAME)
    public static Slice stSymmetricDifference(@SqlType(VARCHAR) Slice left, @SqlType(VARCHAR) Slice right)
    {
        return GeoFunctions.stSymmetricDifference(stGeometryFromText(left), stGeometryFromText(right));
    }

    @SqlNullable
    @Description("Returns TRUE if and only if no points of right lie in the exterior of left, and at least one point of the interior of left lies in the interior of right")
    @ScalarFunction("ST_Contains")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stContains(@SqlType(VARCHAR) Slice left, @SqlType(VARCHAR) Slice right)
    {
        return GeoFunctions.stContains(stGeometryFromText(left), stGeometryFromText(right));
    }

    @SqlNullable
    @Description("Returns TRUE if the supplied geometries have some, but not all, interior points in common")
    @ScalarFunction("ST_Crosses")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stCrosses(@SqlType(VARCHAR) Slice left, @SqlType(VARCHAR) Slice right)
    {
        return GeoFunctions.stCrosses(stGeometryFromText(left), stGeometryFromText(right));
    }

    @SqlNullable
    @Description("Returns TRUE if the Geometries do not spatially intersect - if they do not share any space together")
    @ScalarFunction("ST_Disjoint")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stDisjoint(@SqlType(VARCHAR) Slice left, @SqlType(VARCHAR) Slice right)
    {
        return GeoFunctions.stDisjoint(stGeometryFromText(left), stGeometryFromText(right));
    }

    @SqlNullable
    @Description("Returns TRUE if the given geometries represent the same geometry")
    @ScalarFunction("ST_Equals")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stEquals(@SqlType(VARCHAR) Slice left, @SqlType(VARCHAR) Slice right)
    {
        return GeoFunctions.stEquals(stGeometryFromText(left), stGeometryFromText(right));
    }

    @SqlNullable
    @Description("Returns TRUE if the Geometries spatially intersect in 2D - (share any portion of space) and FALSE if they don't (they are Disjoint)")
    @ScalarFunction("ST_Intersects")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stIntersects(@SqlType(VARCHAR) Slice left, @SqlType(VARCHAR) Slice right)
    {
        return GeoFunctions.stIntersects(stGeometryFromText(left), stGeometryFromText(right));
    }

    @SqlNullable
    @Description("Returns TRUE if the Geometries share space, are of the same dimension, but are not completely contained by each other")
    @ScalarFunction("ST_Overlaps")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stOverlaps(@SqlType(VARCHAR) Slice left, @SqlType(VARCHAR) Slice right)
    {
        return GeoFunctions.stOverlaps(stGeometryFromText(left), stGeometryFromText(right));
    }

    @SqlNullable
    @Description("Returns TRUE if this Geometry is spatially related to another Geometry")
    @ScalarFunction("ST_Relate")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stRelate(@SqlType(VARCHAR) Slice left, @SqlType(VARCHAR) Slice right, @SqlType(StandardTypes.VARCHAR) Slice relation)
    {
        return GeoFunctions.stRelate(stGeometryFromText(left), stGeometryFromText(right), relation);
    }

    @SqlNullable
    @Description("Returns TRUE if the geometries have at least one point in common, but their interiors do not intersect")
    @ScalarFunction("ST_Touches")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stTouches(@SqlType(VARCHAR) Slice left, @SqlType(VARCHAR) Slice right)
    {
        return GeoFunctions.stTouches(stGeometryFromText(left), stGeometryFromText(right));
    }

    @SqlNullable
    @Description("Returns TRUE if the geometry A is completely inside geometry B")
    @ScalarFunction("ST_Within")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean stWithin(@SqlType(VARCHAR) Slice left, @SqlType(VARCHAR) Slice right)
    {
        return GeoFunctions.stWithin(stGeometryFromText(left), stGeometryFromText(right));
    }

    @Description("Returns the type of the geometry")
    @ScalarFunction("ST_GeometryType")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice stGeometryType(@SqlType(VARCHAR) Slice input)
    {
        return GeoFunctions.stGeometryType(stGeometryFromText(input));
    }
}
