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

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.spi.type.Type;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.metadata.FunctionExtractor.extractFunctions;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;

// All Tests aims to test varchar type support, not for function correctness
public class TestVarcharSupportGeoFunctions
        extends AbstractTestFunctions
{
    @BeforeClass
    protected void registerFunctions()
    {
        GeoPlugin plugin = new GeoPlugin();
        for (Type type : plugin.getTypes()) {
            functionAssertions.getTypeRegistry().addType(type);
        }
        functionAssertions.getMetadata().addFunctions(extractFunctions(plugin.getFunctions()));
    }

    @Test
    public void testCast()
    {
        assertFunction("ST_AsText(CAST(st_geometry_to_varbinary('POINT (1 4)') as GEOMETRY))", VARCHAR, "POINT (1 4)");
    }

    @Test
    public void testSTArea()
    {
        assertFunction("ST_Area(('POLYGON EMPTY'))", DOUBLE, 0.0);
        assertInvalidFunction("ST_Area(('POINT (1 4)'))", "ST_Area only applies to POLYGON or MULTI_POLYGON. Input type is: POINT");
    }

    @Test
    public void testSTBuffer()
    {
        assertFunction("ST_AsText(ST_Buffer('LINESTRING (0 0, 1 1, 2 0.5)', 0.2))", VARCHAR, "POLYGON ((0 -0.19999999999999996, 0.013080625846028537 -0.19957178464772052, 0.02610523844401036 -0.19828897227476194, 0.03901806440322564 -0.19615705608064593, 0.05176380902050415 -0.1931851652578136, 0.06428789306063232 -0.18938602589902098, 0.07653668647301792 -0.18477590650225728, 0.0884577380438003 -0.17937454830653754, 0.09999999999999987 -0.17320508075688767, 0.11111404660392044 -0.166293922460509, 0.12175228580174413 -0.15867066805824703, 0.13186916302001372 -0.15036796149579545, 0.14142135623730945 -0.14142135623730945, 1.0394906098164265 0.7566478973418078, 1.9105572809000084 0.32111456180001685, 1.9115422619561997 0.32062545169346235, 1.923463313526982 0.31522409349774266, 1.9357121069393677 0.3106139741009789, 1.9482361909794959 0.3068148347421863, 1.9609819355967744 0.3038429439193539, 1.9738947615559896 0.30171102772523795, 1.9869193741539715 0.30042821535227926, 2 0.3, 2.0130806258460288 0.3004282153522794, 2.02610523844401 0.30171102772523806, 2.0390180644032254 0.30384294391935407, 2.051763809020504 0.30681483474218646, 2.0642878930606323 0.31061397410097896, 2.076536686473018 0.3152240934977427, 2.0884577380438003 0.32062545169346246, 2.1 0.3267949192431123, 2.1111140466039204 0.333706077539491, 2.121752285801744 0.34132933194175297, 2.1318691630200135 0.34963203850420455, 2.1414213562373092 0.35857864376269055, 2.1503679614957956 0.3681308369799863, 2.158670668058247 0.37824771419825587, 2.166293922460509 0.38888595339607956, 2.1732050807568877 0.4, 2.1793745483065377 0.41154226195619975, 2.1847759065022574 0.4234633135269821, 2.189386025899021 0.4357121069393677, 2.193185165257814 0.44823619097949585, 2.1961570560806463 0.46098193559677436, 2.1982889722747623 0.4738947615559897, 2.1995717846477207 0.4869193741539714, 2.2 0.5, 2.1995717846477207 0.5130806258460285, 2.198288972274762 0.5261052384440102, 2.196157056080646 0.5390180644032256, 2.1931851652578134 0.5517638090205041, 2.189386025899021 0.5642878930606323, 2.1847759065022574 0.5765366864730179, 2.1793745483065377 0.5884577380438002, 2.1732050807568877 0.5999999999999999, 2.166293922460509 0.6111140466039204, 2.158670668058247 0.6217522858017441, 2.1503679614957956 0.6318691630200137, 2.1414213562373097 0.6414213562373094, 2.131869163020014 0.6503679614957955, 2.121752285801744 0.658670668058247, 2.1111140466039204 0.666293922460509, 2.1 0.6732050807568877, 2.0894427190999916 0.6788854381999831, 1.0894427190999916 1.1788854381999831, 1.0884577380438003 1.1793745483065377, 1.076536686473018 1.1847759065022574, 1.0642878930606323 1.189386025899021, 1.0517638090205041 1.1931851652578138, 1.0390180644032256 1.196157056080646, 1.0261052384440104 1.198288972274762, 1.0130806258460288 1.1995717846477207, 1 1.2, 0.9869193741539715 1.1995717846477205, 0.9738947615559896 1.1982889722747618, 0.9609819355967744 1.1961570560806458, 0.9482361909794959 1.1931851652578136, 0.9357121069393677 1.189386025899021, 0.9234633135269821 1.1847759065022574, 0.9115422619561997 1.1793745483065377, 0.9000000000000001 1.1732050807568877, 0.8888859533960796 1.166293922460509, 0.8782477141982559 1.158670668058247, 0.8681308369799863 1.1503679614957956, 0.8585786437626906 1.1414213562373094, -0.14142135623730967 0.1414213562373095, -0.15036796149579557 0.13186916302001372, -0.1586706680582468 0.12175228580174413, -0.1662939224605089 0.11111404660392044, -0.17320508075688767 0.09999999999999998, -0.17937454830653765 0.08845773804380025, -0.1847759065022574 0.07653668647301792, -0.18938602589902098 0.06428789306063232, -0.19318516525781382 0.05176380902050415, -0.19615705608064626 0.03901806440322564, -0.19828897227476228 0.026105238444010304, -0.19957178464772074 0.013080625846028593, -0.20000000000000018 0, -0.19957178464772074 -0.013080625846028537, -0.19828897227476183 -0.026105238444010248, -0.19615705608064582 -0.03901806440322564, -0.19318516525781337 -0.05176380902050415, -0.18938602589902098 -0.06428789306063232, -0.1847759065022574 -0.07653668647301792, -0.17937454830653765 -0.0884577380438002, -0.17320508075688767 -0.09999999999999987, -0.1662939224605089 -0.11111404660392044, -0.1586706680582468 -0.12175228580174413, -0.15036796149579557 -0.13186916302001372, -0.14142135623730967 -0.14142135623730945, -0.13186916302001395 -0.15036796149579545, -0.12175228580174391 -0.15867066805824703, -0.11111404660392044 -0.166293922460509, -0.10000000000000009 -0.17320508075688767, -0.0884577380438003 -0.17937454830653765, -0.07653668647301792 -0.1847759065022574, -0.06428789306063232 -0.1893860258990211, -0.05176380902050415 -0.1931851652578137, -0.03901806440322586 -0.19615705608064604, -0.026105238444010137 -0.19828897227476205, -0.01308062584602876 -0.19957178464772074, 0 -0.19999999999999996))");
        assertFunction("ST_AsText(ST_Buffer('POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0))', 1.2))", VARCHAR, "POLYGON ((-1.2 0, -1.1974307078863233 -0.0784837550761715, -1.1897338336485717 -0.15663143066406168, -1.1769423364838756 -0.23410838641935366, -1.1591109915468811 -0.3105828541230246, -1.1363161553941261 -0.38572735836379357, -1.1086554390135435 -0.4592201188381073, -1.0762472898392252 -0.530746428262801, -1.0392304845413258 -0.5999999999999995, -0.9977635347630538 -0.6666842796235222, -0.9520240083494819 -0.7305137148104643, -0.9022077689747725 -0.7912149781200822, -0.8485281374238568 -0.8485281374238567, -0.7912149781200825 -0.9022077689747725, -0.7305137148104647 -0.9520240083494819, -0.6666842796235226 -0.997763534763054, -0.5999999999999999 -1.039230484541326, -0.5307464282628015 -1.0762472898392257, -0.45922011883810765 -1.108655439013544, -0.38572735836379385 -1.1363161553941266, -0.3105828541230249 -1.159110991546882, -0.2341083864193539 -1.1769423364838765, -0.15663143066406188 -1.1897338336485723, -0.07848375507617167 -1.1974307078863242, 0 -1.2, 5 -1.2, 5.078483755076172 -1.1974307078863233, 5.156631430664062 -1.1897338336485717, 5.234108386419353 -1.1769423364838756, 5.310582854123025 -1.1591109915468811, 5.385727358363794 -1.1363161553941261, 5.4592201188381075 -1.1086554390135435, 5.530746428262801 -1.0762472898392252, 5.6 -1.0392304845413258, 5.666684279623523 -0.9977635347630538, 5.730513714810464 -0.9520240083494819, 5.791214978120082 -0.9022077689747725, 5.848528137423857 -0.8485281374238568, 5.9022077689747725 -0.7912149781200825, 5.952024008349482 -0.7305137148104647, 5.997763534763054 -0.6666842796235226, 6.039230484541326 -0.5999999999999999, 6.076247289839226 -0.5307464282628015, 6.108655439013544 -0.45922011883810765, 6.136316155394127 -0.38572735836379385, 6.159110991546882 -0.3105828541230249, 6.176942336483877 -0.2341083864193539, 6.189733833648573 -0.15663143066406188, 6.197430707886324 -0.07848375507617167, 6.2 0, 6.2 5, 6.1974307078863236 5.078483755076172, 6.189733833648572 5.156631430664062, 6.176942336483876 5.234108386419353, 6.159110991546881 5.310582854123025, 6.136316155394126 5.385727358363794, 6.1086554390135435 5.4592201188381075, 6.076247289839225 5.530746428262801, 6.039230484541326 5.6, 5.997763534763054 5.666684279623523, 5.952024008349482 5.730513714810464, 5.9022077689747725 5.791214978120082, 5.848528137423857 5.848528137423857, 5.791214978120083 5.9022077689747725, 5.730513714810464 5.952024008349482, 5.666684279623523 5.997763534763054, 5.6 6.039230484541326, 5.530746428262802 6.076247289839226, 5.4592201188381075 6.108655439013544, 5.385727358363794 6.136316155394127, 5.310582854123025 6.159110991546882, 5.234108386419354 6.176942336483877, 5.156631430664062 6.189733833648573, 5.078483755076172 6.197430707886324, 5 6.2, 0 6.2, -0.0784837550761715 6.1974307078863236, -0.15663143066406168 6.189733833648572, -0.23410838641935366 6.176942336483876, -0.3105828541230246 6.159110991546881, -0.38572735836379357 6.136316155394126, -0.4592201188381073 6.1086554390135435, -0.530746428262801 6.076247289839225, -0.5999999999999995 6.039230484541326, -0.6666842796235222 5.997763534763054, -0.7305137148104643 5.952024008349482, -0.7912149781200822 5.9022077689747725, -0.8485281374238567 5.848528137423857, -0.9022077689747725 5.791214978120083, -0.9520240083494819 5.730513714810464, -0.997763534763054 5.666684279623523, -1.039230484541326 5.6, -1.0762472898392257 5.530746428262802, -1.108655439013544 5.4592201188381075, -1.1363161553941266 5.385727358363794, -1.159110991546882 5.310582854123025, -1.1769423364838765 5.234108386419354, -1.1897338336485723 5.156631430664062, -1.1974307078863242 5.078483755076172, -1.2 5, -1.2 0))");
    }

    @Test
    public void testSTCentroid()
    {
        assertFunction("ST_AsText(ST_Centroid(('LINESTRING EMPTY')))", VARCHAR, "POINT EMPTY");
        assertFunction("ST_AsText(ST_Centroid(('POINT (3 5)')))", VARCHAR, "POINT (3 5)");
        assertFunction("ST_AsText(ST_Centroid(('MULTIPOINT (1 2, 2 4, 3 6, 4 8)')))", VARCHAR, "POINT (2.5 5)");
    }

    @Test
    public void testSTConvexHull()
    {
        assertConvexHull("POINT EMPTY", "POINT EMPTY");
        assertConvexHull("MULTIPOINT EMPTY", "MULTIPOINT EMPTY");
        assertConvexHull("LINESTRING (1 1, 1 9, 2 2, 1 1)", "POLYGON ((1 1, 2 2, 1 9, 1 1))");
        assertConvexHull("POLYGON ((0 0, 0 3, 2 4, 4 2, 3 0, 0 0))", "POLYGON ((0 0, 3 0, 4 2, 2 4, 0 3, 0 0))");
    }

    private void assertConvexHull(String inputWKT, String expectWKT)
    {
        assertFunction(format("ST_AsText(ST_ConvexHull(('%s')))", inputWKT), VARCHAR, expectWKT);
    }

    @Test
    public void testSTCoordDim()
    {
        assertFunction("ST_CoordDim(('POLYGON ((1 1, 1 4, 4 4, 4 1))'))", TINYINT, (byte) 2);
    }

    @Test
    public void testSTDimension()
    {
        assertFunction("ST_Dimension(('POLYGON EMPTY'))", TINYINT, (byte) 2);
    }

    @Test
    public void testSTIsClosed()
    {
        assertFunction("ST_IsClosed(('LINESTRING (1 1, 2 2, 1 3, 1 1)'))", BOOLEAN, true);
    }

    @Test
    public void testSTIsEmpty()
    {
        assertFunction("ST_IsEmpty(('POINT (1.5 2.5)'))", BOOLEAN, false);
    }

    private void assertSimpleGeometry(String text)
    {
        assertFunction("ST_IsSimple(('" + text + "'))", BOOLEAN, true);
    }

    private void assertNotSimpleGeometry(String text)
    {
        assertFunction("ST_IsSimple(('" + text + "'))", BOOLEAN, false);
    }

    @Test
    public void testSTIsSimple()
    {
        assertSimpleGeometry("POINT (1.5 2.5)");
        assertNotSimpleGeometry("MULTIPOINT (1 2, 2 4, 3 6, 1 2)");
    }

    @Test
    public void testSimplifyGeometry()
    {
        // Eliminate unnecessary points on the same line.
        assertFunction("ST_AsText(simplify_geometry(('POLYGON ((1 0, 2 1, 3 1, 3 1, 4 1, 1 0))'), 1.5))", VARCHAR, "POLYGON ((1 0, 4 1, 2 1, 1 0))");
    }

    @Test
    public void testSTIsValid()
    {
        // empty geometries are valid
        assertValidGeometry("POINT EMPTY");
        assertValidGeometry("POINT (1 2)");
        assertFunction("ST_IsValid((null))", BOOLEAN, null);
        assertFunction("geometry_invalid_reason((null))", VARCHAR, null);
    }

    private void assertValidGeometry(String wkt)
    {
        assertFunction("ST_IsValid(('" + wkt + "'))", BOOLEAN, true);
        assertFunction("geometry_invalid_reason(('" + wkt + "'))", VARCHAR, null);
    }

    @Test
    public void testSTLength()
    {
        assertFunction("ST_Length(('LINESTRING EMPTY'))", DOUBLE, 0.0);
    }

    @Test
    public void testLineLocatePoint()
    {
        assertFunction("line_locate_point(('LINESTRING (0 0, 0 1)'), 'Point(0 0.2)')", DOUBLE, 0.2);
    }

    @Test
    public void testSTMax()
    {
        assertFunction("ST_XMax(('POINT (1.5 2.5)'))", DOUBLE, 1.5);
    }

    @Test
    public void testSTMin()
    {
        assertFunction("ST_XMin(('POINT (1.5 2.5)'))", DOUBLE, 1.5);
    }

    @Test
    public void testSTNumInteriorRing()
    {
        assertFunction("ST_NumInteriorRing(('POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0))'))", BIGINT, 0L);
    }

    @Test
    public void testSTNumPoints()
    {
        assertNumPoints("POINT EMPTY", 0);
        assertNumPoints("MULTIPOINT (1 2, 2 4, 3 6, 4 8)", 4);
        assertNumPoints("LINESTRING (8 4, 5 7)", 2);
    }

    private void assertNumPoints(String wkt, int expectedPoints)
    {
        assertFunction(format("ST_NumPoints(('%s'))", wkt), BIGINT, (long) expectedPoints);
    }

    @Test
    public void testSTIsRing()
    {
        assertFunction("ST_IsRing(('LINESTRING (8 4, 4 8)'))", BOOLEAN, false);
    }

    @Test
    public void testSTStartEndPoint()
    {
        assertFunction("ST_AsText(ST_StartPoint(('LINESTRING (8 4, 4 8, 5 6)')))", VARCHAR, "POINT (8 4)");
    }

    @Test
    public void testSTXY()
    {
        assertFunction("ST_Y(('POINT EMPTY'))", DOUBLE, null);
        assertFunction("ST_X(('POINT (1 2)'))", DOUBLE, 1.0);
    }

    @Test
    public void testSTBoundary()
    {
        assertFunction("ST_AsText(ST_Boundary(('POINT (1 2)')))", VARCHAR, "MULTIPOINT EMPTY");
        assertFunction("ST_AsText(ST_Boundary(('MULTIPOINT (1 2, 2 4, 3 6, 4 8)')))", VARCHAR, "MULTIPOINT EMPTY");
    }

    @Test
    public void testSTEnvelope()
    {
        assertFunction("ST_AsText(ST_Envelope(('MULTIPOINT (1 2, 2 4, 3 6, 4 8)')))", VARCHAR, "POLYGON ((1 2, 4 2, 4 8, 1 8, 1 2))");
    }

    @Test
    public void testSTDifference()
    {
        assertFunction("ST_AsText(ST_Difference(('POINT (50 100)'), ('POINT (150 150)')))", VARCHAR, "POINT (50 100)");
    }

    @Test
    public void testSTDistance()
    {
        assertFunction("ST_Distance(ST_Point(50, 100), ST_Point(150, 150))", DOUBLE, 111.80339887498948);
    }

    @Test
    public void testSTExteriorRing()
    {
        assertFunction("ST_AsText(ST_ExteriorRing(('POLYGON EMPTY')))", VARCHAR, null);
    }

    @Test
    public void testSTIntersection()
    {
        assertFunction("ST_AsText(ST_Intersection(('POINT (50 100)'), ('POINT (150 150)')))", VARCHAR, "MULTIPOLYGON EMPTY");
        assertFunction("ST_AsText(ST_Intersection(('MULTIPOINT (50 100, 50 200)'), ('Point (50 100)')))", VARCHAR, "POINT (50 100)");
    }

    @Test
    public void testSTSymmetricDifference()
    {
        assertFunction("ST_AsText(ST_SymDifference(('POINT (50 100)'), ('POINT (50 150)')))", VARCHAR, "MULTIPOINT ((50 100), (50 150))");
    }

    @Test
    public void testStContains()
    {
        assertFunction("ST_Contains((null), ('POINT (25 25)'))", BOOLEAN, null);
        assertFunction("ST_Contains(('POINT (20 20)'), ('POINT (25 25)'))", BOOLEAN, false);
        assertFunction("ST_Contains(('MULTIPOINT (20 20, 25 25)'), ('POINT (25 25)'))", BOOLEAN, true);
    }

    @Test
    public void testSTCrosses()
    {
        assertFunction("ST_Crosses(('POINT (20 20)'), ('POINT (25 25)'))", BOOLEAN, false);
    }

    @Test
    public void testSTDisjoint()
    {
        assertFunction("ST_Disjoint(('POINT (50 100)'), ('POINT (150 150)'))", BOOLEAN, true);
    }

    @Test
    public void testSTEquals()
    {
        assertFunction("ST_Equals(('POINT (50 100)'), ('POINT (150 150)'))", BOOLEAN, false);
    }

    @Test
    public void testSTIntersects()
    {
        assertFunction("ST_Intersects(('POINT (50 100)'), ('POINT (150 150)'))", BOOLEAN, false);
    }

    @Test
    public void testSTOverlaps()
    {
        assertFunction("ST_Overlaps(('POINT (50 100)'), ('POINT (150 150)'))", BOOLEAN, false);
    }

    @Test
    public void testSTRelate()
    {
        assertFunction("ST_Relate(('LINESTRING (0 0, 3 3)'), ('LINESTRING (1 1, 4 1)'), '****T****')", BOOLEAN, false);
    }

    @Test
    public void testSTTouches()
    {
        assertFunction("ST_Touches(('POINT (50 100)'), ('POINT (150 150)'))", BOOLEAN, false);
    }

    @Test
    public void testSTWithin()
    {
        assertFunction("ST_Within(('POINT (50 100)'), ('POINT (150 150)'))", BOOLEAN, false);
    }

    @Test
    public void testSTNumGeometries()
    {
        assertSTNumGeometries("POINT EMPTY", 0);
        assertSTNumGeometries("POINT (1 2)", 1);
        assertSTNumGeometries("MULTIPOINT (1 2, 2 4, 3 6, 4 8)", 4);
    }

    private void assertSTNumGeometries(String wkt, int expected)
    {
        assertFunction("ST_NumGeometries(('" + wkt + "'))", INTEGER, expected);
    }

    @Test
    public void testSTGeometryN()
    {
        assertSTGeometryN("POINT EMPTY", 1, null);
        assertSTGeometryN("POINT EMPTY", 0, null);
        assertSTGeometryN("POINT (1 2)", -1, null);
        assertSTGeometryN("POINT (1 2)", 2, null);
        assertSTGeometryN("LINESTRING(77.29 29.07, 77.42 29.26, 77.27 29.31, 77.29 29.07)", 1, "LINESTRING (77.29 29.07, 77.42 29.26, 77.27 29.31, 77.29 29.07)");
    }

    private void assertSTGeometryN(String wkt, int index, String expected)
    {
        assertFunction("ST_ASText(ST_GeometryN(('" + wkt + "')," + index + "))", VARCHAR, expected);
    }
}
