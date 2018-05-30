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

import com.facebook.presto.Session;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.metadata.FunctionExtractor.extractFunctions;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

public class TestGeoQueries
        extends AbstractTestQueryFramework
{
    public TestGeoQueries()
    {
        super(TestGeoQueries::createLocalQueryRunner);
    }

    @Test
    public void testSTPolygon()
            throws Exception
    {
        assertQuery("select st_geometry_to_text(st_polygon('polygon ((1 1, 1 4, 4 4, 4 1))'))", "select 'POLYGON ((1 1, 4 1, 4 4, 1 4, 1 1))'");
    }

    @Test
    public void testSTLine()
            throws Exception
    {
        assertQuery("select st_geometry_to_text(st_line('linestring(1 1, 2 2, 1 3)'))", "select 'LINESTRING (1 1, 2 2, 1 3)'");
    }

    @Test
    public void testSTCrosses()
            throws Exception
    {
        assertQuery("select st_crosses(st_line('linestring(0 0, 1 1)'), st_line('linestring(1 0, 0 1)'))", "select true");
        assertQuery("select st_crosses(st_polygon('polygon ((1 1, 1 4, 4 4, 4 1))'), st_polygon('polygon ((2 2, 2 5, 5 5, 5 2))'))", "select false");
        assertQuery("select st_crosses(st_polygon('polygon ((1 1, 1 4, 4 4, 4 1))'), st_line('linestring(2 0, 2 3)'))", "select true");
        assertQuery("select st_crosses(st_geometry_to_text(st_line('linestring(0 0, 1 1)')), st_geometry_to_text(st_line('linestring(1 0, 0 1)')))", "select true");
    }

    @Test
    public void testSTDisjoint()
            throws Exception
    {
        assertQuery("select st_disjoint(st_line('linestring(0 0, 0 1)'), st_line('linestring(1 1, 1 0)'))", "select true");
        assertQuery("select st_disjoint(st_polygon('polygon ((1 1, 1 3, 3 3, 3 1))'), st_polygon('polygon ((4 4, 4 5, 5 5, 5 4))'))", "select true");
        assertQuery("select st_disjoint(st_geometry_to_text(st_polygon('polygon ((1 1, 1 3, 3 3, 3 1))')), st_geometry_to_text(st_polygon('polygon ((4 4, 4 5, 5 5, 5 4))')))", "select true");
    }

    @Test
    public void testSTEnvelopeIntersect()
            throws Exception
    {
        assertQuery("select st_envelope_intersect(st_line('linestring(0 0, 2 2)'), st_line('linestring(1 1, 3 3)'))", "select true");
    }

    @Test
    public void testSTEquals()
            throws Exception
    {
        assertQuery("select st_equals(st_line('linestring(0 0, 2 2)'), st_line('linestring(0 0, 2 2)'))", "select true");
        assertQuery("select st_equals(st_geometry_to_text(st_line('linestring(0 0, 2 2)')), st_geometry_to_text(st_line('linestring(0 0, 2 2)')))", "select true");
    }

    @Test
    public void testSTIntersects()
            throws Exception
    {
        assertQuery("select st_intersects(st_line('linestring(8 4, 4 8)'), st_polygon('polygon ((2 2, 2 6, 6 6, 6 2))'))", "select true");
        assertQuery("select st_intersects('linestring(8 4, 4 8)', 'polygon ((2 2, 2 6, 6 6, 6 2))')", "select true");
        assertQuery("select st_intersects(st_line('linestring(8 4, 4 8)'), 'polygon ((2 2, 2 6, 6 6, 6 2))')", "select true");
        assertQuery("select st_intersects('linestring(8 4, 4 8)', st_polygon('polygon ((2 2, 2 6, 6 6, 6 2))'))", "select true");
    }

    @Test
    public void testSTOverlaps()
            throws Exception
    {
        assertQuery("select st_overlaps(st_polygon('polygon ((1 1, 1 4, 4 4, 4 1))'), st_polygon('polygon ((3 3, 3 5, 5 5, 5 3))'))", "select true");
    }

    @Test
    public void testSTRelate()
            throws Exception
    {
        assertQuery("select st_relate(st_polygon('polygon ((2 0, 2 1, 3 1))'), st_polygon('polygon ((1 1, 1 4, 4 4, 4 1))'), '****T****')", "select true");
    }

    @Test
    public void testSTTouches()
            throws Exception
    {
        assertQuery("select st_touches(st_point(1, 2), st_polygon('polygon ((1 1, 1 4, 4 4, 4 1))'))", "select true");
    }

    @Test
    public void testSTWithin()
            throws Exception
    {
        assertQuery("select st_within(st_point(3, 2), st_polygon('polygon ((1 1, 1 4, 4 4, 4 1))'))", "select true");
        assertQuery("select st_within(st_geometry_to_text(st_point(3, 2)), st_geometry_to_text(st_polygon('polygon ((1 1, 1 4, 4 4, 4 1))')))", "select true");
    }

    @Test
    public void testSTEnvelope()
            throws Exception
    {
        assertQuery("select st_geometry_to_text(st_envelope(st_line('linestring(1 1, 2 2, 1 3)')))", "select 'POLYGON ((1 1, 2 1, 2 3, 1 3, 1 1))'");
    }

    @Test
    public void testSTBoundary()
            throws Exception
    {
        assertQuery("select st_geometry_to_text(st_boundary(st_polygon('polygon ((1 1, 4 1, 1 4))')))", "select 'LINESTRING (1 1, 4 1, 1 4, 1 1)'");
    }

    @Test
    public void testSTDifference()
            throws Exception
    {
        assertQuery("select st_geometry_to_text(st_difference(st_polygon('polygon ((1 1, 1 4, 4 4, 4 1))'), st_polygon('polygon ((2 2, 2 5, 5 5, 5 2))')))", "select 'POLYGON ((1 1, 4 1, 4 2, 2 2, 2 4, 1 4, 1 1))'");
    }

    @Test
    public void testSTDistance()
            throws Exception
    {
        assertQuery("select st_distance(st_polygon('polygon ((1 1, 1 3, 3 3, 3 1))'), st_polygon('polygon ((4 4, 4 5, 5 5, 5 4))'))", "select 1.4142135623730951");
        assertQuery("select st_distance(st_geometry_to_text(st_polygon('polygon ((1 1, 1 3, 3 3, 3 1))')), st_geometry_to_text(st_polygon('polygon ((4 4, 4 5, 5 5, 5 4))')))", "select 1.4142135623730951");
    }

    @Test
    public void testSTExteriorRing()
            throws Exception
    {
        assertQuery("select st_geometry_to_text(st_exterior_ring(st_polygon('polygon ((1 1, 1 4, 4 1))')))", "select 'LINESTRING (1 1, 4 1, 1 4, 1 1)'");
    }

    @Test
    public void testSTIntersection()
            throws Exception
    {
        assertQuery("select st_geometry_to_text(st_intersection(st_polygon('polygon ((1 1, 1 4, 4 4, 4 1))'), st_polygon('polygon ((2 2, 2 5, 5 5, 5 2))')))", "select 'POLYGON ((2 2, 4 2, 4 4, 2 4, 2 2))'");
        assertQuery("select st_geometry_to_text(st_intersection(st_polygon('polygon ((1 1, 1 4, 4 4, 4 1))'), st_line('linestring(2 0, 2 3)')))", "select 'LINESTRING (2 1, 2 3)'");
    }

    @Test
    public void testSTSymmetricDifference()
            throws Exception
    {
        assertQuery("select st_geometry_to_text(st_symmetric_difference(st_polygon('polygon ((1 1, 1 4, 4 4, 4 1))'), st_polygon('polygon ((2 2, 2 5, 5 5, 5 2))')))", "select 'MULTIPOLYGON (((1 1, 4 1, 4 2, 2 2, 2 4, 1 4, 1 1)), ((4 2, 5 2, 5 5, 2 5, 2 4, 4 4, 4 2)))'");
    }

    @Test
    public void testSTArea()
            throws Exception
    {
        assertQuery("select st_area(st_geometry_to_text(st_polygon('polygon ((2 2, 2 6, 6 6, 6 2))')))", "select 16.0");
        assertQuery("select st_area(st_polygon('polygon ((2 2, 2 6, 6 6, 6 2))'))", "select 16.0");
    }

    @Test
    public void testSTCentroid()
            throws Exception
    {
        assertQuery("select st_geometry_to_text(st_centroid(st_polygon('polygon ((1 1, 1 4, 4 4, 4 1))')))", "select 'POINT (2.5 2.5)'");
    }

    @Test
    public void testSTIsClosed()
            throws Exception
    {
        assertQuery("select st_is_closed(st_line('linestring(1 1, 2 2, 1 3, 1 1)'))", "select true");
        assertQuery("select st_is_closed(st_geometry_to_text(st_line('linestring(1 1, 2 2, 1 3, 1 1)')))", "select true");
    }

    @Test
    public void testSTIsEmpty()
            throws Exception
    {
        assertQuery("select st_is_empty(st_point(1, 2))", "select false");
    }

    @Test
    public void testSTLength()
            throws Exception
    {
        assertQuery("select st_length(st_line('linestring(0 0, 2 2)'))", "select 2.8284271247461903");
        assertQuery("select st_length(st_polygon('polygon ((1 1, 1 4, 4 4, 4 1))'))", "select 12.0");
    }

    @Test
    public void testSTMax()
            throws Exception
    {
        assertQuery("select st_max_x(st_line('linestring(8 4, 4 8)'))", "select 8.0");
        assertQuery("select st_max_y(st_line('linestring(8 4, 4 8)'))", "select 8.0");
        assertQuery("select st_max_x(st_polygon('polygon ((2 0, 2 1, 3 1))'))", "select 3.0");
        assertQuery("select st_max_y(st_polygon('polygon ((2 0, 2 1, 3 1))'))", "select 1.0");
    }

    @Test
    public void testSTMin()
            throws Exception
    {
        assertQuery("select st_min_x(st_line('linestring(8 4, 4 8)'))", "select 4.0");
        assertQuery("select st_min_y(st_line('linestring(8 4, 4 8)'))", "select 4.0");
        assertQuery("select st_min_x(st_polygon('polygon ((2 0, 2 1, 3 1))'))", "select 2.0");
        assertQuery("select st_min_y(st_polygon('polygon ((2 0, 2 1, 3 1))'))", "select 0.0");
    }

    @Test
    public void testSTInteriorRingNumber()
            throws Exception
    {
        assertQuery("select st_interior_ring_number(st_polygon('polygon ((0 0, 8 0, 0 8, 0 0), (1 1, 1 5, 5 1, 1 1))'))", "select 1");
    }

    @Test
    public void testSTPointNumber()
            throws Exception
    {
        assertQuery("select st_point_number(st_polygon('polygon ((0 0, 8 0, 0 8, 0 0), (1 1, 1 5, 5 1, 1 1))'))", "select 8");
    }

    @Test
    public void testSTIsRing()
            throws Exception
    {
        assertQuery("select st_is_ring(st_line('linestring(8 4, 4 8)'))", "select false");
    }

    @Test
    public void testSTStartEndPoint()
            throws Exception
    {
        assertQuery("select st_geometry_to_text(st_start_point(st_line('linestring(8 4, 4 8, 5 6)')))", "select 'POINT (8 4)'");
        assertQuery("select st_geometry_to_text(st_end_point(st_line('linestring(8 4, 4 8, 5 6)')))", "select 'POINT (5 6)'");
    }

    @Test
    public void testSTXY()
            throws Exception
    {
        assertQuery("select st_x(st_point(1, 2))", "select 1.0");
        assertQuery("select st_y(st_point(1, 2))", "select 2.0");
    }

    @Test
    public void testSTGeoHash()
            throws Exception
    {
        assertQuery("select st_geohash(-126, 48)", "select 'c0w3hf1s70w3'");
        assertQuery("select st_geohash(-126, 48, 5)", "select 'c0w3h'");
        assertQuery("select st_geohash(st_point(-126, 48))", "select 'c0w3hf1s70w3'");
        assertQuery("select st_geohash(st_point(-126, 48), 5)", "select 'c0w3h'");
        assertQueryFails("select st_geohash(st_line('linestring(8 4, 4 8, 5 6)'), 4)", "st_geohash only supports st " +
                "point");
    }

    @Test
    public void testStContains()
            throws Exception
    {
        assertQuery("SELECT ST_Contains(st_geometry_from_text('POLYGON((0 2,1 1,0 -1,0 2))'), st_geometry_from_text('POLYGON((-1 3,2 1,0 -3,-1 3))'))", "SELECT false");
        assertQuery("SELECT ST_Contains(st_geometry_from_text('LINESTRING(20 20,30 30)'), st_geometry_from_text('POINT(25 25)'))", "SELECT true");
        assertQuery("SELECT ST_Contains(st_geometry_from_text('POLYGON((-1 2, 0 3, 0 1, -1 2))'), st_geometry_from_text('POLYGON((0 3, -1 2, 0 1, 0 3))'))", "SELECT true");
        assertQuery("SELECT ST_Contains(st_geometry_from_text(null), st_geometry_from_text('POINT(25 25)'))", "SELECT null");
        assertQuery("SELECT ST_Contains('POLYGON((0 2,1 1,0 -1,0 2))', 'POLYGON((-1 3,2 1,0 -3,-1 3))')", "SELECT false");
        assertQuery("SELECT ST_Contains('LINESTRING(20 20,30 30)', 'POINT(25 25)')", "SELECT true");
        assertQuery("SELECT ST_Contains('POLYGON((-1 2, 0 3, 0 1, -1 2))', 'POLYGON((0 3, -1 2, 0 1, 0 3))')", "SELECT true");
        assertQuery("SELECT ST_Contains(null, 'POINT(25 25)')", "SELECT null");
        assertQuery("SELECT ST_Contains('POLYGON((0 2,1 1,0 -1,0 2))', st_geometry_from_text('POLYGON((-1 3,2 1,0 -3,-1 3))'))", "SELECT false");
        assertQuery("SELECT ST_Contains(st_geometry_from_text('POLYGON((0 2,1 1,0 -1,0 2))'), 'POLYGON((-1 3,2 1,0 -3,-1 3))')", "SELECT false");
        assertQuery("SELECT ST_Contains('LINESTRING(20 20,30 30)', st_geometry_from_text('POINT(25 25)'))", "SELECT true");
        assertQuery("SELECT ST_Contains(st_geometry_from_text('LINESTRING(20 20,30 30)'), 'POINT(25 25)')", "SELECT true");
    }

    @Test
    public void testGetGeoShape()
            throws Exception
    {
        assertQuery("select st_geometry_to_text(get_geo_shape('POINT(25 25)', model)) FROM " +
                "(SELECT build_geo_index(shape_id, shape) as model from (VALUES ('sfo', 'LINESTRING(20 20,30 30)'), ('sjc', 'POLYGON((-1 2, 0 3, 0 1, -1 2))')) as t(shape_id, shape))", "SELECT 'LINESTRING (20 20, 30 30)'");
        assertQuery("select st_geometry_to_text(get_geo_shape(st_geometry_from_text('point(25 25)'), model)) FROM " +
                "(SELECT build_geo_index(shape_id, shape) as model from (VALUES ('sfo', 'LINESTRING(20 20,30 30)'), ('sjc', 'POLYGON((-1 2, 0 3, 0 1, -1 2))')) as t(shape_id, shape))", "SELECT 'LINESTRING (20 20, 30 30)'");
    }

    @Test
    public void testGeoContainsFirst()
            throws Exception
    {
        assertQuery("select geo_contains('POINT(25 25)', model) FROM " +
                "(SELECT build_geo_index(shape_id, shape) as model from (VALUES ('sfo', 'LINESTRING(20 20,30 30)'), ('sjc', 'POLYGON((-1 2, 0 3, 0 1, -1 2))')) as t(shape_id, shape))", "SELECT 'sfo'");
        assertQuery("select geo_contains(st_geometry_from_text('POINT(25 25)'), model) FROM " +
                "(SELECT build_geo_index(shape_id, shape) as model from (VALUES ('sfo', 'LINESTRING(20 20,30 30)'), ('sjc', 'POLYGON((-1 2, 0 3, 0 1, -1 2))')) as t(shape_id, shape))", "SELECT 'sfo'");
        assertQuery("select geo_contains('POINT(25 25)', model) FROM " +
                "(SELECT build_geo_index(shape_id, shape) as model from (VALUES ('sfo', st_geometry_from_text('LINESTRING(20 20,30 30)')), ('sjc', st_geometry_from_text('POLYGON((-1 2, 0 3, 0 1, -1 2))'))) as t(shape_id, shape))", "SELECT 'sfo'");
        assertQuery("select geo_contains(st_geometry_from_text('POINT(25 25)'), model) FROM " +
                "(SELECT build_geo_index(shape_id, shape) as model from (VALUES ('sfo', st_geometry_from_text('LINESTRING(20 20,30 30)')), ('sjc', st_geometry_from_text('POLYGON((-1 2, 0 3, 0 1, -1 2))'))) as t(shape_id, shape))", "SELECT 'sfo'");
        // build_geo_index support json input
        assertQuery("select geo_contains('POINT(25 25)', model) FROM " +
                "(SELECT build_geo_index(shape_id, shape) as model from (VALUES (cast('[sfo, 1]' as json) , 'LINESTRING(20 20,30 30)'), (cast('[sjc, 2]' as json), 'POLYGON((-1 2, 0 3, 0 1, -1 2))')) as t(shape_id, shape))", "SELECT '\"[sfo, 1]\"'");
    }

    @Test
    public void testGeoIntersectsFirst()
            throws Exception
    {
        assertQuery("select geo_intersects('POLYGON((-1 2,0 3,0 1,-1 2))', model) from" +
                "(select build_geo_index(shape_id, shape) as model from (VALUES ('sfo', 'POLYGON((1 0,1 1,0 1,1 0))')) as t(shape_id, shape))", "SELECT 'sfo'");
        assertQuery("select geo_intersects(st_geometry_from_text('POLYGON((-1 2,0 3,0 1,-1 2))'), model) from" +
                "(select build_geo_index(shape_id, shape) as model from (VALUES ('sfo', 'POLYGON((1 0,1 1,0 1,1 0))')) as t(shape_id, shape))", "SELECT 'sfo'");
    }

    @Test
    public void testGeoIntersectsFirstEmptyResult()
            throws Exception
    {
        assertQuery("select geo_intersects('POLYGON((-1 2,0 3,0 1,-1 2))', model) from" +
                "(select build_geo_index(shape_id, shape) as model from (VALUES ('sfo', 'POLYGON((1 0,1 1,2 2,1 0))')) as t(shape_id, shape))", "SELECT NULL ");
    }

    @Test
    public void testGeoContainsFirstEmptyResult()
            throws Exception
    {
        assertQuery("select geo_contains('POINT(25 25)', model) FROM " +
                "(SELECT build_geo_index(shape_id, shape) as model from (VALUES ('34251', 'POLYGON((-1 2, 0 3, 0 1, -1 2))')) as t(shape_id, shape))", "SELECT NULL");
    }

    @Test
    public void testGeoContainsAllIncludeShape()
            throws Exception
    {
        assertQuery("select CAST(element_at(geo_contains_all_with_shape('POINT(25 25)', model),1) as ROW(f1 varchar, f2 varbinary)).f1 FROM (SELECT build_geo_index(shape_id, shape) as model from (VALUES ('34251', 'LINESTRING(20 20,30 30)'), ('341', 'LINESTRING(20 20,30 30)')) as t(shape_id, shape))", "select 34251");
        assertQuery("select CAST(element_at(geo_contains_all_with_shape('POINT(25 25)', model),2) as ROW(f1 varchar, f2 varbinary)).f1 FROM (SELECT build_geo_index(shape_id, shape) as model from (VALUES ('34251', 'LINESTRING(20 20,30 30)'), ('341', 'LINESTRING(20 20,30 30)')) as t(shape_id, shape))", "select 341");
        assertQuery("select st_geometry_to_text(CAST(element_at(geo_contains_all_with_shape('POINT(25 25)', model),1) as ROW(f1 varchar, f2 varbinary)).f2) FROM (SELECT build_geo_index(shape_id, shape) as model from (VALUES ('34251', 'LINESTRING(20 20,30 30)'), ('341', 'LINESTRING(20 20,30 30)')) as t(shape_id, shape))", "select 'LINESTRING (20 20, 30 30)'");
    }

    @Test
    public void testGeoIntersectsAllIncludeShape()
            throws Exception
    {
        assertQuery("select CAST(element_at(geo_intersects_all_with_shape('POINT(25 25)', model),1) as ROW(f1 varchar, f2 varbinary)).f1 FROM (SELECT build_geo_index(shape_id, shape) as model from (VALUES ('34251', 'LINESTRING(20 20,30 30)'), ('341', 'LINESTRING(20 20,30 30)')) as t(shape_id, shape))", "select 34251");
        assertQuery("select CAST(element_at(geo_intersects_all_with_shape('POINT(25 25)', model),2) as ROW(f1 varchar, f2 varbinary)).f1 FROM (SELECT build_geo_index(shape_id, shape) as model from (VALUES ('34251', 'LINESTRING(20 20,30 30)'), ('341', 'LINESTRING(20 20,30 30)')) as t(shape_id, shape))", "select 341");
        assertQuery("select st_geometry_to_text(CAST(element_at(geo_intersects_all_with_shape('POINT(25 25)', model),1) as ROW(f1 varchar, f2 varbinary)).f2) FROM (SELECT build_geo_index(shape_id, shape) as model from (VALUES ('34251', 'LINESTRING(20 20,30 30)'), ('341', 'LINESTRING(20 20,30 30)')) as t(shape_id, shape))", "select 'LINESTRING (20 20, 30 30)'");
    }

    @Test
    public void testInvalidInputGeometryFromText()
            throws Exception
    {
        assertQueryFails("select st_geometry_from_text('linestring( 4, 4 8, 5 6)')", "Cannot convert to geometry: linestring\\( 4, 4 8, 5 6\\)");
    }

    private static LocalQueryRunner createLocalQueryRunner()
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("local")
                .setSchema(TINY_SCHEMA_NAME)
                //.setSystemProperty(SystemSessionProperties.REWRITE_GEOSPATIAL_QUERY, "true")
                .build();

        LocalQueryRunner localQueryRunner = new LocalQueryRunner(defaultSession);

        // add the tpch catalog
        // local queries run directly against the generator
        localQueryRunner.createCatalog(
                defaultSession.getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.<String, String>of());

        localQueryRunner.getMetadata().addFunctions(extractFunctions(new GeoPlugin().getFunctions()));

        return localQueryRunner;
    }
}
