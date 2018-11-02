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

import com.facebook.presto.Session;
import com.facebook.presto.plugin.geospatial.GeoPlugin;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static com.facebook.presto.metadata.FunctionExtractor.extractFunctions;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestGeoIndexFunction
        extends AbstractTestQueryFramework
{
    protected TestGeoIndexFunction()
    {
        super(TestGeoIndexFunction::createLocalQueryRunner);
    }

    private static QueryRunner createLocalQueryRunner()
    {
        Session defaultSession = testSessionBuilder().build();
        LocalQueryRunner localQueryRunner = new LocalQueryRunner(defaultSession);

        GeoPlugin plugin = new GeoPlugin();
        for (Type type : plugin.getTypes()) {
            localQueryRunner.getTypeManager().addType(type);
        }
        localQueryRunner.getMetadata().addFunctions(extractFunctions(plugin.getFunctions()));

        return localQueryRunner;
    }

    @Test
    public void testGeoContainsFirst()
    {
        assertQuery("select geo_contains('POINT(25 25)', model) FROM " +
                "(SELECT build_geo_index(shape_id, shape) as model from (VALUES ('sfo', 'LINESTRING(20 20,30 30)'), ('sjc', 'POLYGON((-1 2, 0 3, 0 1, -1 2))')) as t(shape_id, shape))", "SELECT 'sfo'");
        assertQuery("select geo_contains(st_geometry_from_text('POINT(25 25)'), model) FROM " +
                "(SELECT build_geo_index(shape_id, shape) as model from (VALUES ('sfo', 'LINESTRING(20 20,30 30)'), ('sjc', 'POLYGON((-1 2, 0 3, 0 1, -1 2))')) as t(shape_id, shape))", "SELECT 'sfo'");
        assertQuery("select geo_contains('POINT(25 25)', model) FROM " +
                "(SELECT build_geo_index(shape_id, shape) as model from (VALUES ('sfo', st_geometry_from_text('LINESTRING(20 20,30 30)')), ('sjc', st_geometry_from_text('POLYGON((-1 2, 0 3, 0 1, -1 2))'))) as t(shape_id, shape))", "SELECT 'sfo'");
        assertQuery("select geo_contains(st_geometry_from_text('POINT(25 25)'), model) FROM " +
                "(SELECT build_geo_index(shape_id, shape) as model from (VALUES ('sfo', st_geometry_from_text('LINESTRING(20 20,30 30)')), ('sjc', st_geometry_from_text('POLYGON((-1 2, 0 3, 0 1, -1 2))'))) as t(shape_id, shape))", "SELECT 'sfo'");
    }

    @Test
    public void testGeoIntersectsFirst()
    {
        assertQuery("select geo_intersects('POLYGON((-1 2,0 3,0 1,-1 2))', model) from" +
                "(select build_geo_index(shape_id, shape) as model from (VALUES ('sfo', 'POLYGON((1 0,1 1,0 1,1 0))')) as t(shape_id, shape))", "SELECT 'sfo'");
        assertQuery("select geo_intersects(st_geometry_from_text('POLYGON((-1 2,0 3,0 1,-1 2))'), model) from" +
                "(select build_geo_index(shape_id, shape) as model from (VALUES ('sfo', 'POLYGON((1 0,1 1,0 1,1 0))')) as t(shape_id, shape))", "SELECT 'sfo'");
    }

    @Test
    public void testGeoIntersectsFirstEmptyResult()
    {
        assertQuery("select geo_intersects('POLYGON((-1 2,0 3,0 1,-1 2))', model) from" +
                "(select build_geo_index(shape_id, shape) as model from (VALUES ('sfo', 'POLYGON((1 0,1 1,2 2,1 0))')) as t(shape_id, shape))", "SELECT NULL ");
    }

    @Test
    public void testGeoContainsFirstEmptyResult()
    {
        assertQuery("select geo_contains('POINT(25 25)', model) FROM " +
                "(SELECT build_geo_index(shape_id, shape) as model from (VALUES ('34251', 'POLYGON((-1 2, 0 3, 0 1, -1 2))')) as t(shape_id, shape))", "SELECT NULL");
    }

    @Test
    public void testGeoContainsAllIncludeShape()
    {
        assertQuery("select element_at(geo_contains_all('POINT(25 25)', model),1)  FROM (SELECT build_geo_index(shape_id, shape) as model from (VALUES ('34251', 'LINESTRING(20 20,30 30)'), ('341', 'LINESTRING(20 20,30 30)')) as t(shape_id, shape))", "select 34251");
        assertQuery("select element_at(geo_contains_all('POINT(25 25)', model),2) FROM (SELECT build_geo_index(shape_id, shape) as model from (VALUES ('34251', 'LINESTRING(20 20,30 30)'), ('341', 'LINESTRING(20 20,30 30)')) as t(shape_id, shape))", "select 341");
    }

    @Test
    public void testGeoIntersectsAllIncludeShape()
    {
        assertQuery("select element_at(geo_intersects_all('POINT(25 25)', model),1) FROM (SELECT build_geo_index(shape_id, shape) as model from (VALUES ('34251', 'LINESTRING(20 20,30 30)'), ('341', 'LINESTRING(20 20,30 30)')) as t(shape_id, shape))", "select 34251");
        assertQuery("select element_at(geo_intersects_all('POINT(25 25)', model),2) FROM (SELECT build_geo_index(shape_id, shape) as model from (VALUES ('34251', 'LINESTRING(20 20,30 30)'), ('341', 'LINESTRING(20 20,30 30)')) as t(shape_id, shape))", "select 341");
    }
}
