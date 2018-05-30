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

import com.esri.core.geometry.Envelope2D;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.OperatorSimpleRelation;
import com.esri.core.geometry.QuadTree;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.facebook.presto.spi.PrestoException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.StandardErrorCode.GEOSPATIAL_QUERY_INDEX_ERROR;

public final class QuadTreeUtils
{
    private static final int MAX_QUAD_TREE_LEVEL = 16;

    private QuadTreeUtils()
    {
    }

    public static GeoIndex init(List<GeoItem> geoshapes)
    {
        int id = 0;
        Map<Integer, GeoItem> indexMap = new HashMap<>();
        Map<Integer, Envelope2D> envelope2DMap = new HashMap<>();
        Envelope2D extent = new Envelope2D();

        for (GeoItem shape : geoshapes) {
            OGCGeometry geometry = shape.geometry;

            Envelope2D envelope = new Envelope2D();
            geometry.getEsriGeometry().queryEnvelope2D(envelope);

            envelope2DMap.put(id, envelope);
            extent.merge(envelope);

            indexMap.put(id, shape);
            id++;
        }
        // Init quad tree with simplified envelope2D
        QuadTree quadTree = new QuadTree(extent, MAX_QUAD_TREE_LEVEL);
        for (Map.Entry<Integer, Envelope2D> entry : envelope2DMap.entrySet()) {
            quadTree.insert(entry.getKey(), entry.getValue());
        }

        return new GeoIndex(indexMap, quadTree);
    }

    public static List<GeoItem> queryGeoItem(OGCGeometry inputGeo, GeoIndex geoIndex, OperatorSimpleRelation relationOperator, boolean getFirst)
    {
        QuadTree quadTree = geoIndex.getQuadTree();
        Map<Integer, GeoItem> indexMap = geoIndex.getGeoIndex();
        List<Integer> indexCandidates = new ArrayList<>();
        List<GeoItem> results = new ArrayList<>();

        try {
            Geometry inputGeometry = inputGeo.getEsriGeometry();

            // query quad tree for coarse containing result
            QuadTree.QuadTreeIterator iter = quadTree.getIterator(inputGeometry, 0);
            for (int handle = iter.next(); handle != -1; handle = iter.next()) {
                int element = quadTree.getElement(handle);
                indexCandidates.add(element);
            }

            for (int indexCandidate : indexCandidates) {
                GeoItem geoItem = indexMap.get(indexCandidate);
                OGCGeometry geometry = geoItem.geometry;
                if (relationOperator.execute(geometry.getEsriGeometry(), inputGeometry, geometry.getEsriSpatialReference(), null)) {
                    results.add(geoItem);
                    if (getFirst) {
                        break;
                    }
                }
            }
        }
        catch (Exception e) {
            throw new PrestoException(GEOSPATIAL_QUERY_INDEX_ERROR, "Error in query index", e);
        }
        return results;
    }

    public static List<OGCGeometry> queryGeometry(OGCGeometry inputGeo, GeoIndex geoIndex, OperatorSimpleRelation relationOperator, boolean getFirst)
    {
        return queryGeoItem(inputGeo, geoIndex, relationOperator, getFirst).stream().map(GeoItem::getGeometry).collect(Collectors.toList());
    }

    public static List<String> queryIndexValue(OGCGeometry inputGeo, GeoIndex geoIndex, OperatorSimpleRelation relationOperator, boolean getFirst)
    {
        return queryGeoItem(inputGeo, geoIndex, relationOperator, getFirst).stream().map(GeoItem::getIndexField).collect(Collectors.toList());
    }

    public static class GeoIndex
    {
        private final Map<Integer, GeoItem> geoIndex;
        private final QuadTree quadTree;

        public Map<Integer, GeoItem> getGeoIndex()
        {
            return geoIndex;
        }

        public QuadTree getQuadTree()
        {
            return quadTree;
        }

        public GeoIndex(Map<Integer, GeoItem> geoIndex, QuadTree quadTree)
        {
            this.geoIndex = geoIndex;
            this.quadTree = quadTree;
        }
    }

    public static class GeoItem
    {
        String indexField;
        OGCGeometry geometry;

        public String getIndexField()
        {
            return indexField;
        }

        public OGCGeometry getGeometry()
        {
            return geometry;
        }

        GeoItem(String indexField, OGCGeometry geometry)
        {
            this.indexField = indexField;
            this.geometry = geometry;
        }
    }
}
