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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.plugin.geospatial.GeoFunctions.stGeometryFromText;
import static com.facebook.presto.spi.StandardErrorCode.GEOSPATIAL_INDEX_TOO_LARGE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;

public class GeoDataStateFactory
        implements AccumulatorStateFactory<GeoData>
{
    private static final long ARRAYLIST_PAYLOAD_SIZE = ClassLayout.parseClass(ArrayList.class).instanceSize();
    private static final long MAXIMUM_GEO_LIST_SIZE = 50000;

    @Override
    public GeoData createSingleState()
    {
        return new SingleGeoData();
    }

    @Override
    public Class<? extends GeoData> getSingleStateClass()
    {
        return SingleGeoData.class;
    }

    @Override
    public GeoData createGroupedState()
    {
        return new GroupedGeoData();
    }

    @Override
    public Class<? extends GeoData> getGroupedStateClass()
    {
        return GroupedGeoData.class;
    }

    public static class GroupedGeoData
            implements GeoData
    {
        private static final String GEO_QUERY_DOES_NOT_SUPPORT_GROUP_STATE = "Geo Query does not support Group state";

        public GroupedGeoData()
        {
        }

        @Override
        public long getEstimatedSize()
        {
            throw new UnsupportedOperationException(GEO_QUERY_DOES_NOT_SUPPORT_GROUP_STATE);
        }

        @Override
        public void addVarcharShape(Slice id, Slice geoShape)
        {
            throw new UnsupportedOperationException(GEO_QUERY_DOES_NOT_SUPPORT_GROUP_STATE);
        }

        @Override
        public void output(BlockBuilder out)
        {
            throw new UnsupportedOperationException(GEO_QUERY_DOES_NOT_SUPPORT_GROUP_STATE);
        }

        @Override
        public void addBinaryShape(Slice id, Slice geoShape)
        {
            throw new UnsupportedOperationException(GEO_QUERY_DOES_NOT_SUPPORT_GROUP_STATE);
        }
    }

    public static class SingleGeoData
            implements GeoData
    {
        List<GeoShape> geoShapes = new ArrayList<>();
        Hasher hasher = Hashing.sha256().newHasher();
        private long size;

        @Override
        public long getEstimatedSize()
        {
            return size + ARRAYLIST_PAYLOAD_SIZE;
        }

        public void addVarcharShape(Slice id, Slice geoShapeText)
        {
            Slice slice = stGeometryFromText(geoShapeText);
            addBinaryShape(id, slice);
        }

        public void output(BlockBuilder out)
        {
            if (geoShapes == null) {
                out.appendNull();
            }

            byte[] bytes = GeoShape.serialize(geoShapes);
            int totalBytes = SIZE_OF_INT + +SIZE_OF_INT + bytes.length;
            // Try to build slice with hash_code + geo_shapes
            Slice slice = Slices.allocate(totalBytes);
            slice.setInt(0, hasher.hash().asInt());
            slice.setInt(SIZE_OF_INT, bytes.length);
            slice.setBytes(SIZE_OF_INT + SIZE_OF_INT, bytes);
            VarcharType.VARCHAR.writeSlice(out, slice);
        }

        @Override
        public void addBinaryShape(Slice id, Slice geoShape)
        {
            geoShapes.add(new GeoShape(id, geoShape));
            if (geoShapes.size() > MAXIMUM_GEO_LIST_SIZE) {
                throw new PrestoException(GEOSPATIAL_INDEX_TOO_LARGE, "build_geo_index doesn't support geo list size larger than " + MAXIMUM_GEO_LIST_SIZE);
            }
            size += (id.getRetainedSize() + geoShape.getRetainedSize());
            hasher = hasher.putBytes(id.getBytes());
            hasher = hasher.putBytes(geoShape.getBytes());
        }
    }
}
