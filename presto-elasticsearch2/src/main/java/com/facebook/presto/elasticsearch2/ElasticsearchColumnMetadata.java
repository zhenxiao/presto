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
package com.facebook.presto.elasticsearch2;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;

public class ElasticsearchColumnMetadata
        extends ColumnMetadata
{
    private final String jsonPath;
    private final String jsonType;
    private final boolean isList;

    public ElasticsearchColumnMetadata(String name, Type type, String jsonPath, String jsonType, boolean isList)
    {
        super(name, type);
        this.jsonPath = jsonPath;
        this.jsonType = jsonType;
        this.isList = isList;
    }

    public ElasticsearchColumnMetadata(ElasticsearchColumn column)
    {
        this(column.getName(), column.getType(), column.getJsonPath(), column.getJsonType(), column.isList());
    }

    public String getJsonPath()
    {
        return jsonPath;
    }

    public String getJsonType()
    {
        return jsonType;
    }

    public boolean isList()
    {
        return isList;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        ElasticsearchColumnMetadata that = (ElasticsearchColumnMetadata) o;

        if (!jsonPath.equals(that.jsonPath)) {
            return false;
        }
        return jsonType.equals(that.jsonType);
    }

    @Override
    public int hashCode()
    {
        int result = super.hashCode();
        result = 31 * result + jsonPath.hashCode();
        result = 31 * result + jsonType.hashCode();
        return result;
    }
}
