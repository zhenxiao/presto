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
package com.facebook.presto.rta;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Maps.fromProperties;
import static java.lang.String.format;

public class RtaUtil
{
    private RtaUtil()
    {
    }

    public static Map<String, String> loadProperties(File file)
            throws IOException
    {
        Properties properties = new Properties();
        try (InputStream in = new FileInputStream(file)) {
            properties.load(in);
        }
        return fromProperties(properties);
    }

    public static String checkedOr(String original, String dflt)
    {
        return isNullOrEmpty(original) ? dflt : original;
    }

    public static String checked(String original, String fieldName)
    {
        if (isNullOrEmpty(checkedOr(original, null))) {
            throw new NoSuchElementException("Expected non null string for " + fieldName);
        }
        else {
            return original;
        }
    }

    public static <A, B extends A> B checkType(A value, Class<B> target, String name)
    {
        if (value == null) {
            throw new NullPointerException(format("%s is null", name));
        }
        if (!target.isInstance(value)) {
            throw new IllegalArgumentException(String.format("%s must be of type %s, not %s", name, target.getName(), value.getClass().getName()));
        }
        return target.cast(value);
    }
}
