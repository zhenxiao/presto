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

package com.facebook.presto.aresdb;

import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.PrestoException;

import java.util.Optional;

public class AresDbException
        extends PrestoException
{
    private final Optional<String> request;

    public AresDbException(ErrorCodeSupplier errorCode, String message, String request)
    {
        super(errorCode, message);
        this.request = Optional.of(request);
    }

    public AresDbException(ErrorCodeSupplier errorCode, String message, String request, Throwable cause)
    {
        super(errorCode, message, cause);
        this.request = Optional.of(request);
    }

    public AresDbException(ErrorCodeSupplier errorCode, String message)
    {
        super(errorCode, message);
        this.request = Optional.empty();
    }

    @Override
    public String getMessage()
    {
        String ret = super.getMessage();
        if (request.isPresent()) {
            ret += " with request \"" + request.get() + "\"";
        }
        return ret;
    }
}
