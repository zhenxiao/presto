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

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.ErrorType;

import static com.facebook.presto.spi.ErrorType.EXTERNAL;
import static com.facebook.presto.spi.ErrorType.USER_ERROR;

public enum AresDbErrorCode
        implements ErrorCodeSupplier
{
    ARESDB_HTTP_ERROR(0, EXTERNAL), // numServersResponded < numServersQueried
    ARESDB_UNSUPPORTED_EXPRESSION(1, USER_ERROR), // expression not supported natively in AresDB
    ARESDB_UNSUPPORTED_OUTPUT_TYPE(2, USER_ERROR); // output type returned in not supported in AresDB

    private final ErrorCode errorCode;

    AresDbErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0605_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
