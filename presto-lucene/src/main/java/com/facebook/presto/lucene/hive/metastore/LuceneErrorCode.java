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
package com.facebook.presto.lucene.hive.metastore;

import static com.facebook.presto.spi.ErrorType.EXTERNAL;

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.ErrorType;

public enum LuceneErrorCode implements ErrorCodeSupplier {
    
    LUCENE_METASTORE_ERROR(0, EXTERNAL),
    LUCENE_CURSOR_ERROR(1, EXTERNAL),
    LUCENE_CANNOT_OPEN_SPLIT(2, EXTERNAL),
    LUCENE_FILE_NOT_FOUND(3, EXTERNAL),
    LUCENE_UNKNOWN_ERROR(4, EXTERNAL),
    LUCENE_BAD_DATA(5, EXTERNAL),
    LUCENE_MISSING_DATA(6, EXTERNAL),
    LUCENE_PATH_ALREADY_EXISTS(7, EXTERNAL),
    LUCENE_FILESYSTEM_ERROR(8, EXTERNAL),
    LUCENE_INVALID_METADATA(9, EXTERNAL);

    private final ErrorCode errorCode;

    LuceneErrorCode(int code, ErrorType type) {
        this.errorCode = new ErrorCode(code, name(), type);
    }

    @Override
    public ErrorCode toErrorCode() {
        return errorCode;
    }
}
