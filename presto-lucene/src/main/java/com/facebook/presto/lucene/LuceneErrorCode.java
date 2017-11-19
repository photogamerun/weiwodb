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
package com.facebook.presto.lucene;

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.ErrorType;

import static com.facebook.presto.spi.ErrorType.EXTERNAL;

public enum LuceneErrorCode
        implements ErrorCodeSupplier
{
    LUCENE_METASTORE_ERROR(0, EXTERNAL),
    LUCENE_CURSOR_ERROR(1, EXTERNAL),
    LUCENE_TABLE_OFFLINE(2, EXTERNAL),
    LUCENE_CANNOT_OPEN_SPLIT(3, EXTERNAL),
    LUCENE_FILE_NOT_FOUND(4, EXTERNAL),
    LUCENE_UNKNOWN_ERROR(5, EXTERNAL),
    LUCENE_PARTITION_OFFLINE(6, EXTERNAL),
    LUCENE_BAD_DATA(7, EXTERNAL),
    LUCENE_PARTITION_SCHEMA_MISMATCH(8, EXTERNAL),
    LUCENE_MISSING_DATA(9, EXTERNAL),
    LUCENE_INVALID_PARTITION_VALUE(10, EXTERNAL),
    LUCENE_TIMEZONE_MISMATCH(11, EXTERNAL),
    LUCENE_INVALID_METADATA(12, EXTERNAL),
    LUCENE_INVALID_VIEW_DATA(13, EXTERNAL),
    LUCENE_DATABASE_LOCATION_ERROR(14, EXTERNAL),
    LUCENE_PATH_ALREADY_EXISTS(15, EXTERNAL),
    LUCENE_FILESYSTEM_ERROR(16, EXTERNAL),
    // code LUCENE_WRITER_ERROR(17) is deprecated
    LUCENE_SERDE_NOT_FOUND(18, EXTERNAL),
    LUCENE_UNSUPPORTED_FORMAT(19, EXTERNAL),
    LUCENE_PARTITION_READ_ONLY(20, EXTERNAL),
    LUCENE_TOO_MANY_OPEN_PARTITIONS(21, EXTERNAL),
    LUCENE_CONCURRENT_MODIFICATION_DETECTED(22, EXTERNAL),
    LUCENE_COLUMN_ORDER_MISMATCH(23, EXTERNAL),
    LUCENE_FILE_MISSING_COLUMN_NAMES(24, EXTERNAL),
    LUCENE_WRITER_OPEN_ERROR(25, EXTERNAL),
    LUCENE_WRITER_CLOSE_ERROR(26, EXTERNAL),
    LUCENE_WRITER_DATA_ERROR(27, EXTERNAL),
    LUCENE_INVALID_BUCKET_FILES(28, EXTERNAL);

    private final ErrorCode errorCode;

    LuceneErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0100_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
