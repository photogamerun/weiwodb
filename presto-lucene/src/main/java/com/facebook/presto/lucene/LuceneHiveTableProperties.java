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

import static com.facebook.presto.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static com.facebook.presto.spi.session.PropertyMetadata.integerSessionProperty;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.inject.Inject;

import com.facebook.presto.lucene.util.LuceneUtils;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;

public class LuceneHiveTableProperties {
	public static final String STORAGE_FORMAT_PROPERTY = "format";
	public static final String STORAGE_MAPPINGT_PROPERTY = LuceneUtils.WEIWODB_SCHEMA_MAPPING_KEY;
	public static final String STORAGE_DATA_LOCATION = LuceneUtils.WEIWODB_DATA_LOCATION;
	public static final String STORAGE_HANDLER = LuceneUtils.WEIWODB_STORAGE_HANDLER;
	public static final String BUCKETED_BY_PROPERTY = "bucketed_by";
    public static final String BUCKET_COUNT_PROPERTY = "bucket_count";

	private final List<PropertyMetadata<?>> tableProperties;

	@Inject
	public LuceneHiveTableProperties(TypeManager typeManager,
			LuceneConfig config) {
		tableProperties = ImmutableList.of(
				new PropertyMetadata<>(STORAGE_FORMAT_PROPERTY,
						"Hive storage format for the table",
						createUnboundedVarcharType(),
						LuceneHiveStorageFormat.class,
						LuceneHiveStorageFormat.WEIWO, false,
						value -> LuceneHiveStorageFormat
								.valueOf(((String) value).toUpperCase(ENGLISH)),
						LuceneHiveStorageFormat::toString),
				new PropertyMetadata<>(STORAGE_MAPPINGT_PROPERTY,
						"Weiwo mapping field", VarcharType.VARCHAR,
						String.class, "", false, value -> (String) value,
						object -> object),
				new PropertyMetadata<>(STORAGE_DATA_LOCATION,
						"Weiwo data location", VarcharType.VARCHAR,
						String.class, "", false, value -> (String) value,
						object -> object),
				new PropertyMetadata<>(STORAGE_HANDLER, "Weiwo storage handle",
						VarcharType.VARCHAR, String.class, "", false,
						value -> (String) value, object -> object),
				new PropertyMetadata<>(
                        BUCKETED_BY_PROPERTY,
                        "Bucketing columns",
                        typeManager.getType(parseTypeSignature("array(varchar)")),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ImmutableList.copyOf(((Collection<?>) value).stream()
                                .map(name -> ((String) name).toLowerCase(ENGLISH))
                                .collect(Collectors.toList())),
                        value -> value),
                integerSessionProperty(BUCKET_COUNT_PROPERTY, "Number of buckets", 0, false));
	}

	public List<PropertyMetadata<?>> getTableProperties() {
		return tableProperties;
	}

	public static LuceneHiveStorageFormat getLuceneHiveStorageFormat(
			Map<String, Object> tableProperties) {
		return (LuceneHiveStorageFormat) tableProperties
				.get(STORAGE_FORMAT_PROPERTY);
	}
	
	public static Optional<HiveBucketProperty> getBucketProperty(Map<String, Object> tableProperties)
    {
        List<String> bucketedBy = getBucketedBy(tableProperties);
        int bucketCount = (Integer) tableProperties.get(BUCKET_COUNT_PROPERTY);
        if ((bucketedBy.isEmpty()) && (bucketCount == 0)) {
            return Optional.empty();
        }
        if (bucketCount < 0) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, format("%s must be greater than zero", BUCKET_COUNT_PROPERTY));
        }
        if (bucketedBy.isEmpty() || bucketCount == 0) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, format("%s and %s must be specified together", BUCKETED_BY_PROPERTY, BUCKET_COUNT_PROPERTY));
        }
        return Optional.of(new HiveBucketProperty(bucketedBy, bucketCount));
    }
	
	@SuppressWarnings("unchecked")
    private static List<String> getBucketedBy(Map<String, Object> tableProperties)
    {
        return (List<String>) tableProperties.get(BUCKETED_BY_PROPERTY);
    }
}
