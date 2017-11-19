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
package com.facebook.presto.sql.analyzer;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.sql.analyzer.FeaturesConfig.FILE_BASED_RESOURCE_GROUP_MANAGER;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.ProcessingOptimization.COLUMNAR_DICTIONARY;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.ProcessingOptimization.DISABLED;
import static com.facebook.presto.sql.analyzer.RegexLibrary.JONI;
import static com.facebook.presto.sql.analyzer.RegexLibrary.RE2J;
import static io.airlift.configuration.testing.ConfigAssertions.assertDeprecatedEquivalence;
import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;

public class TestFeaturesConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(ConfigAssertions.recordDefaults(FeaturesConfig.class)
                .setExperimentalSyntaxEnabled(false)
                .setResourceGroupsEnabled(false)
                .setDistributedIndexJoinsEnabled(false)
                .setDistributedJoinsEnabled(true)
                .setColocatedJoinsEnabled(false)
                .setRedistributeWrites(true)
                .setOptimizeMetadataQueries(false)
                .setOptimizeHashGeneration(true)
                .setOptimizeSingleDistinct(true)
                .setPushTableWriteThroughUnion(true)
                .setProcessingOptimization(DISABLED)
                .setDictionaryAggregation(false)
                .setLegacyArrayAgg(false)
                .setRegexLibrary(JONI)
                .setRe2JDfaStatesLimit(Integer.MAX_VALUE)
                .setRe2JDfaRetries(5)
                .setResourceGroupManager(FILE_BASED_RESOURCE_GROUP_MANAGER));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> propertiesLegacy = new ImmutableMap.Builder<String, String>()
                .put("analyzer.experimental-syntax-enabled", "true")
                .put("experimental.resource-groups-enabled", "true")
                .put("deprecated.legacy-array-agg", "true")
                .put("distributed-index-joins-enabled", "true")
                .put("distributed-joins-enabled", "false")
                .put("colocated-joins-enabled", "true")
                .put("redistribute-writes", "false")
                .put("optimizer.optimize-metadata-queries", "true")
                .put("optimizer.optimize-hash-generation", "false")
                .put("optimizer.optimize-single-distinct", "false")
                .put("optimizer.push-table-write-through-union", "false")
                .put("optimizer.processing-optimization", "columnar_dictionary")
                .put("optimizer.dictionary-aggregation", "true")
                .put("regex-library", "RE2J")
                .put("re2j.dfa-states-limit", "42")
                .put("re2j.dfa-retries", "42")
                .put("resource-group-manager", "test")
                .build();
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("experimental-syntax-enabled", "true")
                .put("experimental.resource-groups-enabled", "true")
                .put("deprecated.legacy-array-agg", "true")
                .put("distributed-index-joins-enabled", "true")
                .put("distributed-joins-enabled", "false")
                .put("colocated-joins-enabled", "true")
                .put("redistribute-writes", "false")
                .put("optimizer.optimize-metadata-queries", "true")
                .put("optimizer.optimize-hash-generation", "false")
                .put("optimizer.optimize-single-distinct", "false")
                .put("optimizer.push-table-write-through-union", "false")
                .put("optimizer.processing-optimization", "columnar_dictionary")
                .put("optimizer.dictionary-aggregation", "true")
                .put("regex-library", "RE2J")
                .put("re2j.dfa-states-limit", "42")
                .put("re2j.dfa-retries", "42")
                .put("resource-group-manager", "test")
                .build();

        FeaturesConfig expected = new FeaturesConfig()
                .setExperimentalSyntaxEnabled(true)
                .setResourceGroupsEnabled(true)
                .setDistributedIndexJoinsEnabled(true)
                .setDistributedJoinsEnabled(false)
                .setColocatedJoinsEnabled(true)
                .setRedistributeWrites(false)
                .setOptimizeMetadataQueries(true)
                .setOptimizeHashGeneration(false)
                .setOptimizeSingleDistinct(false)
                .setPushTableWriteThroughUnion(false)
                .setProcessingOptimization(COLUMNAR_DICTIONARY)
                .setDictionaryAggregation(true)
                .setLegacyArrayAgg(true)
                .setRegexLibrary(RE2J)
                .setRe2JDfaStatesLimit(42)
                .setRe2JDfaRetries(42)
                .setResourceGroupManager("test");

        assertFullMapping(properties, expected);
        assertDeprecatedEquivalence(FeaturesConfig.class, properties, propertiesLegacy);
    }
}
