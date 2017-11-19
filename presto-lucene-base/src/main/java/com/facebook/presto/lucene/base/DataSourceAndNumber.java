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
package com.facebook.presto.lucene.base;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DataSourceAndNumber {

    private DataSource source;
    private DataSourceNumber num;

    @JsonCreator
    public DataSourceAndNumber(@JsonProperty("source") DataSource source, @JsonProperty("num") DataSourceNumber num) {
        this.source = source;
        this.num = num;
    }

    @JsonProperty
    public DataSource getSource() {
        return source;
    }

    @JsonProperty
    public DataSourceNumber getNum() {
        return num;
    }

}
