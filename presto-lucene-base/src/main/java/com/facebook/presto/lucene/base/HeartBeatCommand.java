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

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class HeartBeatCommand {

    private List<DataSourceAndNumber> add;
    private List<DataSourceAndNumber> delete;

    @JsonCreator
    public HeartBeatCommand(@JsonProperty("add") List<DataSourceAndNumber> add,
            @JsonProperty("delete") List<DataSourceAndNumber> delete) {
        this.add = add;
        this.delete = delete;
    }

    @JsonProperty
    public List<DataSourceAndNumber> getAdd() {
        return add;
    }

    public void setAdd(List<DataSourceAndNumber> add) {
        this.add = add;
    }

    @JsonProperty
    public List<DataSourceAndNumber> getDelete() {
        return delete;
    }

    public void setDelete(List<DataSourceAndNumber> delete) {
        this.delete = delete;
    }

}
