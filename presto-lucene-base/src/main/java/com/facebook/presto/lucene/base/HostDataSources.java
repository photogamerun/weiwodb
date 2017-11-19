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

import com.facebook.presto.spi.HostAddress;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class HostDataSources {

    private HostAddress hostAddress;
    private List<DataSourceNumber> sources;
    private List<DataSourceNumber> adds;
    private List<DataSourceNumber> deletes;

    @JsonCreator
    public HostDataSources(@JsonProperty("hostAddress") HostAddress hostAddress,
            @JsonProperty("sources") List<DataSourceNumber> sources, @JsonProperty("adds") List<DataSourceNumber> adds,
            @JsonProperty("deletes") List<DataSourceNumber> deletes) {
        this.hostAddress = hostAddress;
        this.sources = sources;
        this.adds = adds;
        this.deletes = deletes;
    }

    @JsonProperty
    public HostAddress getHostAddress() {
        return hostAddress;
    }

    public void setHostAddress(HostAddress hostAddress) {
        this.hostAddress = hostAddress;
    }

    @JsonProperty
    public List<DataSourceNumber> getSources() {
        return sources;
    }

    public void setSources(List<DataSourceNumber> sources) {
        this.sources = sources;
    }

    @JsonProperty
    public List<DataSourceNumber> getAdds() {
        return adds;
    }

    public void setAdds(List<DataSourceNumber> adds) {
        this.adds = adds;
    }

    @JsonProperty
    public List<DataSourceNumber> getDeletes() {
        return deletes;
    }

    public void setDeletes(List<DataSourceNumber> deletes) {
        this.deletes = deletes;
    }

}
