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
package com.facebook.presto.weiwo.data.source;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.facebook.presto.lucene.base.DataSourceNumber;
import com.facebook.presto.spi.HostAddress;

public class SourceHostAddresses {

    private String name;
    private Map<HostAddress, List<DataSourceNumber>> addresses;
    private long lastTime = 0;

    public SourceHostAddresses(String name, Map<HostAddress, List<DataSourceNumber>> addresses) {
        this.name = name;
        this.addresses = addresses;
    }

    public long getLastTime() {
        return lastTime;
    }

    public void setLastTime(long lastTime) {
        this.lastTime = lastTime;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<HostAddress, List<DataSourceNumber>> getAddresses() {
        return addresses;
    }

    public void setAddresses(Map<HostAddress, List<DataSourceNumber>> addresses) {
        this.addresses = addresses;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final SourceHostAddresses other = (SourceHostAddresses) obj;
        if (this.name == null) {
            return false;
        }
        return this.name.equals(other.getName());
    }

}
