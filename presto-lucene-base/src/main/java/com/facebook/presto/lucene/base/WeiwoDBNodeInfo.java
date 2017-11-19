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

import static java.util.Objects.requireNonNull;

import java.util.Objects;

import com.facebook.presto.spi.HostAddress;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class WeiwoDBNodeInfo {

    private final String nodeId;
    private final WeiwoDBMemoryInfo memInfo;
    private final HostAddress address;
    private final String uri;

    @JsonCreator
    public WeiwoDBNodeInfo(@JsonProperty("nodeId") String nodeId, @JsonProperty("memInfo") WeiwoDBMemoryInfo memInfo,
            @JsonProperty("address") HostAddress address, @JsonProperty("uri") String uri) {
        this.nodeId = requireNonNull(nodeId, "nodeId is null");
        this.memInfo = requireNonNull(memInfo, "memInfo is null");
        this.address = requireNonNull(address, "address is null");
        this.uri = requireNonNull(uri, "memInfo is null");
    }

    @JsonProperty
    public HostAddress getAddress() {
        return address;
    }

    @JsonProperty
    public String getUri() {
        return uri;
    }

    @JsonProperty
    public String getNodeId() {
        return nodeId;
    }

    @JsonProperty
    public WeiwoDBMemoryInfo getMemInfo() {
        return memInfo;
    }

    @Override
    public String toString() {
        return "NodeId=" + nodeId + "_HostAddress=" + address;
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, address.getHostText(), address.getPort());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final WeiwoDBNodeInfo other = (WeiwoDBNodeInfo) obj;
        if (other.getNodeId() == null || other.getAddress() == null) {
            return false;
        }
        return (other.getNodeId().equals(this.getNodeId()) && other.getAddress().equals(this.getAddress()));
    }

}
