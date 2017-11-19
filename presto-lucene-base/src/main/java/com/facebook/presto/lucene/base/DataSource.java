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

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DataSource {

    private String type;
    private String name;
    private int nodes;
    private boolean allowConcurrentPerNode;
    private String className;
    private Map<String, Object> properties;

    public DataSource() {
    };

    @JsonCreator
    public DataSource(@JsonProperty("type") String type, @JsonProperty("name") String name,
            @JsonProperty("nodes") int nodes, @JsonProperty("allowConcurrentPerNode") boolean allowConcurrentPerNode,
            @JsonProperty("className") String className, @JsonProperty("properties") Map<String, Object> properties) {
        this.type = requireNonNull(type, "type is null");
        this.name = requireNonNull(name, "name is null");
        this.nodes = nodes;
        this.allowConcurrentPerNode = allowConcurrentPerNode;
        this.className = className;
        this.properties = requireNonNull(properties, "properties is null");
    }

    @JsonProperty
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @JsonProperty
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty
    public int getNodes() {
        return nodes;
    }

    public void setNodes(int nodes) {
        this.nodes = nodes;
    }

    @JsonProperty
    public boolean isAllowConcurrentPerNode() {
        return allowConcurrentPerNode;
    }

    public void setAllowConcurrentPerNode(boolean allowConcurrentPerNode) {
        this.allowConcurrentPerNode = allowConcurrentPerNode;
    }

    @JsonProperty
    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    @JsonProperty
    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

}
