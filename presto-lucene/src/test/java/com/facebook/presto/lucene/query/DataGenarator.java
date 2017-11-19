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
package com.facebook.presto.lucene.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSONObject;
import com.facebook.presto.lucene.base.util.WeiwoDBConfigureKeys;

public class DataGenarator {

    public static void main(String[] args) {
        JSONObject json = new JSONObject();
        Map<String, Object> map = new HashMap<>();
        map.put("id", 0);
        map.put("name", "chenfolin");
        map.put("age", 18);
        Map<String, Object> map2 = new HashMap<>();
        map2.put("id", 1);
        map2.put("name", "chenfolin2");
        map2.put("age", 19);
        List<Map<String, Object>> list = new ArrayList<>();
        list.add(map);
        list.add(map2);
        json.put(WeiwoDBConfigureKeys.JSON_DB_KEY, "default");
        json.put(WeiwoDBConfigureKeys.JSON_TABLE_KEY, "weiwo_test3");
        json.put(WeiwoDBConfigureKeys.JSON_PARTITION_KEY, "20161209");
        json.put(WeiwoDBConfigureKeys.JSON_DATAS_KEY, list);
        System.out.println(json.toJSONString());
    }

}
