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
package com.facebook.presto.storage.writer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.util.IOUtils;

public class JSONWritable implements WritableComparable<JSONObject> {
	
	private JSONObject json;
	
	public JSONWritable() {
	}
	
	public JSONWritable(JSONObject json) {
		this.json = json;
	}

	public JSONObject getJson() {
		return json;
	}

	public void setJson(JSONObject json) {
		this.json = json;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		byte[] buf = JSON.toJSONString(json).getBytes();
		out.writeInt(buf.length);
		out.write(buf);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int l = in.readInt();
		byte[] buff = new byte[l];
		in.readFully(buff);
		json = (JSONObject) JSON.parse(new String(buff));
	}

	@Override
	public int compareTo(JSONObject o) {
		return 0;
	}
}
