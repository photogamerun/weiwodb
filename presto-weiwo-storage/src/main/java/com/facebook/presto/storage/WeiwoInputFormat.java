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
package com.facebook.presto.storage;

import java.io.IOException;

import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.facebook.presto.storage.writer.JSONWritable;

/**
 * Created by wally.hong on 2017/2/28.
 */
public class WeiwoInputFormat  extends HiveInputFormat<Text, JSONWritable>
{
	@Override
	public RecordReader<Text,JSONWritable> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
		return new WeiwoRecordReader();
	}
	
	
	static class WeiwoRecordReader implements RecordReader<Text, JSONWritable>
	{

		@Override
		public boolean next(Text key, JSONWritable value) throws IOException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public Text createKey() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public JSONWritable createValue() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public long getPos() throws IOException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public float getProgress() throws IOException {
			// TODO Auto-generated method stub
			return 0;
		}
		
	}
}
