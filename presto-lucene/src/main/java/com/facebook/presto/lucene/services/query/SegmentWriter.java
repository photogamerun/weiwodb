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
package com.facebook.presto.lucene.services.query;
/**
 * segmentWriter
 * 
 * 仅仅对外暴露写数据的方法，后期会将hash方法去除
 * 
 * @author peter.wei
 *
 */
public interface SegmentWriter {

	public long hash(int docid);

	public Object[] write(Object[] row);

	public boolean write(Object[] row, int docid);
}
