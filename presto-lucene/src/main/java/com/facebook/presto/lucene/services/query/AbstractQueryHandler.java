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

import static java.util.Objects.requireNonNull;

import java.io.IOException;

import org.apache.lucene.search.Query;

import com.facebook.presto.spi.pushdown.PushDown;
import com.facebook.presto.spi.pushdown.QueryTransfer;
import com.facebook.presto.sql.tree.Expression;
/**
 * 
 * 抽象handler 用来组合一个可回归的责任链用于处理下推逻辑。
 * 
 * 下推链条包括： 过滤语法查询： 分组语法查询，分组聚合语法查询。
 * 
 * @see com.facebook.presto.lucene.services.query.internals.StandardQueryHandler
 * @see com.facebook.presto.lucene.services.query.internals.GroupAggregationQueryHandler
 * @see com.facebook.presto.lucene.services.query.internals.AggregationQueryHandler
 * 
 * @author peter.wei
 *
 */
public abstract class AbstractQueryHandler implements QueryHandler {

	private AbstractQueryHandler successor;

	protected final QueryTransfer<Expression, Query> transfer;

	protected AbstractQueryHandler(QueryTransfer<Expression, Query> transfer) {
		this.transfer = requireNonNull(transfer, "transfer is null");
	}

	@Override
	public WeiwoCollector handle(PushDown pushdown) throws IOException {
		if (ishandover(pushdown)) {
			return this.getSuccessor().handle(pushdown);
		} else {
			return handlenow(pushdown);
		}
	}

	public abstract WeiwoCollector handlenow(PushDown pushdown)
			throws IOException;

	public abstract boolean ishandover(PushDown pushdown);

	public AbstractQueryHandler setSuccessor(AbstractQueryHandler successor) {
		this.successor = successor;
		return successor;
	}

	public AbstractQueryHandler getSuccessor() {
		return successor;
	}
}