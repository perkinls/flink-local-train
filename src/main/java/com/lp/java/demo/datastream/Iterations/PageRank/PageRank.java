/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.lp.java.demo.datastream.Iterations.PageRank;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

import static org.apache.flink.api.java.aggregation.Aggregations.SUM;

/**
 算法会维护两个数据集：一个由（pageID，linkList）的元素组成，包含每个页面的相邻页面的列表；
 另一个由（pageID，rank）元素组成，包含每个页面的当前排序值。它按如下步骤进行计算：
 将每个页面的排序值初始化为1.0。
 在每次迭代中，对页面p，向其每个相邻页面（有直接链接的页面）发送一个值为rank(p)/numNeighbors(p)的贡献值。
 将每个页面的排序值设为0.15 + 0.85 * contributionsReceived。
 最后两个步骤会重复几个循环，在此过程中，算法会逐渐收敛于每个页面的实际PageRank值。在实际操作中，收敛通常需要大约10轮迭代。
 */
@SuppressWarnings("serial")
public class PageRank {

	private static final double DAMPENING_FACTOR = 0.85;
	private static final double EPSILON = 0.0001;

	// *************************************************************************
	//     PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);

		final int numPages = params.getInt("numPages", PageRankData.getNumberOfPages());
		final int maxIterations = params.getInt("iterations", 10);

		// set up execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// make the parameters available to the web ui
		env.getConfig().setGlobalJobParameters(params);

		// get input data
		DataSet<Long> pagesInput = getPagesDataSet(env, params);
		DataSet<Tuple2<Long, Long>> linksInput = getLinksDataSet(env, params);

		// 初始化rank <pageID,rank>
		DataSet<Tuple2<Long, Double>> pagesWithRanks = pagesInput.
				map(new RankAssigner((1.0d / numPages)));

		// 获取<pageID,NeighborsLinkList>
		DataSet<Tuple2<Long, Long[]>> adjacencyListInput =
				linksInput.groupBy(0).reduceGroup(new BuildOutgoingEdgeList());

		// set iterative data set
		IterativeDataSet<Tuple2<Long, Double>> iteration = pagesWithRanks.iterate(maxIterations);

		DataSet<Tuple2<Long, Double>> newRanks = iteration
				// join pages with outgoing edges and distribute rank
				.join(adjacencyListInput).where(0).equalTo(0).flatMap(new JoinVertexWithEdgesMatch())
				// collect and sum ranks
				.groupBy(0).aggregate(SUM, 1)
				// apply dampening factor
				.map(new Dampener(DAMPENING_FACTOR, numPages));

		DataSet<Tuple2<Long, Double>> finalPageRanks = iteration.closeWith(
				newRanks,
				newRanks.join(iteration).where(0).equalTo(0)
				// termination condition
				.filter(new EpsilonFilter())); //自定义了一个终止条件

		// emit result
		if (params.has("output")) {
			finalPageRanks.writeAsCsv(params.get("output"), "\n", " ");
			// execute program
			env.execute("Basic Page Rank Example");
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			finalPageRanks.print();
		}
	}

	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************

	/**
	 * A map function that assigns an initial rank to all pages.
	 */
	public static final class RankAssigner implements MapFunction<Long, Tuple2<Long, Double>> {
		Tuple2<Long, Double> outPageWithRank;

		public RankAssigner(double rank) {
			this.outPageWithRank = new Tuple2<Long, Double>(-1L, rank);
		}

		@Override
		public Tuple2<Long, Double> map(Long page) {
			outPageWithRank.f0 = page;
			return outPageWithRank;
		}
	}

	/**
	 * A reduce function that takes a sequence of edges and builds the adjacency list for the vertex where the edges
	 * originate. Run as a pre-processing step.
	 */
	@ForwardedFields("0")
	public static final class BuildOutgoingEdgeList implements GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long[]>> {

		private final ArrayList<Long> neighbors = new ArrayList<Long>();

		@Override
		public void reduce(Iterable<Tuple2<Long, Long>> values, Collector<Tuple2<Long, Long[]>> out) {
			neighbors.clear();
			Long id = 0L;

			for (Tuple2<Long, Long> n : values) {
				id = n.f0;
				neighbors.add(n.f1);
			}
			out.collect(new Tuple2<Long, Long[]>(id, neighbors.toArray(new Long[neighbors.size()])));
		}
	}

	/**
	 * Join function that distributes a fraction of a vertex's rank to all neighbors.
	 */
	public static final class JoinVertexWithEdgesMatch implements FlatMapFunction<Tuple2<Tuple2<Long, Double>, Tuple2<Long, Long[]>>, Tuple2<Long, Double>> {

		@Override
		public void flatMap(Tuple2<Tuple2<Long, Double>, Tuple2<Long, Long[]>> value, Collector<Tuple2<Long, Double>> out){
			Long[] neighbors = value.f1.f1;
			double rank = value.f0.f1;
			double rankToDistribute = rank / ((double) neighbors.length);

			for (Long neighbor: neighbors) {
				out.collect(new Tuple2<Long, Double>(neighbor, rankToDistribute));
			}
		}
	}

	/**
	 * The function that applies the page rank dampening formula.
	 */
	@ForwardedFields("0")
	public static final class Dampener implements MapFunction<Tuple2<Long, Double>, Tuple2<Long, Double>> {

		private final double dampening;
		private final double randomJump;

		public Dampener(double dampening, double numVertices) {
			this.dampening = dampening;
			this.randomJump = (1 - dampening) / numVertices;
		}

		@Override
		public Tuple2<Long, Double> map(Tuple2<Long, Double> value) {
			value.f1 = (value.f1 * dampening) + randomJump;
			return value;
		}
	}

	/**
	 * Filter that filters vertices where the rank difference is below a threshold.
	 */
	public static final class EpsilonFilter implements FilterFunction<Tuple2<Tuple2<Long, Double>, Tuple2<Long, Double>>> {

		@Override
		public boolean filter(Tuple2<Tuple2<Long, Double>, Tuple2<Long, Double>> value) {
			return Math.abs(value.f0.f1 - value.f1.f1) > EPSILON;
		}
	}

	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	private static DataSet<Long> getPagesDataSet(ExecutionEnvironment env, ParameterTool params) {
		if (params.has("pages")) {
			return env.readCsvFile(params.get("pages"))
				.fieldDelimiter(" ")
				.lineDelimiter("\n")
				.types(Long.class)
				.map(new MapFunction<Tuple1<Long>, Long>() {
					@Override
					public Long map(Tuple1<Long> v) {
						return v.f0;
					}
				});
		} else {
			System.out.println("Executing PageRank example with default pages data set.");
			System.out.println("Use --pages to specify file input.");
			return PageRankData.getDefaultPagesDataSet(env);
		}
	}

	private static DataSet<Tuple2<Long, Long>> getLinksDataSet(ExecutionEnvironment env, ParameterTool params) {
		if (params.has("links")) {
			return env.readCsvFile(params.get("links"))
				.fieldDelimiter(" ")
				.lineDelimiter("\n")
				.types(Long.class, Long.class);
		} else {
			System.out.println("Executing PageRank example with default links data set.");
			System.out.println("Use --links to specify file input.");
			return PageRankData.getDefaultEdgeDataSet(env);
		}
	}
}
