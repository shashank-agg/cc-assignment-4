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

// package org.apache.flink.examples.java.clustering;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

import java.io.Serializable;
import java.util.Collection;
import java.util.Arrays;
import java.util.List;

/**
 USAGE: 
 flink run cloud-assignment-kmeans-1.0-SNAPSHOT.jar --input berlin.csv --iterations 10 --mnc 1,6,78 --k 500 --output clusters.csv

 */
@SuppressWarnings("serial")
public class CellCluster {

	public static void main(String[] args) throws Exception {

		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);
		if (params.get("input") == null) {
			System.err.println("Specify 'input'");
			return;
		}
		int iterations = params.getInt("iterations", 10);
		String mncs = params.get("mnc");
		int noOfClusters = params.getInt("k", -1);

		// set up execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setGlobalJobParameters(params); // make parameters available in the web interface

		// get input data:
		// read the points and centroids from the provided paths or fall back to default data
		DataSet<Tower> allTowers = getAllTowers(params, env);
		DataSet<Tower> dataPoints = allTowers.filter(new FilterFunction<Tower>() {
			public boolean filter(Tower t) {return ((t.type).equals("GSM") || (t.type).equals("UMTS"));}
		});
		
		if (mncs != null) {
			final List<String> mncList = Arrays.asList(mncs.split(","));
			// System.out.println(mncList);
			dataPoints = dataPoints.filter(new FilterFunction<Tower>() {
				public boolean filter(Tower t) {return mncList.contains(Integer.toString(t.mnc));}
			});
		}
		
		DataSet<Tower> centroids = allTowers.filter(new FilterFunction<Tower>() {
			public boolean filter(Tower t) {return ((t.type).equals("LTE"));}
		});
		if(noOfClusters != -1 && noOfClusters < centroids.count()) {
			centroids = centroids.first(noOfClusters);
		}

		// set number of bulk iterations for KMeans algorithm
		IterativeDataSet<Tower> loop = centroids.iterate(iterations);

		DataSet<Tower> newCentroids = dataPoints
			// compute closest centroid for each point
			.map(new SelectNearestCenter()).withBroadcastSet(loop, "centroids")
			// count and sum point coordinates for each centroid
			.map(new CountAppender())
			.groupBy(0).reduce(new CentroidAccumulator())
			// compute new centroids from point counts and coordinate sums
			.map(new CentroidAverager());

		// feed new centroids back into next iteration
		DataSet<Tower> finalCentroids = loop.closeWith(newCentroids);

		DataSet<Tuple2<Integer, Tower>> clusteredPoints = dataPoints
			// assign points to final clusters
			.map(new SelectNearestCenter()).withBroadcastSet(finalCentroids, "centroids");

		// emit result
		if (params.has("output")) {
			clusteredPoints.writeAsCsv(params.get("output"), "\n", ",", WriteMode.OVERWRITE);

			// since file sinks are lazy, we trigger the execution explicitly
			env.execute("KMeans Example");
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			clusteredPoints.print();
		}
	}


	private static DataSet<Tower> getAllTowers(ParameterTool params, ExecutionEnvironment env) {
		DataSet<Tower> points;
			// read points from CSV file
		points = env.readCsvFile(params.get("input"))
			.ignoreFirstLine()
			.fieldDelimiter(",")
			.includeFields("10101011000000")
			.pojoType(Tower.class, "type", "mnc", "id", "x", "y");
		return points;
	}

	public static class Tower implements Serializable {
		public String type;
		public double x,y;
		public int id;
		public int mnc;

		public Tower() {}
	
		public Tower(String type, int id, int mnc, double x, double y) {
			this.type = type;
			this.id = id;
			this.x = x;
			this.y = y;
		}

		public double euclideanDistance(Tower other) {
			return Math.sqrt((x - other.x) * (x - other.x) + (y - other.y) * (y - other.y));
		}

		public Tower add(Tower other) {
			x += other.x;
			y += other.y;
			return this;
		}

		public Tower div(long val) {
			x /= val;
			y /= val;
			return this;
		}

		@Override
		public String toString() {
			return x + "," + y; 
		}

	}
	/** Determines the closest cluster center for a data point. */
	@ForwardedFields("*->1")
	public static final class SelectNearestCenter extends RichMapFunction<Tower, Tuple2<Integer, Tower>> {
		private Collection<Tower> centroids;

		/** Reads the centroid values from a broadcast variable into a collection. */
		@Override
		public void open(Configuration parameters) throws Exception {
			this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
		}

		@Override
		public Tuple2<Integer, Tower> map(Tower t) throws Exception {

			double minDistance = Double.MAX_VALUE;
			int closestCentroidId = -1;

			// check all cluster centers
			for (Tower centroid : centroids) {
				// compute distance
				double distance = t.euclideanDistance(centroid);

				// update nearest cluster if necessary
				if (distance < minDistance) {
					minDistance = distance;
					closestCentroidId = centroid.id;
				}
			}

			// emit a new record with the center id and the data point.
			return new Tuple2<Integer, Tower>(closestCentroidId, t);
		}
	}

	/** Appends a count variable to the tuple. */
	@ForwardedFields("f0;f1")
	public static final class CountAppender implements MapFunction<Tuple2<Integer, Tower>, Tuple3<Integer, Tower, Long>> {

		// @Override
		public Tuple3<Integer, Tower, Long> map(Tuple2<Integer, Tower> t) {
			return new Tuple3<Integer, Tower, Long>(t.f0, t.f1, 1L);
		}
	}

	/** Sums and counts point coordinates. */
	@ForwardedFields("0")
	public static final class CentroidAccumulator implements ReduceFunction<Tuple3<Integer, Tower, Long>> {

		// @Override
		public Tuple3<Integer, Tower, Long> reduce(Tuple3<Integer, Tower, Long> val1, Tuple3<Integer, Tower, Long> val2) {
			return new Tuple3<Integer, Tower, Long>(val1.f0, val1.f1.add(val2.f1), val1.f2 + val2.f2);
		}
	}

	/** Computes new centroid from coordinate sum and count of points. */
	@ForwardedFields("0->id")
	public static final class CentroidAverager implements MapFunction<Tuple3<Integer, Tower, Long>, Tower> {

		// @Override
		public Tower map(Tuple3<Integer, Tower, Long> value) {
			return new Tower("LTE", value.f0, 0, value.f1.x/value.f2, value.f1.y/value.f2);
		}
	}
}
