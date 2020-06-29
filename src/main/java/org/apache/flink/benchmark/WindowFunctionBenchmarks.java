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

package org.apache.flink.benchmark;

import org.apache.flink.benchmark.functions.IntLongApplications;
import org.apache.flink.benchmark.functions.IntegerLongSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.contrib.streaming.state.RocksDBOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.IOException;

import static org.openjdk.jmh.annotations.Scope.Thread;

@OperationsPerInvocation(value = WindowFunctionBenchmarks.RECORDS_PER_INVOCATION)
public class WindowFunctionBenchmarks extends StateBackendBenchmarkBase {

	public static final int RECORDS_PER_INVOCATION = 100_000;

	public static void main(String[] args)
			throws RunnerException {
		Options options = new OptionsBuilder()
				.verbosity(VerboseMode.NORMAL)
				.include(".*" + WindowFunctionBenchmarks.class.getCanonicalName() + ".*")
				.build();

		new Runner(options).run();
	}

//	@Benchmark
//	public void globalWindow(TimeWindowContext context) throws Exception {
//		IntLongApplications.reduceWithWindowProcessFunction(context.source, GlobalWindows.create());
//		context.execute();
//	}

	@Benchmark
	public void tumblingWindow(TimeWindowContext context) throws Exception {
		IntLongApplications.reduceWithWindowProcessFunction(context.source, TumblingEventTimeWindows.of(Time.seconds(10_000)));
		context.execute();
	}

	@Benchmark
	public void slidingWindow(TimeWindowContext context) throws Exception {
		IntLongApplications.reduceWithWindowProcessFunction(context.source, SlidingEventTimeWindows.of(Time.seconds(10_000), Time.seconds(1000)));
		context.execute();
	}

//	@Benchmark
//	public void sessionWindow(TimeWindowContext context) throws Exception {
//		IntLongApplications.reduceWithWindowProcessFunction(context.source, EventTimeSessionWindows.withGap(Time.seconds(500)));
//		context.execute();
//	}

	@State(Thread)
	public static class TimeWindowContext extends StateBackendContext {
		@Param({"ROCKS_INC"})
		public StateBackend stateBackend = StateBackend.MEMORY;

		@Setup
		@Override
		public void setUp() throws IOException {
			super.setUp(stateBackend, RECORDS_PER_INVOCATION);
		}

		@TearDown
		@Override
		public void tearDown() throws IOException {
			super.tearDown();
		}

		@Override
		protected Configuration createConfiguration() {
			Configuration configuration = super.createConfiguration();
			// explicit set the managed memory as 322122552 bytes, which is the default managed memory of 1GB TM with 1 slot.
			configuration.set(RocksDBOptions.FIX_PER_SLOT_MEMORY_SIZE, MemorySize.parse("322122552b"));
			return configuration;
		}
	}
}
