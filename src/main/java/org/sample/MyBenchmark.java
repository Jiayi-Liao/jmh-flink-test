/*
 * Copyright (c) 2005, 2014, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

package org.sample;

import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverTopology;
import org.apache.flink.runtime.executiongraph.failover.flip1.PipelinedRegionComputeUtil;
import org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease.PipelinedRegion;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.*;
import org.openjdk.jmh.annotations.*;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Warmup(iterations = 10)
@Measurement(iterations = 10)
@Fork(value = 3)
@State(Scope.Thread)
public class MyBenchmark {

    @Param({"1000", "3000", "5000"})
    public String distinctRegionsCount;

    public Set<? extends Set<? extends SchedulingExecutionVertex<?, ?>>> distinctRegions;

    @Setup(Level.Invocation)
    public void setUp() {
        distinctRegions = buildDistinctRegions(Integer.parseInt(distinctRegionsCount));
    }

    @Benchmark
    public void testStream() {
        distinctRegions.stream()
                .map(toExecutionVertexIdSet1())
                .map(PipelinedRegion::from)
                .collect(Collectors.toSet());
    }

    @Benchmark
    public void testNonStream() {
        final Set<PipelinedRegion> regions = new HashSet<>();
        for (Set<? extends SchedulingExecutionVertex<?, ?>> region : distinctRegions) {
            regions.add(PipelinedRegion.from(toExecutionVertexIdSet2(region)));
        }
    }

    public static Set<ExecutionVertexID> toExecutionVertexIdSet2(Set<? extends SchedulingExecutionVertex<?, ?>> failoverVertices) {
        final Set<ExecutionVertexID> ids = new HashSet<>();
        for (SchedulingExecutionVertex<?, ?> vertex : failoverVertices) {
            ids.add(vertex.getId());
        }
        return ids;
    }

    private static Function<Set<? extends SchedulingExecutionVertex<?, ?>>, Set<ExecutionVertexID>> toExecutionVertexIdSet1() {
        return failoverVertices -> failoverVertices.stream()
                .map(SchedulingExecutionVertex::getId)
                .collect(Collectors.toSet());
    }

    // 5000 * 5000 blocking
    public static Set<? extends Set<? extends SchedulingExecutionVertex<?, ?>>> buildDistinctRegions(int parallelism) {
        return PipelinedRegionComputeUtil.computePipelinedRegions(buildComplexTopology(parallelism));
    }

    public static TestingSchedulingTopology buildComplexTopology(int parallelism) {
        final TestingSchedulingTopology testingSchedulingTopology = new TestingSchedulingTopology();

        final List<TestingSchedulingExecutionVertex> producers = testingSchedulingTopology.addExecutionVertices().withParallelism(parallelism).finish();
        final List<TestingSchedulingExecutionVertex> consumers = testingSchedulingTopology.addExecutionVertices().withParallelism(parallelism).finish();
        final List<TestingSchedulingResultPartition> resultPartitions = testingSchedulingTopology.connectPointwise(producers, consumers).finish();

        return testingSchedulingTopology;
    }
}
