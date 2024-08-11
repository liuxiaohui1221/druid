/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.materializedview;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.Tuple3;
import junit.framework.AssertionFailedError;
import org.apache.druid.client.materializedview.ClientTaskGranularitySpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.error.EntryAlreadyExists;
import org.apache.druid.indexer.HadoopIOConfig;
import org.apache.druid.indexer.HadoopIngestionSpec;
import org.apache.druid.indexer.HadoopTuningConfig;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexing.common.RetryPolicyConfig;
import org.apache.druid.indexing.common.RetryPolicyFactory;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.indexing.common.actions.LocalTaskActionClientFactory;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.task.AbstractTask;
import org.apache.druid.indexing.common.task.HadoopIndexTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.indexing.common.task.materializedview.MaterializedViewTask;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.RemoteTaskRunner;
import org.apache.druid.indexing.overlord.TaskLockConfigTest;
import org.apache.druid.indexing.overlord.TaskLockbox;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskQueue;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.config.TaskLockConfig;
import org.apache.druid.indexing.overlord.config.TaskQueueConfig;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManagerConfig;
import org.apache.druid.indexing.test.TestIndexerMetadataStorageCoordinator;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import org.apache.druid.metadata.MetadataSupervisorManager;
import org.apache.druid.metadata.SqlSegmentsMetadataManager;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.metadata.SegmentSchemaManager;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.timeline.BaseShardSpecsSpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.MaterializedDataSegment;
import org.apache.druid.timeline.MaterializedSpec;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static it.unimi.dsi.fastutil.ints.IntIterators.any;
import static org.apache.druid.indexing.materializedview.MaterializedViewSupervisorSpecTest.BASE_DATA_SOURCE;
import static org.apache.druid.indexing.materializedview.MaterializedViewSupervisorSpecTest.RETRY_POLICY_FACTORY;
import static org.apache.druid.query.QueryRunnerTestHelper.DATA_SOURCE;

public class MaterializedViewSupervisorTest extends TaskLockConfigTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule
      = new TestDerbyConnector.DerbyConnectorRule();

  private TaskStorage taskStorage;
  private TaskMaster taskMaster;
  private IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  private MetadataSupervisorManager metadataSupervisorManager;
  private SqlSegmentsMetadataManager sqlSegmentsMetadataManager;
  private TaskQueue taskQueue;
  private MaterializedViewSupervisor supervisor;
  private String derivativeDatasourceName = "derivative";
  private MaterializedViewSupervisorSpec spec;
  private final Map<String, Object> context = new HashMap<>();
  private final ObjectMapper objectMapper = TestHelper.makeJsonMapper();
  private SegmentSchemaManager segmentSchemaManager;
  private SegmentCacheManagerFactory segmentCacheManagerFactory;
  private MaterializedViewTaskConfig materializedViewTaskConfig;
  //指定segment物化区间范围
  private Period ingestDuration = PolicyConfig.DEFAULT_INGESTION_DURATION;

  @Before
  public void setUp()
  {
    TestDerbyConnector derbyConnector = derbyConnectorRule.getConnector();
    derbyConnector.createDataSourceTable();
    derbyConnector.createSegmentSchemasTable();
    derbyConnector.createSegmentTable();
    taskStorage = EasyMock.createMock(TaskStorage.class);
    taskMaster = EasyMock.createMock(TaskMaster.class);
    segmentSchemaManager = new SegmentSchemaManager(
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        objectMapper,
        derbyConnector
    );
    segmentCacheManagerFactory = new SegmentCacheManagerFactory(objectMapper);
    indexerMetadataStorageCoordinator = new IndexerSQLMetadataStorageCoordinator(
        objectMapper,
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        derbyConnector,
        segmentSchemaManager,
        CentralizedDatasourceSchemaConfig.create()
    );
    metadataSupervisorManager = EasyMock.createMock(MetadataSupervisorManager.class);
    sqlSegmentsMetadataManager = EasyMock.createMock(SqlSegmentsMetadataManager.class);
    taskQueue = EasyMock.createMock(TaskQueue.class);
    taskQueue.start();
    materializedViewTaskConfig = new MaterializedViewTaskConfig();
    objectMapper.registerSubtypes(new NamedType(HashBasedNumberedShardSpec.class, "hashed"));
    context.put("maxTaskCount", 5);

    spec = new NativeBatchMaterializedViewSupervisorSpec(
        "base",
        new DimensionsSpec(Collections.singletonList(new StringDimensionSchema("dim"))),
        new AggregatorFactory[]{new LongSumAggregatorFactory("m1", "m1")},
        new ClientTaskGranularitySpec(Granularities.HOUR, Granularities.HOUR, null),
        null,
        DATA_SOURCE,
        new PolicyConfig(null, null, null, ingestDuration, null, null, null),
        context,
        false,
        objectMapper,
        taskMaster,
        taskStorage,
        metadataSupervisorManager,
        sqlSegmentsMetadataManager,
        indexerMetadataStorageCoordinator,
        materializedViewTaskConfig,
        EasyMock.createMock(AuthorizerMapper.class),
        EasyMock.createMock(ChatHandlerProvider.class),
        new SupervisorStateManagerConfig(),
        segmentCacheManagerFactory,
        new RetryPolicyFactory(new RetryPolicyConfig())
    );
    derivativeDatasourceName = spec.getDataSourceName();
    supervisor = (MaterializedViewSupervisor) spec.createSupervisor();
  }

  @Test
  public void testClearIntervalCacheTimeout()
  {
    final Map<Interval, AtomicLong> intervals = new ConcurrentHashMap<>();
    intervals.put(
        Intervals.of("2022-01-01/2022-01-02"),
        new AtomicLong(System.currentTimeMillis()
                       - materializedViewTaskConfig.getTaskCheckDuration().toStandardDuration().getMillis()
                       - 2000)
    );
    intervals.put(
        Intervals.of("2022-01-02/2022-01-03"),
        new AtomicLong(System.currentTimeMillis())
    );
    supervisor.clearIntervalCacheTimeout(intervals);
    Assert.assertEquals(1, intervals.size());
    Assert.assertEquals(true, intervals.containsKey(Intervals.of("2022-01-02/2022-01-03")));
  }

  // @Test
  public void testReachCacheTimeout() throws InterruptedException
  {
    String cacheInterval = "2022-01-01/2022-01-02";
    supervisor.reachCacheTimeout(Intervals.of(cacheInterval));
    Thread.sleep(1001);
    Assert.assertEquals(false, supervisor.reachCacheTimeout(Intervals.of(cacheInterval)));
    Thread.sleep(supervisor.getConfig().getTaskCheckDuration().toStandardDuration().getMillis());
    Assert.assertEquals(true, supervisor.reachCacheTimeout(Intervals.of(cacheInterval)));
  }

  @Test
  public void testMapDifference()
  {
    Map<String, List<DataSegment>> ds1 = new HashMap<>();
    Map<String, List<DataSegment>> ds2 = new HashMap<>();
    ds1.put("1", createBaseSegments());
    ds2.put("1", createBaseSegments());

    MapDifference<String, List<DataSegment>> difference = Maps.difference(ds1, ds2);
    Map<String, MapDifference.ValueDifference<List<DataSegment>>> stringValueDifferenceMap = difference.entriesDiffering();
    Assert.assertEquals(0, stringValueDifferenceMap.size());

    Map<Interval, String> ver1 = new HashMap<>();
    Map<Interval, String> ver2 = new HashMap<>();
    ver1.put(Intervals.of("2015-02-02T01/2015-02-02T02"), "test_ver2");
    ver1.put(Intervals.of("2015-01-01T01/2015-01-01T02"), "test_ver1");
    ver1.put(Intervals.of("2015-01-01T02/2015-01-01T03"), "test_ver2");
    ver2.put(Intervals.of("2015-01-01T01/2015-01-01T02"), "test_ver2");
    ver2.put(Intervals.of("2015-01-01T02/2015-01-01T03"), "test_ver2");

    MapDifference<Interval, String> difference2 = Maps.difference(ver1, ver2);
    Map<Interval, String> intervalStringMap = difference2.entriesOnlyOnLeft();
    Assert.assertEquals(1, intervalStringMap.size());
  }

  @Test
  public void testCheckSegmentsForHadoopHistory() throws IOException
  {
    DataSegment expectedSegment1 = new DataSegment(
        "base",
        Intervals.of("2015-01-03T00Z/2015-01-04T00Z"),
        "2015-01-04",
        ImmutableMap.of(),
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("m1"),
        new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null, null),
        9,
        1024
    );
    DataSegment expectedSegment2 = new DataSegment(
        "base",
        Intervals.of("2014-12-30T00Z/2014-12-31T00Z"),
        "2014-12-30",
        ImmutableMap.of(),
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("m1"),
        new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null, null),
        9,
        1024
    );
    DataSegment expectedSegment3 = new DataSegment(
        "base",
        Intervals.of("2015-01-04T00Z/2015-01-05T00Z"),
        "2015-01-04",
        ImmutableMap.of(),
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("m1"),
        new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null, null),
        9,
        1024
    );
    Set<DataSegment> baseSegments = Sets.newHashSet(
        expectedSegment1,
        expectedSegment2,
        expectedSegment3,
        new DataSegment(
            "base",
            Intervals.of("2015-01-01T00Z/2015-01-02T00Z"),
            "2015-01-02",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null, null),
            9,
            1024
        ),
        new DataSegment(
            "base",
            Intervals.of("2015-01-02T00Z/2015-01-03T00Z"),
            "2015-01-03",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(0, 2, 0, 2, null, null, null),
            9,
            1024
        ),
        new DataSegment(
            "base",
            Intervals.of("2015-01-02T00Z/2015-01-03T00Z"),
            "2015-01-03",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(1, 2, 1, 2, null, null, null),
            9,
            1024
        )
    );
    DataSegment historyDerivativeSegmentForHadoop = new DataSegment(
        derivativeDatasourceName,
        Intervals.of("2014-12-30T00Z/2014-12-31T00Z"),
        "2015-01-02",
        ImmutableMap.of(),
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("m1"),
        new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null, null),
        9,
        1024
    );
    Set<DataSegment> derivativeSegments = Sets.newHashSet(
        historyDerivativeSegmentForHadoop,
        new MaterializedDataSegment(
            derivativeDatasourceName,
            Intervals.of("2015-01-01T00Z/2015-01-02T00Z"),
            "2015-01-02",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null, null),
            null,
            9,
            1024,
            DataSegment.PruneSpecsHolder.DEFAULT,
            new MaterializedSpec(
                MaterializedSpec.TYPE_SAME_SEGMENT_GRAN,
                new BaseShardSpecsSpec(0, 1, "2015-01-02"),
                null,
                (short) 1
            )
        ),
        new MaterializedDataSegment(
            derivativeDatasourceName,
            Intervals.of("2015-01-02T00Z/2015-01-03T00Z"),
            "2022-01-01",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null, null),
            null,
            9,
            1024, DataSegment.PruneSpecsHolder.DEFAULT,
            new MaterializedSpec(
                MaterializedSpec.TYPE_SAME_SEGMENT_GRAN,
                new BaseShardSpecsSpec(
                    0,
                    1,
                    "2015-01-03"
                ),
                null, (short) 1
            )
        )
    );
    indexerMetadataStorageCoordinator.commitSegments(baseSegments, null);
    indexerMetadataStorageCoordinator.commitSegments(derivativeSegments, null);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    Pair<SortedMap<Interval, Pair<Boolean, String>>, Map<Interval, List<DataSegment>>> toBuildInterval = supervisor.checkSegments();
    Set<Interval> expectedToBuildInterval = Sets.newHashSet(
        Intervals.of("2015-01-04T00Z/2015-01-05T00Z"),
        Intervals.of("2015-01-03T00Z/2015-01-04T00Z"),
        Intervals.of("2015-01-02T00Z/2015-01-03T00Z")
        // Intervals.of("2014-12-30T00Z/2014-12-31T00Z")
    );
    Map<Interval, List<DataSegment>> expectedSegments = new HashMap<>();
    expectedSegments.put(
        Intervals.of("2015-01-02T00Z/2015-01-03T00Z"),
        Collections.singletonList(
            new DataSegment(
                "base",
                Intervals.of("2015-01-02T00Z/2015-01-03T00Z"),
                "2015-01-03",
                ImmutableMap.of(),
                ImmutableList.of("dim1", "dim2"),
                ImmutableList.of("m1"),
                new HashBasedNumberedShardSpec(1, 2, 1, 2, null, null, null),
                9,
                1024
            )
        )
    );
    expectedSegments.put(
        expectedSegment1.getInterval(),
        Collections.singletonList(
            expectedSegment1
        )
    );
    expectedSegments.put(
        expectedSegment2.getInterval(),
        Collections.singletonList(
            expectedSegment2
        )
    );
    expectedSegments.put(
        expectedSegment3.getInterval(),
        Collections.singletonList(
            expectedSegment3
        )
    );
    for (DataSegment segment : derivativeSegments) {
      byte[] bytes = objectMapper.writeValueAsBytes(segment);
      MaterializedDataSegment materializedDataSegment = objectMapper.readValue(bytes, MaterializedDataSegment.class);
      Assert.assertEquals(segment.getMaterializedSpec(), materializedDataSegment.getMaterializedSpec());
    }
    Assert.assertEquals(0, PolicyConfig.DEFAULT_SKIP_PERIOD_FROM_LATEST.getHours());
    Assert.assertEquals(expectedToBuildInterval, toBuildInterval.lhs.keySet());
    Assert.assertEquals(expectedSegments, toBuildInterval.rhs);
  }

  /**
   * 关闭第二段全量物化区间
   *
   * @throws IOException
   */
  @Test
  public void testCheckSegmentsForHourAndOnlyAppendMV() throws IOException
  {
    //关闭第二段全量物化区间
    Period skipPeriodFromLatest = Period.parse("P1D");
    MaterializedViewSupervisor supervisor = createSupervisor(false, skipPeriodFromLatest, Granularities.HOUR, false);
    String secondRangeInterval = "2014-12-29T00Z/2014-12-29T01Z";
    String secondRangeVersion = "2014-12-29";
    int materializedPartitionNum = 0;
    int unMaterializedPartitionNum = 1;
    //interval,version,partitionNum
    Map<String, Tuple2<String, Integer[]>> baseAllSegments = new HashMap<>();
    baseAllSegments.put("2014-12-29T00Z/2014-12-29T01Z", new Tuple2<>("2014-12-29", new Integer[]{0, 1}));
    baseAllSegments.put("2015-01-01T00Z/2015-01-01T01Z", new Tuple2<>("2015-01-02", new Integer[]{0, 1, 2, 3}));
    baseAllSegments.put("2015-01-02T01Z/2015-01-02T02Z", new Tuple2<>("2015-01-03", new Integer[]{0, 1}));
    baseAllSegments.put("2015-01-02T02Z/2015-01-02T03Z", new Tuple2<>("2015-01-02", new Integer[]{0}));
    baseAllSegments.put("2015-01-04T00Z/2015-01-04T01Z", new Tuple2<>("2015-01-04", new Integer[]{0, 1}));

    // expected to build segments and intervals
    Map<String, Tuple2<String, Integer[]>> expectedSegments = new HashMap<>();
    expectedSegments.put("2014-12-29T00Z/2014-12-29T01Z", new Tuple2<>("2014-12-29", new Integer[]{1}));
    expectedSegments.put("2015-01-01T00Z/2015-01-01T01Z", new Tuple2<>("2015-01-02", new Integer[]{2, 3}));
    expectedSegments.put("2015-01-02T01Z/2015-01-02T02Z", new Tuple2<>("2015-01-03", new Integer[]{1}));
//    baseAllSegments.put("2015-01-02T02Z/2015-01-02T03Z", new Tuple2<>("2015-01-02", new Integer[]{0}));
    expectedSegments.put("2015-01-04T00Z/2015-01-04T01Z", new Tuple2<>("2015-01-04", new Integer[]{0, 1}));
    // expected to build intervals
    Map<Interval, Pair<Boolean, String>> expectedToBuildInterval = new HashMap<>();
    expectedToBuildInterval.put(
        Intervals.of("2014-12-29T00Z/2014-12-29T01Z"),
        Pair.of(false, expectedSegments.get("2014-12-29T00Z/2014-12-29T01Z")._1)
    );
    expectedToBuildInterval.put(
        Intervals.of("2015-01-01T00Z/2015-01-01T01Z"),
        Pair.of(false, expectedSegments.get("2015-01-01T00Z/2015-01-01T01Z")._1)
    );
    expectedToBuildInterval.put(
        Intervals.of("2015-01-02T01Z/2015-01-02T02Z"),
        Pair.of(false, expectedSegments.get("2015-01-02T01Z/2015-01-02T02Z")._1)
    );
    //skip from latest P1D
//    expectedToBuildInterval.put(
//        Intervals.of("2015-01-04T00Z/2015-01-04T01Z"),
//        Pair.of(false, expectedSegments.get("2015-01-04T00Z/2015-01-04T01Z")._1)
//    );

    // ------already materialized segments---------
    Map<String, Map<Short, BaseShardSpecsSpec>> mvIntervals = new HashMap<>();
    // interval处于第二段物化区间，且存在物化过和没物化过的segment,并开启了第二段区间的overwrite
    Map<Short, BaseShardSpecsSpec> mvBaseSegs = new HashMap<>();
    mvBaseSegs.put(
        (short) 0, // intervalId 2014-12-29T00Z/2014-12-29T01Z
        new BaseShardSpecsSpec(0, 1, baseAllSegments.get("2014-12-29T00Z/2014-12-29T01Z")._1)
    );
    mvIntervals.put("2014-12-29T00Z/2014-12-30T00Z", mvBaseSegs);

    Map<Short, BaseShardSpecsSpec> multiSegmentGrans1 = new HashMap<>();
    multiSegmentGrans1.put(
        (short) 0, // intervalId 2015-01-01T00Z/2015-01-01T01Z 0,1
        new BaseShardSpecsSpec(0, 2, baseAllSegments.get("2015-01-01T00Z/2015-01-01T01Z")._1)
    );
    mvIntervals.put("2015-01-01T00Z/2015-01-02T00Z", multiSegmentGrans1);

    Map<Short, BaseShardSpecsSpec> multiSegmentGrans2 = new HashMap<>();
    // added due to same version
    multiSegmentGrans2.put(
        (short) 1, // 1 means 2015-01-02T01Z/2015-01-02T02Z
        new BaseShardSpecsSpec(0, 1, baseAllSegments.get("2015-01-02T01Z/2015-01-02T02Z")._1)
    );
    // need overwrite due to different version
    multiSegmentGrans2.put(
        (short) 2, // intervalId is 2 means 2015-01-02T02Z/2015-01-02T03Z
        new BaseShardSpecsSpec(0, 1, baseAllSegments.get("2015-01-02T02Z/2015-01-02T03Z")._1)
    );
    mvIntervals.put("2015-01-02T00Z/2015-01-03T00Z", multiSegmentGrans2);

    createExpectedToBuildSegments(unMaterializedPartitionNum);
    assertSegmentsForHour(supervisor, expectedSegments, expectedToBuildInterval, baseAllSegments,
                          mvIntervals
    );
  }

  /**
   * 开启第二段全量物化区间
   * 物化区间     [2014-12-27T01:00:00.000Z, 2015-01-03T01:00:00.000Z],
   * 全量物化区间 [2014-12-27T01:00:00.000Z, 2014-12-31T01:00:00.000Z],
   * 增量物化区间 [2014-12-31T01:00:00.000Z, 2015-01-03T01:00:00.000Z]
   *
   * @throws IOException
   */
  /*@Test
  public void testCheckSegmentsForHourAndEnableSecondOverwriteMV() throws IOException
  {
    //开启第二段全量物化区间
    MaterializedViewSupervisor supervisorEnableSecond = createSupervisor(false, Granularities.HOUR, true);
    String secondRangeInterval = "2014-12-29T00Z/2014-12-29T01Z";
    String secondRangeVersion = "2014-12-29";
    List<Pair<Boolean, String>> expectedSubmitTypeAndVersion = new ArrayList<>();
    expectedSubmitTypeAndVersion = new ArrayList<>();
    expectedSubmitTypeAndVersion.add(new Pair<>(true, "2022-02-02"));//Pair<isOverwrite, version>
    //version相同，属于第二段区间，且有新增segment，开启了全量物化，故isOverwrite=true。
    expectedSubmitTypeAndVersion.add(new Pair<>(true, secondRangeVersion));
    expectedSubmitTypeAndVersion.add(new Pair<>(false, "2015-01-03"));
    expectedSubmitTypeAndVersion.add(new Pair<>(false, "2015-01-02"));
    //由于开启了全量物化，secondRangeInterval属于全量物化区间，且该interval区间内存在新增的partitionNum=1号segment,
    // 所以materializedPartitionNum=0号segment需要重新提交物化。
    int materializedPartitionNum = 0;
    int unMaterializedPartitionNum = 1;
    int partitions = 2;
    Set<DataSegment> expectedSegments = createExpectedToBuildSegments(unMaterializedPartitionNum);
    expectedSegments.add(getDataSegmentByPartitionNum(materializedPartitionNum));

    assertSegmentsForHour(
        supervisorEnableSecond,
        expectedSegments,
        expectedSubmitTypeAndVersion,
        secondRangeInterval,
        secondRangeVersion,
        materializedPartitionNum
    );
  }*/
  private Set<DataSegment> createBaseSegments(Map<String, Tuple2<String, Integer[]>> intervalAndVersions)
  {
    Iterator<Map.Entry<String, Tuple2<String, Integer[]>>> iterator = intervalAndVersions.entrySet().iterator();
    Set<DataSegment> baseSegments = new HashSet<>();
    while (iterator.hasNext()) {
      Map.Entry<String, Tuple2<String, Integer[]>> next = iterator.next();
      Integer[] partNums = next.getValue()._2;
      for (int partitionNum : partNums) {
        baseSegments.add(new DataSegment(
            "base",
            Intervals.of(next.getKey()),
            next.getValue()._1,
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new NumberedShardSpec(partitionNum, 0),
            9,
            1024
        ));
      }
    }
    return baseSegments;
  }

  private void assertSegmentsForHour(
      MaterializedViewSupervisor supervisor,
      Map<String, Tuple2<String, Integer[]>> expectedIntervalAndVersions,
      Map<Interval, Pair<Boolean, String>> expectedToBuildInterval,
      Map<String, Tuple2<String, Integer[]>> allIntervalsAndVersions,
      Map<String, Map<Short, BaseShardSpecsSpec>> mvIntervals
  ) throws IOException
  {
    Set<DataSegment> baseSegments = createBaseSegments(allIntervalsAndVersions);
    Set<DataSegment> expectedSegments = createBaseSegments(expectedIntervalAndVersions);


    Set<DataSegment> derivativeSegments = new HashSet<>();
    Iterator<Map.Entry<String, Map<Short, BaseShardSpecsSpec>>> iterator = mvIntervals.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, Map<Short, BaseShardSpecsSpec>> next = iterator.next();
      derivativeSegments.add(new MaterializedDataSegment(
          derivativeDatasourceName,
          Intervals.of(next.getKey()),
          "2022-02-22",
          ImmutableMap.of(),
          ImmutableList.of("dim1", "dim2"),
          ImmutableList.of("m1"),
          new NumberedShardSpec(0, 0),
          null,
          9,
          1024,
          new MaterializedSpec(
              MaterializedSpec.TYPE_DIFF_SEGMENT_GRAN,
              null,
              next.getValue(),
              (short) 24
          )
      ));
    }

    indexerMetadataStorageCoordinator.commitSegments(baseSegments, null);
    indexerMetadataStorageCoordinator.commitSegments(derivativeSegments, null);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    Pair<SortedMap<Interval, Pair<Boolean, String>>, Map<Interval, List<DataSegment>>> toBuildInterval = supervisor.checkSegments();

    SortedMap<Interval, Pair<Boolean, String>> actualToBuildIntervals = toBuildInterval.lhs;

    Assert.assertEquals(expectedToBuildInterval.keySet(), toBuildInterval.lhs.keySet());

    Assert.assertEquals(
        expectedToBuildInterval,
        actualToBuildIntervals
    );

    assertEqualsForSegmentsOverwrite(
        expectedToBuildInterval.keySet(),
        expectedSegments,
        toBuildInterval.rhs
    );

  }

  @Test
  public void testCheckSegmentsForHourOvershodow() throws IOException
  {
    MaterializedViewSupervisor supervisor = createSupervisor(false, Period.parse("P1D"), Granularities.HOUR, true);
    String oldVersion1 = "2015-01-01";

    Set<DataSegment> baseSegments = Sets.newHashSet(

        new DataSegment(//used but overshadowed
                        "base",
                        Intervals.of("2015-01-01T00Z/2015-01-01T01Z"),
                        "2015-01-02",
                        ImmutableMap.of(),
                        ImmutableList.of("dim1", "dim2"),
                        ImmutableList.of("m1"),
                        new HashBasedNumberedShardSpec(0, 2, 0, 2, null, null, null),
                        9,
                        1024
        ),
        new DataSegment(//used but overshadowed
                        "base",
                        Intervals.of("2015-01-01T00Z/2015-01-01T01Z"),
                        "2015-01-02",
                        ImmutableMap.of(),
                        ImmutableList.of("dim1", "dim2"),
                        ImmutableList.of("m1"),
                        new HashBasedNumberedShardSpec(1, 2, 1, 2, null, null, null),
                        9,
                        1024
        ),
        new DataSegment(//overshadowed
                        "base",
                        Intervals.of("2015-01-02T00Z/2015-01-02T01Z"),
                        oldVersion1,
                        ImmutableMap.of(),
                        ImmutableList.of("dim1", "dim2"),
                        ImmutableList.of("m1"),
                        new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null, null),
                        9,
                        1024
        ),
        new DataSegment(
            "base",
            Intervals.of("2015-01-04T00Z/2015-01-04T01Z"),
            "2015-01-04",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null, null),
            9,
            1024
        )
    );
    String materialiedOldAndNewVersion1 = "2022-01-01";
    String newVersion2 = "2022-02-03";

    Set<DataSegment> expectedToBuilds = Sets.newHashSet(
        new DataSegment(//used and overshadow other version
                        "base",
                        Intervals.of("2015-01-01T00Z/2015-01-01T01Z"),
                        newVersion2,
                        ImmutableMap.of(),
                        ImmutableList.of("dim1", "dim2"),
                        ImmutableList.of("m1"),
                        new HashBasedNumberedShardSpec(
                            0,
                            1,
                            0,
                            1,
                            null,
                            null,
                            null
                        ),
                        9,
                        1024
        ), new DataSegment(//used and materialized old version
                           "base",
                           Intervals.of("2015-01-02T00Z/2015-01-02T01Z"),
                           materialiedOldAndNewVersion1,
                           ImmutableMap.of(),
                           ImmutableList.of("dim1", "dim2"),
                           ImmutableList.of("m1"),
                           new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null, null),
                           9,
                           1024
        ));
    baseSegments.addAll(expectedToBuilds);

    // already materialized segments
    Set<DataSegment> derivativeSegments = createDerivativeSegments(oldVersion1);
    indexerMetadataStorageCoordinator.commitSegments(baseSegments, null);
    indexerMetadataStorageCoordinator.commitSegments(derivativeSegments, null);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    Pair<SortedMap<Interval, Pair<Boolean, String>>, Map<Interval, List<DataSegment>>> toBuildInterval = supervisor.checkSegments();
    Set<Interval> expectedToBuildInterval = Sets.newHashSet(
        Intervals.of("2015-01-01T00Z/2015-01-01T01Z"),
        Intervals.of("2015-01-02T00Z/2015-01-02T01Z")
    );

    List<Pair<Boolean, String>> expectedSubmitTypeAndVersion = new ArrayList<>();
    expectedSubmitTypeAndVersion.add(new Pair<>(true, materialiedOldAndNewVersion1));
    expectedSubmitTypeAndVersion.add(new Pair<>(false, newVersion2));
    List<Pair<Boolean, String>> actualSubmitTypeAndVersion = new ArrayList<>(toBuildInterval.lhs.values());
    actualSubmitTypeAndVersion.sort(Comparator.comparing(Pair::toString));
    expectedSubmitTypeAndVersion.sort(Comparator.comparing(Pair::toString));
    Assert.assertEquals(expectedToBuildInterval, toBuildInterval.lhs.keySet());
    assertEqualsForSegmentsOverwrite(
        expectedToBuildInterval,
        expectedToBuilds,
        toBuildInterval.rhs
    );
    Assert.assertEquals(
        expectedSubmitTypeAndVersion,
        actualSubmitTypeAndVersion
    );
  }

  @Test
  public void testGetMaterializedVersionAndBaseSegments()
  {
    //hour
    String hourInterval = "2015-01-02T00Z/2015-01-02T01Z";
    String version = "2015-01-01";
    MaterializedViewSupervisor supervisor = createSupervisor(false, Period.parse("P1D"), Granularities.HOUR, true);
    Set<DataSegment> derivativeSegments = createDerivativeSegmentsForHour(
        hourInterval,
        version
    );

    Map<Interval, Pair<Boolean, String>> toBuildHistoryMvInterval = new HashMap<>();
    Pair<Map<Interval, String>, Map<Interval, List<DataSegment>>> materializedVersionAndBaseSegments =
        supervisor.getMaterializedVersionAndBaseSegments(derivativeSegments, toBuildHistoryMvInterval);

    Set<DataSegment> materializedDataSegments = Sets.newHashSet(
        new MaterializedDataSegment(
            derivativeDatasourceName,
            Intervals.of("2015-01-02T00Z/2015-01-02T01Z"),
            "2015-01-01",
            new BaseShardSpecsSpec(0, 3, "2015-01-01"),
            1024
        )
    );
    Assert.assertEquals(
        materializedDataSegments,
        new HashSet<>(materializedVersionAndBaseSegments.rhs.get(Intervals.of(hourInterval)))
    );

    //测试兼容老版本数据
    toBuildHistoryMvInterval = new HashMap<>();
    supervisor.getMaterializedVersionAndBaseSegments(
        createBaseSegments(), toBuildHistoryMvInterval);
    Assert.assertEquals(2, toBuildHistoryMvInterval.size());

    supervisor = createSupervisor(false, Period.parse("P1D"), Granularities.DAY, true);
    toBuildHistoryMvInterval = new HashMap<>();
    supervisor.getMaterializedVersionAndBaseSegments(
        createHistoryDaySegments(), toBuildHistoryMvInterval);
    Assert.assertEquals(2, toBuildHistoryMvInterval.size());
  }

  private Set<DataSegment> createDerivativeSegmentsForHour(String interval, String version)
  {
    return Sets.newHashSet(
        new MaterializedDataSegment(
            derivativeDatasourceName,
            Intervals.of(interval),
            "2022-02-22",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null, null),
            null,
            9,
            1024,
            new MaterializedSpec(
                MaterializedSpec.TYPE_SAME_SEGMENT_GRAN,
                new BaseShardSpecsSpec(0, 3, version),
                null,
                (short) 24
            )
        )
    );
  }

  private Set<DataSegment> createDerivativeSegments(String version)
  {
    return Sets.newHashSet(
        new MaterializedDataSegment(
            derivativeDatasourceName,
            Intervals.of("2015-01-02T00Z/2015-01-02T01Z"),
            "2022-02-22",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null, null),
            null,
            9,
            1024,
            new MaterializedSpec(
                MaterializedSpec.TYPE_SAME_SEGMENT_GRAN,
                new BaseShardSpecsSpec(0, 1, version),
                null,
                (short) 24
            )
        )
    );
  }

  private DataSegment getDataSegmentByPartitionNum(int partitionNum)
  {
    return new DataSegment(
        "base",
        Intervals.of("2014-12-29T00Z/2014-12-29T01Z"),
        "2014-12-29",
        ImmutableMap.of(),
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("m1"),
        new NumberedShardSpec(partitionNum, 0),
        9,
        1024
    );
  }

  private Set<DataSegment> createExpectedToBuildSegments(int partitionNum)
  {
    return Sets.newHashSet(
        new DataSegment(
            "base",
            Intervals.of("2015-01-01T00Z/2015-01-01T01Z"),
            "2015-01-02",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(2, 4, 2, 4, null, null, null),
            9,
            1024
        ),
        new DataSegment(
            "base",
            Intervals.of("2015-01-01T00Z/2015-01-01T01Z"),
            "2015-01-02",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(3, 4, 3, 4, null, null, null),
            9,
            1024
        ), new DataSegment(
            "base",
            Intervals.of("2015-01-02T01Z/2015-01-02T02Z"),
            "2015-01-03",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(1, 2, 1, 2, null, null, null),
            9,
            1024
        ), getDataSegmentByPartitionNum(partitionNum), new DataSegment(
            "base",
            Intervals.of("2015-01-02T02Z/2015-01-02T03Z"),
            "2022-02-02",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null, null),
            9,
            1024
        )
    );
  }

  @Test
  public void testRemoveIntervalOrAddedMaterializedBaseSegments()
  {
    List<DataSegment> baseSegments = createBaseSegments();
    List<DataSegment> derivateSegments = Collections.singletonList(
        new MaterializedDataSegment(
            "base",
            Intervals.of("2015-01-02T00Z/2015-01-03T00Z"),
            "2015-01-04",
            new MaterializedSpec(
                MaterializedSpec.TYPE_SAME_SEGMENT_GRAN,
                new BaseShardSpecsSpec(0, 3, "2015-01-03"),
                null,
                (short) 1
            ),
            1024
        )
    );

    List<DataSegment> derivatePartSegments = Collections.singletonList(
        new MaterializedDataSegment(
            "base",
            Intervals.of("2015-01-02T00Z/2015-01-03T00Z"),
            "2015-01-04",
            new MaterializedSpec(
                MaterializedSpec.TYPE_SAME_SEGMENT_GRAN,
                new BaseShardSpecsSpec(0, 2, "2015-01-03"),
                null,
                (short) 1
            ),
            1024
        )
    );
    boolean hasAddedForOverwrite = createSupervisor(
        true,
        Period.parse("P1D"),
        Granularities.DAY,
        true
    ).checkAddedOrRemoveCommonSegmentsInInterval(
        baseSegments,
        derivateSegments,
        false
    );
    boolean hasAddedForAppending = createSupervisor(
        false,
        Period.parse("P1D"),
        Granularities.DAY, true
    ).checkAddedOrRemoveCommonSegmentsInInterval(
        baseSegments,
        derivateSegments,
        false
    );

    baseSegments = createBaseSegments();
    boolean hasAddedForOverwrite2 = createSupervisor(
        true,
        Period.parse("P1D"),
        Granularities.DAY, true
    ).checkAddedOrRemoveCommonSegmentsInInterval(
        baseSegments,
        derivatePartSegments,
        false
    );
    boolean hasAddedForAppending2 = createSupervisor(
        false,
        Period.parse("P1D"),
        Granularities.DAY, true
    ).checkAddedOrRemoveCommonSegmentsInInterval(
        baseSegments,
        derivatePartSegments,
        false
    );

    Assert.assertEquals(false, hasAddedForAppending);
    Assert.assertEquals(false, hasAddedForOverwrite);

    Assert.assertEquals(true, hasAddedForAppending2);
    Assert.assertEquals(true, hasAddedForOverwrite2);

    //range
    final int numSegs = 10;
    List<DataSegment> derivateSegmentsRange = Collections.singletonList(
        new MaterializedDataSegment(
            "base",
            Intervals.of("2022-01-01T01Z/2022-01-01T02Z"),
            "test_version01",
            new MaterializedSpec(
                MaterializedSpec.TYPE_SAME_SEGMENT_GRAN,
                new BaseShardSpecsSpec(0, numSegs, "2015-01-03"),
                null,
                (short) 1
            ),
            1024
        )
    );
    baseSegments = MaterializedViewSupervisorSpecTest.createAutoStoreAndHourGranDataSegments(numSegs, true);
    boolean hasAddedForOverwrite3 = createSupervisor(
        true,
        Period.parse("P1D"),
        Granularities.DAY, true
    ).checkAddedOrRemoveCommonSegmentsInInterval(
        baseSegments,
        derivateSegmentsRange,
        false
    );
    boolean hasAddedForAppending3 = createSupervisor(
        false,
        Period.parse("P1D"),
        Granularities.DAY, true
    ).checkAddedOrRemoveCommonSegmentsInInterval(
        baseSegments,
        derivateSegmentsRange,
        false
    );

    baseSegments = createBaseSegments();
    baseSegments.remove(1);
    boolean hasAddedForOverwrite4 = createSupervisor(
        true,
        Period.parse("P1D"),
        Granularities.DAY, true
    ).checkAddedOrRemoveCommonSegmentsInInterval(
        baseSegments,
        derivateSegmentsRange,
        true
    );
    Assert.assertEquals(false, hasAddedForOverwrite4);
    Assert.assertEquals(false, hasAddedForOverwrite3);
    Assert.assertEquals(false, hasAddedForAppending3);

  }

  @Test
  public void testCheckIncompleteVersions()
  {
    MaterializedViewSupervisor supervisor = createSupervisor(true, Period.parse("P1D"), Granularities.DAY, true);
    String commonVersion = "2015-02-01";
    Set<Interval> expectedOverwriteInterval = Sets.newHashSet(
        Intervals.of("2015-01-01T00Z/2015-01-02T00Z"),
        Intervals.of("2015-01-02T00Z/2015-01-03T00Z")
    );
    Set<Interval> commonMvIntervals = Sets.newHashSet(
        Intervals.of("2015-01-03T00Z/2015-01-04T00Z")
    );
    commonMvIntervals.addAll(expectedOverwriteInterval);

    HashMap<Interval, String> commonBaseVersions = new HashMap<>();
    commonBaseVersions.put(Intervals.of("2015-01-01T00Z/2015-01-01T01Z"), commonVersion);

    Map<Interval, String> baseVersions = new HashMap<>(commonBaseVersions);
    Map<Interval, String> derivativeVersions = new HashMap<>(commonBaseVersions);
    baseVersions.putAll(derivativeVersions);

    //incomplete:supplement for overwrite day
    baseVersions.put(Intervals.of("2015-01-01T01Z/2015-01-01T02Z"), commonVersion);

    //incomplete
    baseVersions.put(Intervals.of("2015-01-03T01Z/2015-01-03T02Z"), commonVersion);

    //incomplete:different interval count
    baseVersions.put(Intervals.of("2015-01-02T01Z/2015-01-02T02Z"), commonVersion);
    baseVersions.put(Intervals.of("2015-01-02T02Z/2015-01-02T03Z"), commonVersion);
    derivativeVersions.put(Intervals.of("2015-01-02T02Z/2015-01-02T03Z"), commonVersion);

    //unchecked incomplete,do nothing for here
    baseVersions.put(Intervals.of("2015-01-03T01Z/2015-01-03T02Z"), commonVersion);
    derivativeVersions.put(Intervals.of("2015-01-03T02Z/2015-01-03T03Z"), commonVersion);

    Set<Interval> actualInCompleteMvIntervals = supervisor.checkIncompleteVersions(
        commonMvIntervals,
        baseVersions,
        derivativeVersions
    );
    Assert.assertEquals(expectedOverwriteInterval, actualInCompleteMvIntervals);
  }

  private List<DataSegment> createBaseSegments()
  {
    return Lists.newArrayList(
        new DataSegment(
            "base",
            Intervals.of("2015-01-02T00Z/2015-01-02T01Z"),
            "2015-01-03",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(0, 3, 0, 3, null, null, null),
            9,
            1024
        ),
        new DataSegment(
            "base",
            Intervals.of("2015-01-02T02Z/2015-01-02T03Z"),
            "2015-01-03",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(1, 3, 1, 3, null, null, null),
            9,
            1024
        ),
        new DataSegment(
            "base",
            Intervals.of("2015-01-02T02Z/2015-01-02T03Z"),
            "2015-01-03",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(2, 3, 2, 3, null, null, null),
            9,
            1024
        )
    );
  }

  private List<DataSegment> createHistoryDaySegments()
  {
    return Lists.newArrayList(
        new DataSegment(
            "base",
            Intervals.of("2015-01-02/2015-01-03"),
            "2015-01-03",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null, null),
            9,
            1024
        ),
        new DataSegment(
            "base",
            Intervals.of("2015-01-04/2015-01-05"),
            "2015-01-05",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null, null),
            9,
            1024
        )
    );
  }

  @Test
  public void testSort()
  {
    Map<Integer, Integer> maps = new HashMap<>();
    maps.put(10, 10);
    maps.put(8, 8);
    maps.put(7, 7);
    maps.put(4, 4);
    maps.put(11, 11);

    List<Integer> baseIntervalSegments = new ArrayList<>();
    baseIntervalSegments.add(4);
    baseIntervalSegments.add(11);
    baseIntervalSegments.add(8);
    baseIntervalSegments.add(10);
    baseIntervalSegments.add(7);

    List<Integer> materializedPartNums = Arrays.asList(maps.keySet().toArray(new Integer[0]));
    Assert.assertEquals(false, materializedPartNums.equals(baseIntervalSegments));

    Collections.sort(materializedPartNums);
    List<Integer> sortedBasePartNums = baseIntervalSegments.stream()
                                                           .sorted().collect(Collectors.toList());
    Assert.assertEquals(true, materializedPartNums.equals(sortedBasePartNums));
  }

  @Test
  public void testCheckSegmentsAndSubmitTasks() throws IOException, InterruptedException
  {
    MaterializedViewSupervisor supervisor = createSupervisor(false, Period.parse("P1D"), Granularities.DAY, true);
    Set<DataSegment> baseSegments = Sets.newHashSet(
        new DataSegment(
            "base",
            Intervals.of("2015-01-02T00Z/2015-01-03T00Z"),
            "2015-01-03",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null, null),
            9,
            1024
        )
    );
    indexerMetadataStorageCoordinator.commitSegments(baseSegments, null);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of()).anyTimes();
    EasyMock.expect(taskStorage.getStatus("test_task1"))
            .andReturn(Optional.of(TaskStatus.failure("test_task1", "Dummy task status failure err message")))
            .anyTimes();
    EasyMock.expect(taskStorage.getStatus("test_task2"))
            .andReturn(Optional.of(TaskStatus.running("test_task2")))
            .anyTimes();
    EasyMock.replay(taskStorage);

    Pair<Map<Interval, Task>, Map<Interval, String>> runningTasksPair1 = supervisor.getRunningTasks();
    Map<Interval, Task> runningTasks1 = runningTasksPair1.lhs;
    Map<Interval, String> runningVersion1 = runningTasksPair1.rhs;

    DataSchema dataSchema = new DataSchema(
        "test_datasource",
        null,
        null,
        null,
        TransformSpec.NONE,
        objectMapper
    );
    HadoopIOConfig hadoopIOConfig = new HadoopIOConfig(new HashMap<>(), null, null);
    HadoopIngestionSpec spec = new HadoopIngestionSpec(dataSchema, hadoopIOConfig, null);
    HadoopIndexTask task1 = new HadoopIndexTask(
        "test_task1",
        spec,
        null,
        null,
        null,
        objectMapper,
        null,
        null,
        null
    );
    runningTasks1.put(Intervals.of("2015-01-01T00Z/2015-01-02T00Z"), task1);
    runningVersion1.put(Intervals.of("2015-01-01T00Z/2015-01-02T00Z"), "test_version1");

    HadoopIndexTask task2 = new HadoopIndexTask(
        "test_task2",
        spec,
        null,
        null,
        null,
        objectMapper,
        null,
        null,
        null
    );
    runningTasks1.put(Intervals.of("2015-01-02T00Z/2015-01-03T00Z"), task2);
    runningVersion1.put(Intervals.of("2015-01-02T00Z/2015-01-03T00Z"), "test_version2");

    supervisor.checkSegmentsAndSubmitTasks();
    Thread.sleep(supervisor.getConfig().getTaskCheckDuration().toStandardDuration().getMillis() + 1001);

    supervisor.checkSegmentsAndSubmitTasks();
    Pair<Map<Interval, Task>, Map<Interval, String>> runningTasksPair2 = supervisor.getRunningTasks();
    Map<Interval, Task> runningTasks2 = runningTasksPair2.lhs;
    Map<Interval, String> runningVersion2 = runningTasksPair2.rhs;

    Map<Interval, HadoopIndexTask> expectedRunningTasks = new HashMap<>();
    Map<Interval, String> expectedRunningVersion = new HashMap<>();
    expectedRunningTasks.put(Intervals.of("2015-01-02T00Z/2015-01-03T00Z"), task2);
    expectedRunningVersion.put(Intervals.of("2015-01-02T00Z/2015-01-03T00Z"), "test_version2");

    Assert.assertEquals(expectedRunningTasks, runningTasks2);
    Assert.assertEquals(expectedRunningVersion, runningVersion2);


  }

  @Test
  public void testCheckSegmentsAndSubmitTasksForNative() throws IOException, InterruptedException
  {
    MaterializedViewSupervisor supervisor = createSupervisor(false, Period.parse("P1D"), Granularities.DAY, true);
    Set<DataSegment> baseSegments = Sets.newHashSet(
        new DataSegment(
            "base",
            Intervals.of("2015-01-02T00Z/2015-01-03T00Z"),
            "2015-01-03",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null, null),
            9,
            1024
        )
    );
    indexerMetadataStorageCoordinator.commitSegments(baseSegments, null);
    taskQueue = EasyMock.createMock(TaskQueue.class);
    taskQueue.start();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.expect(taskStorage.getStatus("test_task1"))
            .andStubReturn(Optional.of(TaskStatus.failure("test_task1", "Dummy task status failure err message")));
    EasyMock.expect(taskStorage.getStatus("test_task2"))
            .andStubReturn(Optional.of(TaskStatus.running("test_task2")));
    EasyMock.replay(taskStorage);
    EasyMock.replay(taskMaster);

    Pair<Map<Interval, Task>, Map<Interval, String>> runningTasksPair = supervisor.getRunningTasks();
    Map<Interval, Task> runningTasks = runningTasksPair.lhs;
    Map<Interval, String> runningVersion = runningTasksPair.rhs;

    DataSchema dataSchema = new DataSchema(
        "test_datasource",
        null,
        null,
        null,
        TransformSpec.NONE,
        objectMapper
    );
    final MaterializedViewTask.Builder builder1 = new MaterializedViewTask.Builder(
        BASE_DATA_SOURCE,
        DATA_SOURCE,
        segmentCacheManagerFactory,
        RETRY_POLICY_FACTORY
    );
    final MaterializedViewTask task1 = builder1
        .taskId("test_task1")
        .interval(Intervals.of("2015-01-01T00Z/2015-01-02T00Z"))
        .context(context)
        .build();
    runningTasks.put(Intervals.of("2015-01-01T00Z/2015-01-02T00Z"), task1);
    runningVersion.put(Intervals.of("2015-01-01T00Z/2015-01-02T00Z"), "test_version1");

    final MaterializedViewTask.Builder builder2 = new MaterializedViewTask.Builder(
        BASE_DATA_SOURCE,
        DATA_SOURCE,
        segmentCacheManagerFactory,
        RETRY_POLICY_FACTORY
    );
    final MaterializedViewTask task2 = builder2
        .taskId("test_task2")
        .interval(Intervals.of("2015-01-02T00Z/2015-01-03T00Z"))
        .context(context)
        .build();
    runningTasks.put(Intervals.of("2015-01-02T00Z/2015-01-03T00Z"), task2);
    runningVersion.put(Intervals.of("2015-01-02T00Z/2015-01-03T00Z"), "test_version2");

    supervisor.checkSegmentsAndSubmitTasks();
    Thread.sleep(supervisor.getConfig().getTaskCheckDuration().toStandardDuration().getMillis() + 1001);

    supervisor.checkSegmentsAndSubmitTasks();
    Pair<Map<Interval, Task>, Map<Interval, String>> runningTasksPair2 = supervisor.getRunningTasks();
    Map<Interval, Task> runningTasks2 = runningTasksPair2.lhs;
    Map<Interval, String> runningVersion2 = runningTasksPair2.rhs;

    Map<Interval, MaterializedViewTask> expectedRunningTasks = new HashMap<>();
    Map<Interval, String> expectedRunningVersion = new HashMap<>();
    expectedRunningTasks.put(Intervals.of("2015-01-02T00Z/2015-01-03T00Z"), task2);
    expectedRunningVersion.put(Intervals.of("2015-01-02T00Z/2015-01-03T00Z"), "test_version2");

    Assert.assertEquals(expectedRunningTasks, runningTasks2);
    Assert.assertEquals(expectedRunningVersion, runningVersion2);

  }

  @Test
  public void testScorePolicy()
  {
    SortedMap<Interval, Pair<Boolean, String>> sortedToBuildVersion = new TreeMap<>(Comparators.intervalsByStartThenEnd()
                                                                                               .reversed());
    // HOUR segment
    sortedToBuildVersion.put(Intervals.of("2022-02-24T00Z/2022-02-24T01Z"), new Pair<>(false, "test_version01"));
    sortedToBuildVersion.put(Intervals.of("2022-02-22T00Z/2022-02-22T01Z"), new Pair<>(false, "test_version02"));
    // TWO HOUR segment
    // sortedToBuildVersion.put(Intervals.of("2015-01-01T02Z/2015-01-01T04Z"), "test_version03");
    // Another DAY HOUR segment
    sortedToBuildVersion.put(Intervals.of("2022-02-24T10Z/2022-02-24T11Z"), new Pair<>(false, "test_version04"));
    // DAY segment
    // sortedToBuildVersion.put(Intervals.of("2015-01-03T00Z/2015-01-04T00Z"), "test_version05");

    Map<Interval, List<DataSegment>> baseSegments = new HashMap<>();
    baseSegments.put(
        Intervals.of("2022-02-24T00Z/2022-02-24T01Z"),
        Collections.singletonList(new DataSegment(
                                      "base",
                                      Intervals.of("2022-02-24T00Z/2022-02-24T01Z"),
                                      "test_version01",
                                      ImmutableMap.of(),
                                      ImmutableList.of("dim1", "dim2"),
                                      ImmutableList.of("m1"),
                                      new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null, null),
                                      9,
                                      10240
                                  )
        )
    );
    baseSegments.put(Intervals.of("2022-02-22T00Z/2022-02-22T01Z"), Collections.singletonList(
        new DataSegment(
            "base",
            Intervals.of("2022-02-22T00Z/2022-02-22T01Z"),
            "test_version02",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null, null),
            9,
            1024
        )));
    baseSegments.put(Intervals.of("2022-02-24T10Z/2022-02-24T11Z"), Collections.singletonList(
        new DataSegment(
            "base",
            Intervals.of("2022-02-24T10Z/2022-02-24T11Z"),
            "test_version03",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null, null),
            9,
            1024
        )
    ));
    MaterializedViewSearchPolicy materializedViewSearchPolicy = new ScoreMaterializedViewSearchPolicy();
    MaterializedViewIterator<CandidateSegments> reset = materializedViewSearchPolicy.reset(
        baseSegments,
        sortedToBuildVersion,
        new PolicyConfig(null, null, null, ingestDuration, null, null, null)
    );
    while (reset.hasNext()) {
      CandidateSegments next = reset.next();
    }
  }

  /*public TaskQueue createTaskQueue(@Nullable Boolean forceTimeChunkLock)
  {
    final TaskLockConfig lockConfig;
    if (forceTimeChunkLock != null) {
      lockConfig = new TaskLockConfig()
      {
        @Override
        public boolean isForceTimeChunkLock()
        {
          return forceTimeChunkLock;
        }
      };
    } else {
      lockConfig = new TaskLockConfig();
    }
    final TaskQueueConfig queueConfig = new TaskQueueConfig(null, null, null, null);
    final TaskRunner taskRunner = EasyMock.createNiceMock(RemoteTaskRunner.class);
    final TaskActionClientFactory actionClientFactory = EasyMock.createNiceMock(LocalTaskActionClientFactory.class);
    final TaskLockbox lockbox = new TaskLockbox(taskStorage, new TestIndexerMetadataStorageCoordinator());
    final ServiceEmitter emitter = new NoopServiceEmitter();
    return new TaskQueue(lockConfig, queueConfig, taskStorage, taskRunner, actionClientFactory, lockbox, emitter);
  }*/

  @Test
  public void testSubmitTasks()
  {
//    TaskQueue mockQueue = EasyMock.createMock(TaskQueue.class);
    TaskQueue taskQueue = createTaskQueue(true);
    taskQueue.start();
    EasyMock.expect(taskMaster.isLeader()).andReturn(true).anyTimes();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(
        Optional.of(taskQueue)
    ).anyTimes();
    // EasyMock.replay(taskStorage);
    EasyMock.replay(taskMaster);

    SortedMap<Interval, Pair<Boolean, String>> sortedToBuildVersion = new TreeMap<>(Comparators.intervalsByStartThenEnd()
                                                                                               .reversed());
    // HOUR segment
    sortedToBuildVersion.put(Intervals.of("2015-01-01T00Z/2015-01-01T01Z"), new Pair<>(false, "test_version01"));
    sortedToBuildVersion.put(Intervals.of("2015-01-01T01Z/2015-01-01T02Z"), new Pair<>(false, "test_version02"));
    // TWO HOUR segment
    // sortedToBuildVersion.put(Intervals.of("2015-01-01T02Z/2015-01-01T04Z"), "test_version03");
    // Another DAY HOUR segment
    sortedToBuildVersion.put(Intervals.of("2015-01-02T00Z/2015-01-02T01Z"), new Pair<>(false, "test_version04"));
    // DAY segment
    // sortedToBuildVersion.put(Intervals.of("2015-01-03T00Z/2015-01-04T00Z"), "test_version05");

    Map<Interval, List<DataSegment>> baseSegments = new HashMap<>();
    baseSegments.put(
        Intervals.of("2015-01-01T00Z/2015-01-01T01Z"),
        Collections.singletonList(new DataSegment(
                                      "base",
                                      Intervals.of("2015-01-01T00Z/2015-01-01T01Z"),
                                      "test_version01",
                                      ImmutableMap.of(),
                                      ImmutableList.of("dim1", "dim2"),
                                      ImmutableList.of("m1"),
                                      new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null, null),
                                      9,
                                      PolicyConfig.MAX_TASK_INPUT_SIZE + 1
                                  )
        )
    );
    baseSegments.put(Intervals.of("2015-01-01T01Z/2015-01-01T02Z"), Collections.singletonList(
        new DataSegment(
            "base",
            Intervals.of("2015-01-01T01Z/2015-01-01T02Z"),
            "test_version02",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null, null),
            9,
            1024
        )));
    baseSegments.put(Intervals.of("2015-01-02T00Z/2015-01-02T01Z"), Collections.singletonList(
        new DataSegment(
            "base",
            Intervals.of("2015-01-02T00Z/2015-01-02T01Z"),
            "test_version03",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null, null),
            9,
            1024
        )
    ));

    // DAY segment materialized
    final MaterializedViewTask.Builder builder = new MaterializedViewTask.Builder(
        BASE_DATA_SOURCE,
        DATA_SOURCE,
        segmentCacheManagerFactory,
        RETRY_POLICY_FACTORY
    );
    final MaterializedViewTask task2 = builder
        .interval(Intervals.of("2014-01-01/2014-01-02"))
        .context(context)
        .build();

    Pair<Map<Interval, Task>, Map<Interval, String>> runningTasksPair = supervisor.getRunningTasks();
    Map<Interval, Task> runningTasks = runningTasksPair.lhs;
    Map<Interval, String> runningVersion = runningTasksPair.rhs;

    supervisor.submitTasks(sortedToBuildVersion, baseSegments);
    Map<Interval, MaterializedViewTask> expectedRunningTasks = new HashMap<>();
    Map<Interval, String> expectedRunningVersion = new HashMap<>();
    expectedRunningTasks.put(Intervals.of("2015-01-01T00Z/2015-01-01T01Z"), task2);
    expectedRunningTasks.put(Intervals.of("2015-01-01T01Z/2015-01-01T02Z"), task2);
    expectedRunningTasks.put(Intervals.of("2015-01-02T00Z/2015-01-02T01Z"), task2);
    expectedRunningVersion.put(Intervals.of("2015-01-01T00Z/2015-01-01T01Z"), "test_version01");
    expectedRunningVersion.put(Intervals.of("2015-01-01T01Z/2015-01-01T02Z"), "test_version02");
    expectedRunningVersion.put(Intervals.of("2015-01-02T00Z/2015-01-02T01Z"), "test_version04");

    Set<Task> runningTaskSets = supervisor.getRunningTaskSets();
    Assert.assertEquals(expectedRunningTasks.keySet(), runningTasks.keySet());
    Assert.assertEquals(expectedRunningVersion, runningVersion);
    Assert.assertEquals(sortedToBuildVersion.size() - 1, supervisor.getRunningTaskSets().size());
  }

  @Test
  public void testSubmitTasksForOverwrite()
  {
    MaterializedViewSupervisor supervisor = createSupervisor(true, Period.parse("P1D"), Granularities.DAY, true);
    boolean isOverwrite = true;
    TaskQueue taskQueue = createTaskQueue(true);
    taskQueue.start();
    EasyMock.expect(taskMaster.isLeader()).andReturn(true).anyTimes();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(
        Optional.of(taskQueue)
    ).anyTimes();
    // EasyMock.replay(taskStorage);
    EasyMock.replay(taskMaster);

    SortedMap<Interval, Pair<Boolean, String>> sortedToBuildVersion = new TreeMap<>(Comparators.intervalsByStartThenEnd()
                                                                                               .reversed());
    // HOUR segment
    sortedToBuildVersion.put(Intervals.of("2015-01-01T00Z/2015-01-01T01Z"), new Pair<>(isOverwrite, "test_version01"));
    sortedToBuildVersion.put(Intervals.of("2015-01-01T01Z/2015-01-01T02Z"), new Pair<>(isOverwrite, "test_version02"));
    // TWO HOUR segment
    // sortedToBuildVersion.put(Intervals.of("2015-01-01T02Z/2015-01-01T04Z"), "test_version03");
    // Another DAY HOUR segment
    sortedToBuildVersion.put(Intervals.of("2015-01-02T00Z/2015-01-02T01Z"), new Pair<>(isOverwrite, "test_version04"));
    // DAY segment
    // sortedToBuildVersion.put(Intervals.of("2015-01-03T00Z/2015-01-04T00Z"), "test_version05");

    Map<Interval, List<DataSegment>> baseSegments = new HashMap<>();
    baseSegments.put(
        Intervals.of("2015-01-01T00Z/2015-01-01T01Z"),
        Collections.singletonList(new DataSegment(
                                      "base",
                                      Intervals.of("2015-01-01T00Z/2015-01-01T01Z"),
                                      "test_version01",
                                      ImmutableMap.of(),
                                      ImmutableList.of("dim1", "dim2"),
                                      ImmutableList.of("m1"),
                                      new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null, null),
                                      9,
                                      1024
                                  )
        )
    );
    baseSegments.put(Intervals.of("2015-01-01T01Z/2015-01-01T02Z"), Collections.singletonList(
        new DataSegment(
            "base",
            Intervals.of("2015-01-01T01Z/2015-01-01T02Z"),
            "test_version02",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null, null),
            9,
            1024
        )));
    baseSegments.put(Intervals.of("2015-01-02T00Z/2015-01-02T01Z"), Collections.singletonList(
        new DataSegment(
            "base",
            Intervals.of("2015-01-02T00Z/2015-01-02T01Z"),
            "test_version03",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null, null),
            9,
            1024
        )
    ));

    // DAY segment materialized
    final MaterializedViewTask.Builder builder = new MaterializedViewTask.Builder(
        BASE_DATA_SOURCE,
        DATA_SOURCE,
        segmentCacheManagerFactory,
        RETRY_POLICY_FACTORY
    );
    final MaterializedViewTask task = builder
        .interval(Intervals.of("2014-01-01/2014-01-02"))
        .context(context)
        .build();

    Pair<Map<Interval, Task>, Map<Interval, String>> runningTasksPair = supervisor.getRunningTasks();
    Map<Interval, Task> runningTasks = runningTasksPair.lhs;
    Map<Interval, String> runningVersion = runningTasksPair.rhs;

    supervisor.submitTasks(sortedToBuildVersion, baseSegments);
    Map<Interval, MaterializedViewTask> expectedRunningTasks = new HashMap<>();
    Map<Interval, String> expectedRunningVersion = new HashMap<>();
    expectedRunningTasks.put(Intervals.of("2015-01-01T00Z/2015-01-01T01Z"), task);
    expectedRunningTasks.put(Intervals.of("2015-01-01T01Z/2015-01-01T02Z"), task);
    expectedRunningTasks.put(Intervals.of("2015-01-02T00Z/2015-01-02T01Z"), task);
    expectedRunningVersion.put(Intervals.of("2015-01-01T00Z/2015-01-01T01Z"), "test_version01");
    expectedRunningVersion.put(Intervals.of("2015-01-01T01Z/2015-01-01T02Z"), "test_version02");
    expectedRunningVersion.put(Intervals.of("2015-01-02T00Z/2015-01-02T01Z"), "test_version04");

    Assert.assertEquals(expectedRunningTasks.keySet(), runningTasks.keySet());
    Assert.assertEquals(expectedRunningVersion, runningVersion);
    Assert.assertEquals(2, supervisor.getRunningTaskSets().size());
  }

  @Test
  public void testCheckSegmentsForOverwrite() throws IOException
  {
    MaterializedViewSupervisor supervisor = createSupervisor(true, Period.parse("P1D"), Granularities.DAY, true);

    Set<DataSegment> baseSegments = Sets.newHashSet(
        new DataSegment(//fully materialized
                        "base",
                        Intervals.of("2015-01-04T00Z/2015-01-04T01Z"),
                        "2015-01-04",
                        ImmutableMap.of(),
                        ImmutableList.of("dim1", "dim2"),
                        ImmutableList.of("m1"),
                        new HashBasedNumberedShardSpec(0, 2, 0, 2, null, null, null),
                        9,
                        1024
        ),
        new DataSegment(
            "base",
            Intervals.of("2015-01-04T00Z/2015-01-04T01Z"),
            "2015-01-04",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(1, 2, 1, 2, null, null, null),
            9,
            1024
        ),
        new DataSegment(//filter due to minTimeLag
                        "base",
                        Intervals.of("2015-01-05T09Z/2015-01-05T10Z"),
                        "2015-01-04",
                        ImmutableMap.of(),
                        ImmutableList.of("dim1", "dim2"),
                        ImmutableList.of("m1"),
                        new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null, null),
                        9,
                        1024
        ),
        new DataSegment(//filter due to minTimeLag
                        "base",
                        Intervals.of("2015-01-06T09Z/2015-01-06T10Z"),
                        "2015-01-04",
                        ImmutableMap.of(),
                        ImmutableList.of("dim1", "dim2"),
                        ImmutableList.of("m1"),
                        new HashBasedNumberedShardSpec(1, 2, 1, 2, null, null, null),
                        9,
                        1024
        )
    );
    Set<DataSegment> expectedSegments = Sets.newHashSet(
        new DataSegment(
            "base",
            Intervals.of("2015-01-01T00Z/2015-01-01T01Z"),
            "2015-01-02",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(0, 4, 0, 4, null, null, null),
            9,
            1024
        ),
        new DataSegment(
            "base",
            Intervals.of("2015-01-01T00Z/2015-01-01T01Z"),
            "2015-01-02",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(1, 4, 1, 4, null, null, null),
            9,
            1024
        ),
        new DataSegment(
            "base",
            Intervals.of("2015-01-01T00Z/2015-01-01T01Z"),
            "2015-01-02",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(2, 4, 2, 4, null, null, null),
            9,
            1024
        ),
        new DataSegment(
            "base",
            Intervals.of("2015-01-01T00Z/2015-01-01T01Z"),
            "2015-01-02",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(3, 4, 3, 4, null, null, null),
            9,
            1024
        ),
        new DataSegment(
            "base",
            Intervals.of("2015-01-02T01Z/2015-01-02T02Z"),
            "2015-01-03",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(0, 2, 0, 2, null, null, null),
            9,
            1024
        ),
        new DataSegment(
            "base",
            Intervals.of("2015-01-02T01Z/2015-01-02T02Z"),
            "2015-01-03",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(1, 2, 1, 2, null, null, null),
            9,
            1024
        ),
        new DataSegment(
            "base",
            Intervals.of("2015-01-02T02Z/2015-01-02T03Z"),
            "2022-02-02",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null, null),
            9,
            1024
        ),
        new DataSegment(//need materialize ingestion time range default P3D means [2015-02T09Z,2015-01-05T09Z)
                        "base",
                        Intervals.of("2015-01-05T08Z/2015-01-05T09Z"),
                        "2015-01-04",
                        ImmutableMap.of(),
                        ImmutableList.of("dim1", "dim2"),
                        ImmutableList.of("m1"),
                        new HashBasedNumberedShardSpec(1, 2, 1, 2, null, null, null),
                        9,
                        1024
        )
    );
    baseSegments.addAll(expectedSegments);
    // already materialized segments

    Map<Short, BaseShardSpecsSpec> multiSegmentGrans1 = new HashMap<>();
    multiSegmentGrans1.put(
        (short) 0, // intervalId
        new BaseShardSpecsSpec(0, 2, "2015-01-02")
    );

    Map<Short, BaseShardSpecsSpec> multiSegmentGrans2 = new HashMap<>();
    // added due to same version
    multiSegmentGrans2.put(
        (short) 1, // intervalId
        new BaseShardSpecsSpec(0, 1, "2015-01-03")
    );
    // need overwrite due to different version
    multiSegmentGrans2.put(
        (short) 2, // intervalId is 2 means 2015-01-02T02Z/2015-01-02T03Z
        new BaseShardSpecsSpec(0, 1, "2015-01-03")
    );

    //fully materialized
    Map<Short, BaseShardSpecsSpec> multiSegmentGrans3 = new HashMap<>();
    multiSegmentGrans3.put(
        (short) 0, //0 means 2015-01-04T00Z/2015-01-04T01Z
        new BaseShardSpecsSpec(0, 2, "2015-01-04")
    );
    Set<DataSegment> derivativeSegments = Sets.newHashSet(
        new MaterializedDataSegment(
            derivativeDatasourceName,
            Intervals.of("2015-01-01T00Z/2015-01-02T00Z"),
            "2022-02-22",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null, null),
            null,
            9,
            1024,
            new MaterializedSpec(
                MaterializedSpec.TYPE_DIFF_SEGMENT_GRAN,
                null,
                multiSegmentGrans1,
                (short) 24
            )
        ),
        new MaterializedDataSegment(
            derivativeDatasourceName,
            Intervals.of("2015-01-02T00Z/2015-01-03T00Z"),
            "2022-02-22",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null, null),
            null,
            9,
            1024,
            new MaterializedSpec(
                MaterializedSpec.TYPE_DIFF_SEGMENT_GRAN,
                null,
                multiSegmentGrans2,
                (short) 24
            )
        ),
        new MaterializedDataSegment(
            derivativeDatasourceName,
            Intervals.of("2015-01-04T00Z/2015-01-05T00Z"),
            "2022-02-22",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null, null),
            null,
            9,
            1024,
            new MaterializedSpec(
                MaterializedSpec.TYPE_DIFF_SEGMENT_GRAN,
                null,
                multiSegmentGrans3,
                (short) 24
            )
        )
    );
    indexerMetadataStorageCoordinator.commitSegments(baseSegments, null);
    indexerMetadataStorageCoordinator.commitSegments(derivativeSegments, null);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    Pair<SortedMap<Interval, Pair<Boolean, String>>, Map<Interval, List<DataSegment>>> toBuildInterval = supervisor.checkSegments();
    Set<Interval> expectedToBuildInterval = Sets.newHashSet(
        Intervals.of("2015-01-01T00Z/2015-01-01T01Z"),
        Intervals.of("2015-01-02T01Z/2015-01-02T02Z"),
        Intervals.of("2015-01-02T02Z/2015-01-02T03Z")
    );


    List<Pair<Boolean, String>> expectedSubmitTypeAndVersion = new ArrayList<>();
    expectedSubmitTypeAndVersion.add(new Pair<>(true, "2022-02-02"));
    expectedSubmitTypeAndVersion.add(new Pair<>(true, "2015-01-03"));
    expectedSubmitTypeAndVersion.add(new Pair<>(true, "2015-01-02"));
    List<Pair<Boolean, String>> actualSubmitTypeAndVersion = new ArrayList<>(toBuildInterval.lhs.values());
    // sort
    actualSubmitTypeAndVersion.sort(Comparator.comparing(Pair::toString));
    expectedSubmitTypeAndVersion.sort(Comparator.comparing(Pair::toString));

    // overwrite: contains all interval segments
    Assert.assertEquals(expectedToBuildInterval, toBuildInterval.lhs.keySet());
    assertEqualsForSegmentsOverwrite(
        expectedToBuildInterval,
        expectedSegments,
        toBuildInterval.rhs
    );
    Assert.assertEquals(
        expectedSubmitTypeAndVersion,
        actualSubmitTypeAndVersion
    );
  }

  @Test
  public void testCheckSegmentsForDayAuto() throws IOException
  {
    MaterializedViewSupervisor supervisor = createSupervisor(false, Period.parse("P1D"), Granularities.DAY, true);

    Set<DataSegment> baseSegments = Sets.newHashSet(
        new DataSegment(
            "base",
            Intervals.of("2015-01-01T00Z/2015-01-01T01Z"),
            "2015-01-02",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(0, 4, 0, 4, null, null, null),
            9,
            1024
        ),
        new DataSegment(
            "base",
            Intervals.of("2015-01-01T00Z/2015-01-01T01Z"),
            "2015-01-02",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(1, 4, 1, 4, null, null, null),
            9,
            1024
        ),

        new DataSegment(
            "base",
            Intervals.of("2015-01-04T00Z/2015-01-04T01Z"),
            "2015-01-04",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null, null),
            9,
            1024
        )
    );

    // need overwrite segments
    Set<DataSegment> overwriteOrAppendSegments = creatOverwriteOrAppendingDayByDiffVersion();
    baseSegments.addAll(overwriteOrAppendSegments);

    // already materialized segments

    Map<Short, BaseShardSpecsSpec> multiSegmentGrans1 = new HashMap<>();
    multiSegmentGrans1.put(
        (short) 0, // "2015-01-01T00Z/2015-01-01T01Z"
        new BaseShardSpecsSpec(0, 2, "2015-01-02")
    );

    Map<Short, BaseShardSpecsSpec> multiSegmentGrans2 = new HashMap<>();
    // same version
    multiSegmentGrans2.put(
        (short) 1, // 2015-01-02T01Z/2015-01-02T02Z
        new BaseShardSpecsSpec(0, 1, "2015-01-03")
    );
    // need overwrite due to different version
    multiSegmentGrans2.put(
        (short) 2, // 2 means 2015-01-02T02Z/2015-01-02T03Z
        new BaseShardSpecsSpec(0, 1, "2015-01-03")
    );

    Set<DataSegment> derivativeSegments = Sets.newHashSet(
        new MaterializedDataSegment(
            derivativeDatasourceName,
            Intervals.of("2015-01-01T00Z/2015-01-02T00Z"),
            "2022-02-22",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null, null),
            null,
            9,
            1024,
            new MaterializedSpec(
                MaterializedSpec.TYPE_DIFF_SEGMENT_GRAN,
                null,
                multiSegmentGrans1,
                (short) 24
            )
        ),
        new MaterializedDataSegment(
            derivativeDatasourceName,
            Intervals.of("2015-01-02T00Z/2015-01-03T00Z"),
            "2022-02-22",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null, null),
            null,
            9,
            1024,
            new MaterializedSpec(
                MaterializedSpec.TYPE_DIFF_SEGMENT_GRAN,
                null,
                multiSegmentGrans2,
                (short) 24
            )
        )
    );
    indexerMetadataStorageCoordinator.commitSegments(baseSegments, null);
    indexerMetadataStorageCoordinator.commitSegments(derivativeSegments, null);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    Pair<SortedMap<Interval, Pair<Boolean, String>>, Map<Interval, List<DataSegment>>> toBuildInterval = supervisor.checkSegments();
    Set<Interval> expectedToBuildInterval = Sets.newHashSet(
        Intervals.of("2015-01-01T00Z/2015-01-01T01Z"),
        Intervals.of("2015-01-02T01Z/2015-01-02T02Z"),
        Intervals.of("2015-01-02T02Z/2015-01-02T03Z")
    );


    List<Pair<Boolean, String>> expectedSubmitTypeAndVersion = new ArrayList<>();
    expectedSubmitTypeAndVersion.add(new Pair<>(true, "2022-02-02"));
    expectedSubmitTypeAndVersion.add(new Pair<>(true, "2015-01-03"));
    expectedSubmitTypeAndVersion.add(new Pair<>(false, "2015-01-02"));
    List<Pair<Boolean, String>> actualSubmitTypeAndVersion = new ArrayList<>(toBuildInterval.lhs.values());
    // sort
    actualSubmitTypeAndVersion.sort(Comparator.comparing(Pair::toString));
    expectedSubmitTypeAndVersion.sort(Comparator.comparing(Pair::toString));

    // overwrite: contains all interval segments
    assertEqualsForSegmentsOverwrite(
        expectedToBuildInterval,
        overwriteOrAppendSegments,
        toBuildInterval.rhs
    );
    Assert.assertEquals(expectedToBuildInterval, toBuildInterval.lhs.keySet());
    Assert.assertEquals(
        expectedSubmitTypeAndVersion,
        actualSubmitTypeAndVersion
    );
  }

  private Set<DataSegment> creatOverwriteOrAppendingDayByDiffVersion()
  {
    return Sets.newHashSet(
        new DataSegment(
            "base",
            Intervals.of("2015-01-01T00Z/2015-01-01T01Z"),
            "2015-01-02",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(2, 4, 2, 4, null, null, null),
            9,
            1024
        ),
        new DataSegment(
            "base",
            Intervals.of("2015-01-01T00Z/2015-01-01T01Z"),
            "2015-01-02",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(3, 4, 3, 4, null, null, null),
            9,
            1024
        ),
        new DataSegment(//done part
                        "base",
                        Intervals.of("2015-01-02T01Z/2015-01-02T02Z"),
                        "2015-01-03",
                        ImmutableMap.of(),
                        ImmutableList.of("dim1", "dim2"),
                        ImmutableList.of("m1"),
                        new HashBasedNumberedShardSpec(0, 2, 0, 2, null, null, null),
                        9,
                        1024
        ),
        new DataSegment(
            "base",
            Intervals.of("2015-01-02T01Z/2015-01-02T02Z"),
            "2015-01-03",
            ImmutableMap.of(),
            ImmutableList.of("dim1", "dim2"),
            ImmutableList.of("m1"),
            new HashBasedNumberedShardSpec(1, 2, 1, 2, null, null, null),
            9,
            1024
        ),
        new DataSegment(//different version
                        "base",
                        Intervals.of("2015-01-02T02Z/2015-01-02T03Z"),
                        "2022-02-02",
                        ImmutableMap.of(),
                        ImmutableList.of("dim1", "dim2"),
                        ImmutableList.of("m1"),
                        new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null, null),
                        9,
                        1024
        )
    );
  }

  private void assertEqualsForSegmentsOverwrite(
      Set<Interval> expectedToBuildInterval,
      Set<DataSegment> baseSegments,
      Map<Interval, List<DataSegment>> toBuildIntervalSegs
  )
  {
    Map<Interval, List<DataSegment>> expectedSegments = new HashMap<>();
    for (DataSegment dataSegment : baseSegments) {
      Interval interval = dataSegment.getInterval();
      if (expectedToBuildInterval.contains(interval)) {
        List<DataSegment> dataSegments = expectedSegments.computeIfAbsent(
            interval,
            k -> new ArrayList<>()
        );
        dataSegments.add(dataSegment);
      }
    }

    // Assert.assertEquals(expectedSegments.size(), toBuildIntervalSegs.size());
    for (Map.Entry<Interval, List<DataSegment>> entry : toBuildIntervalSegs.entrySet()) {
      if (expectedSegments.containsKey(entry.getKey())) {
        expectedSegments.get(entry.getKey()).sort(Comparator.comparing(DataSegment::toString));
        entry.getValue().sort(Comparator.comparing(DataSegment::toString));
        Assert.assertEquals(expectedSegments.get(entry.getKey()), entry.getValue());
      }
    }
  }

  /**
   * 物化视图分区内触发全量物化情况：
   * 1.forceRollup为true，物化视图分区内有对应新增base segment，触发物化视图区间全量物化
   * 2.enableSecondRegionOverwrite为true（即启用第二个物化区间范围内的segment进行全量物化），物化视图分区处于全量物化区间内（即第二个物化区间）内有对应新增base
   * segment，触发物化视图区间全量物化.
   * 3.物化视图分区内,base segment的version与物化视图segment的version不一致，触发物化视图区间全量物化。
   *
   * @param forceRollup
   * @param segmentGranularity
   * @param enableSecondRegionOverwrite
   * @return
   */
  private MaterializedViewSupervisor createSupervisor(
      boolean forceRollup,
      Period skipPeriodFromLatest,
      Granularity segmentGranularity,
      boolean enableSecondRegionOverwrite
  )
  {
    MaterializedViewTaskConfig materializedViewTaskConfig = new MaterializedViewTaskConfig();
    materializedViewTaskConfig.setEnableTruncateIngestionTime(true);
    materializedViewTaskConfig.setTaskCheckDuration(new Period("PT3S"));
    NativeBatchMaterializedViewSupervisorSpec spec = new NativeBatchMaterializedViewSupervisorSpec(
        BASE_DATA_SOURCE,
        new DimensionsSpec(Collections.singletonList(new StringDimensionSchema("dim"))),
        new AggregatorFactory[]{new LongSumAggregatorFactory("m1", "m1")},
        new ClientTaskGranularitySpec(segmentGranularity, segmentGranularity, true),
        createParallelIndexTuningConfig(forceRollup),
        DATA_SOURCE,
        new PolicyConfig(null, skipPeriodFromLatest, null, ingestDuration, null, enableSecondRegionOverwrite, null),
        context,
        false,
        objectMapper,
        taskMaster,
        taskStorage,
        metadataSupervisorManager,
        sqlSegmentsMetadataManager,
        indexerMetadataStorageCoordinator,
        materializedViewTaskConfig,
        EasyMock.createMock(AuthorizerMapper.class),
        EasyMock.createMock(ChatHandlerProvider.class),
        new SupervisorStateManagerConfig(),
        segmentCacheManagerFactory,
        new RetryPolicyFactory(new RetryPolicyConfig())
    );
    MaterializedViewSupervisor supervisor = (MaterializedViewSupervisor) spec.createSupervisor();

    // mock IndexerSQLMetadataStorageCoordinator to ensure that retrieveDataSourceMetadata is not called
    // which will be true if truly suspended, since this is the first operation of the 'run' method otherwise
    IndexerSQLMetadataStorageCoordinator mock = EasyMock.createMock(IndexerSQLMetadataStorageCoordinator.class);
    EasyMock.expect(mock.retrieveDataSourceMetadata(spec.getDataSourceName()))
            .andThrow(new AssertionFailedError())
            .anyTimes();

    EasyMock.replay(mock);
    supervisor.run();
    return supervisor;
  }

  @Test
  public void testResetOffsetsNotSupported()
  {
    MaterializedViewSupervisorSpec suspended = new NativeBatchMaterializedViewSupervisorSpec(
        "base",
        new DimensionsSpec(Collections.singletonList(new StringDimensionSchema("dim"))),
        new AggregatorFactory[]{new LongSumAggregatorFactory("m1", "m1")},
        new ClientTaskGranularitySpec(Granularities.HOUR, Granularities.HOUR, null),
        null,
        DATA_SOURCE,
        null,
        null,
        true,
        objectMapper,
        taskMaster,
        taskStorage,
        metadataSupervisorManager,
        sqlSegmentsMetadataManager,
        indexerMetadataStorageCoordinator,
        new MaterializedViewTaskConfig(),
        EasyMock.createMock(AuthorizerMapper.class),
        EasyMock.createMock(ChatHandlerProvider.class),
        new SupervisorStateManagerConfig(),
        segmentCacheManagerFactory,
        new RetryPolicyFactory(new RetryPolicyConfig())
    );
    MaterializedViewSupervisor supervisor = (MaterializedViewSupervisor) suspended.createSupervisor();
    Assert.assertThrows(
        "Reset offsets not supported in MaterializedViewSupervisor",
        UnsupportedOperationException.class,
        () -> supervisor.resetOffsets(null)
    );
  }

  private DataSegment createSegment(String datasource, String interval, String version)
  {
    return new DataSegment(
        datasource,
        Intervals.of(interval),
        version,
        Collections.emptyMap(),
        Arrays.asList("dim1", "dim2"),
        Collections.singletonList("m2"),
        new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null, null),
        9,
        1024
    );
  }

  private ParallelIndexTuningConfig createParallelIndexTuningConfig(boolean forceRollup)
  {
    return new ParallelIndexTuningConfig(
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        forceRollup == true ? HashedPartitionsSpec.defaultSpec() : new DynamicPartitionsSpec(null, null),
        null,
        null,
        null,
        forceRollup,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );
  }
}
