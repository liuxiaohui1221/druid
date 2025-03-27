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

package org.apache.druid.client.materializedview;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.smile.SmileGenerator;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.vavr.Tuple2;
import io.vavr.Tuple3;
import io.vavr.Tuple4;
import junitparams.converters.Nullable;
import org.apache.druid.client.BatchServerInventoryView;
import org.apache.druid.client.BrokerSegmentWatcherConfig;
import org.apache.druid.client.BrokerServerView;
import org.apache.druid.client.DirectDruidClientFactory;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.selector.HighestPriorityTierSelectorStrategy;
import org.apache.druid.client.selector.RandomServerSelectorStrategy;
import org.apache.druid.client.selector.ServerSelector;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.curator.CuratorTestBase;
import org.apache.druid.indexing.overlord.DerivativeDataSource;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.spec.MultipleSpecificSegmentSpec;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.metadata.SegmentSchemaManager;
import org.apache.druid.segment.realtime.appenderator.SegmentSchemas;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.initialization.ZkPathsConfig;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.timeline.BaseShardSpecsSpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.MaterializedSpec;
import org.apache.druid.timeline.TimelineLookup;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.apache.druid.timeline.partition.ShardSpecTestUtils;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class DatasourceOptimizerTest extends CuratorTestBase
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();
  private final ObjectMapper objectMapper = ShardSpecTestUtils.initObjectMapper();
  private DerivativeDataSourceManager derivativesManager;
  private DruidServer druidServer;
  private ObjectMapper jsonMapper;
  private ZkPathsConfig zkPathsConfig;
  private DataSourceOptimizer optimizer;
  private IndexerSQLMetadataStorageCoordinator metadataStorageCoordinator;
  private SegmentSchemaManager segmentSchemaManager;
  private BatchServerInventoryView baseView;
  private BrokerServerView brokerServerView;
  private String dataSourceDay = "derivative_ds_day";
  private String dataSourceHour = "derivative_ds_hour";
  private String dataSourceTwoHour = "derivative_ds_twohour";
  private String dataSource = "derivative";
  private String baseDataSource = "base";
  private String baseVersion = "v1";
  private String derivativeVersion1 = "v2";
  private String derivativeVersion2 = "v3";

  @Before
  public void setUp() throws Exception
  {
    TestDerbyConnector derbyConnector = derbyConnectorRule.getConnector();
    derbyConnector.createDataSourceTable();
    derbyConnector.createSegmentTable();
    MaterializedViewConfig viewConfig = new MaterializedViewConfig();
    jsonMapper = TestHelper.makeJsonMapper();
    jsonMapper.registerSubtypes(new NamedType(DerivativeDataSourceMetadata.class, "view"));
    metadataStorageCoordinator = EasyMock.createMock(IndexerSQLMetadataStorageCoordinator.class);
    derivativesManager = new DerivativeDataSourceManager(
        viewConfig,
        derbyConnectorRule.metadataTablesConfigSupplier(),
        jsonMapper,
        derbyConnector
    );
    segmentSchemaManager = new SegmentSchemaManager(
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        objectMapper,
        derbyConnector
    );
    metadataStorageCoordinator = new IndexerSQLMetadataStorageCoordinator(
        jsonMapper,
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        derbyConnector,
        segmentSchemaManager,
        CentralizedDatasourceSchemaConfig.create()
    );
    segmentSchemaManager = new SegmentSchemaManager(
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        objectMapper,
        derbyConnector
    );

    setupServerAndCurator();
    curator.start();
    curator.blockUntilConnected();

    zkPathsConfig = new ZkPathsConfig();


    druidServer = new DruidServer(
        "localhost:1234",
        "localhost:1234",
        null,
        10000000L,
        ServerType.HISTORICAL,
        "default_tier",
        0
    );
    setupZNodeForServer(druidServer, new ZkPathsConfig(), jsonMapper);
    NullHandling.initializeForTests();
  }

  private ImmutableMap<String, DerivativeDataSource> createSubDerivativeDataSources()
  {
    List<DerivativeDataSource> list = new ArrayList<>();
    list.add(new DerivativeDataSource(dataSource, baseDataSource, Granularities.DAY));
    return ImmutableMap.copyOf(list.stream().collect(Collectors.toMap(ds -> ds.getDataSource(), name -> name)));
  }

//  private ImmutableMap<String, DerivativeDataSource> createMultiSubDerivativeDataSources(boolean multiSub)
//  {
//    HashMap<String, DerivativeDataSource> sets = new HashMap<>();
//    if (multiSub) {
//      sets.put(dataSource, new DerivativeDataSource(dataSource, baseDataSource, Granularities.HOUR));
//      sets.put(dataSourceDay, new DerivativeDataSource(dataSourceDay, dataSource, Granularities.DAY));
//    }
//    sets.put(dataSourceDay, new DerivativeDataSource(dataSourceDay, baseDataSource, Granularities.TWO_HOUR));
//
//    return ImmutableMap.copyOf(sets);
//  }

  private Map<String, DerivativeDataSource> createMultiSubDerivativeDataSources(
      Granularity curSegmentGranularity,
      Map<String, String> linkDataSources
  )
  {
    HashMap<String, DerivativeDataSource> sets = new HashMap<>();
    for (Map.Entry<String, String> entry : linkDataSources.entrySet()) {
      sets.put(entry.getKey(), new DerivativeDataSource(entry.getKey(), entry.getValue(), curSegmentGranularity));
    }
    return sets;
  }

  @After
  public void tearDown() throws IOException
  {
    baseView.stop();
    tearDownServerAndCurator();
  }

  private DerivativeDataSourceManager mockDerivatives(Query query) throws Exception
  {
    DerivativeDataSourceManager mockClient = EasyMock.createMock(DerivativeDataSourceManager.class);
    EasyMock.expect(mockClient.getSubDerivativeDataSources(baseDataSource)).andStubReturn(ImmutableMap.of());
    EasyMock.expect(mockClient.getDirectBaseDataSource(baseDataSource)).andStubReturn(null);
    EasyMock.expect(mockClient.getRootBaseDataSource(baseDataSource)).andStubReturn(baseDataSource);
    // dataSource-->baseDataSource
    HashMap<String, String> subDataSources1 = new HashMap<>();
    subDataSources1.put(dataSource, baseDataSource);
    Map<String, DerivativeDataSource> subDs1 = createMultiSubDerivativeDataSources(
        Granularities.HOUR,
        subDataSources1
    );
    EasyMock.expect(mockClient.getSubDerivativeDataSources(dataSource))
            .andStubReturn(ImmutableMap.copyOf(subDs1));
    EasyMock.expect(mockClient.getDirectBaseDataSource(dataSource)).andStubReturn(baseDataSource);
    EasyMock.expect(mockClient.getRootBaseDataSource(dataSource)).andStubReturn(baseDataSource);

    // dataSourceDay-->dataSource-->baseDataSource
    HashMap<String, String> subDataSources2 = new HashMap<>();
    subDataSources2.put(dataSourceDay, dataSource);
    Map<String, DerivativeDataSource> subDs2 = createMultiSubDerivativeDataSources(
        Granularities.DAY,
        subDataSources2
    );
    subDs2.putAll(subDs1);
    EasyMock.expect(mockClient.getSubDerivativeDataSources(dataSourceDay))
            .andStubReturn(ImmutableMap.copyOf(subDs2));
    EasyMock.expect(mockClient.getDirectBaseDataSource(dataSourceDay)).andStubReturn(dataSource);
    EasyMock.expect(mockClient.getRootBaseDataSource(dataSourceDay)).andStubReturn(baseDataSource);

    SortedSet<DerivativeDataSource> expectedSortedDers=
        new TreeSet<>(subDs2.values());
    Set<String> requiredFields = MaterializedViewUtils.getRequiredFields(query);
    EasyMock.expect(mockClient.getCandidateSortedDerivatives(baseDataSource,requiredFields)).andStubReturn(expectedSortedDers);

    EasyMock.replay(mockClient);
    setupViews(mockClient);
    return mockClient;
  }

  private DerivativeDataSourceManager mockMultiDerivatives() throws Exception
  {
    DerivativeDataSourceManager mockClient = EasyMock.createMock(DerivativeDataSourceManager.class);
    EasyMock.expect(mockClient.getSubDerivativeDataSources(baseDataSource)).andStubReturn(ImmutableMap.of());
    EasyMock.expect(mockClient.getDirectBaseDataSource(baseDataSource)).andStubReturn(null);
    EasyMock.expect(mockClient.getRootBaseDataSource(baseDataSource)).andStubReturn(null);
    // dataSourceHour --> baseDataSource
    HashMap<String, String> subDataSources1 = new HashMap<>();
    subDataSources1.put(dataSourceHour, baseDataSource);
    EasyMock.expect(mockClient.getSubDerivativeDataSources(dataSourceHour))
            .andStubReturn(ImmutableMap.copyOf(createMultiSubDerivativeDataSources(Granularities.HOUR,
                                                                                   subDataSources1)));
    EasyMock.expect(mockClient.getDirectBaseDataSource(dataSourceHour)).andStubReturn(baseDataSource);
    EasyMock.expect(mockClient.getRootBaseDataSource(dataSourceHour)).andStubReturn(baseDataSource);
    // dataSourceTwoHour --> dataSourceHour --> baseDataSource
    HashMap<String, String> subDataSources2 = new HashMap<>(subDataSources1);
    subDataSources2.put(dataSourceTwoHour, dataSourceHour);
    EasyMock.expect(mockClient.getSubDerivativeDataSources(dataSourceTwoHour))
            .andStubReturn(ImmutableMap.copyOf(createMultiSubDerivativeDataSources(Granularities.TWO_HOUR,
                                                                                   subDataSources2)));
    EasyMock.expect(mockClient.getDirectBaseDataSource(dataSourceTwoHour)).andStubReturn(dataSourceHour);
    EasyMock.expect(mockClient.getRootBaseDataSource(dataSourceTwoHour)).andStubReturn(baseDataSource);
    // dataSourceDay --> dataSourceTwoHour --> dataSourceHour --> baseDataSource
    HashMap<String, String> subDataSources3 = new HashMap<>(subDataSources2);
    subDataSources3.put(dataSourceDay, dataSourceTwoHour);
    EasyMock.expect(mockClient.getSubDerivativeDataSources(dataSourceDay))
            .andStubReturn(ImmutableMap.copyOf(createMultiSubDerivativeDataSources(Granularities.DAY,
                                                                                   subDataSources3)));
    EasyMock.expect(mockClient.getDirectBaseDataSource(dataSourceDay)).andStubReturn(dataSourceTwoHour);
    EasyMock.expect(mockClient.getRootBaseDataSource(dataSourceDay)).andStubReturn(baseDataSource);
    EasyMock.replay(mockClient);
    setupViews(mockClient);
    return mockClient;
  }

  @Test(timeout = 60_000L)
  public void testOptimize() throws Exception
  {
    //init
    DerivativeDataSourceManager mockClient = EasyMock.createMock(DerivativeDataSourceManager.class);
    EasyMock.expect(mockClient.getSubDerivativeDataSources(baseDataSource)).andStubReturn(ImmutableMap.of());
    EasyMock.expect(mockClient.getSubDerivativeDataSources(dataSource)).andStubReturn(createSubDerivativeDataSources());
    EasyMock.expect(mockClient.getDirectBaseDataSource(dataSource)).andStubReturn(baseDataSource);
    EasyMock.expect(mockClient.getRootBaseDataSource(dataSource)).andStubReturn(baseDataSource);
    EasyMock.replay(mockClient);
    setupViews(mockClient);
    optimizer = new DataSourceOptimizer(brokerServerView, mockClient);

    // insert datasource metadata
    DerivativeDataSourceMetadata metadata = new DerivativeDataSourceMetadata(
        baseDataSource,
        new ClientTaskGranularitySpec(
            Granularities.DAY,
            Granularities.MINUTE,
            true
        ),
        Collections.<String>emptySet(),
        Collections.<String>emptySet()
    );
    metadataStorageCoordinator.insertDataSourceMetadata(dataSource, metadata);
    // insert base datasource segments
    List<Boolean> baseResult = Lists.transform(
        ImmutableList.of(
            new Tuple2<>(
                "2011-04-01T00Z/2011-04-01T01Z",
                new HashBasedNumberedShardSpec(0, 2, 0, 2, null, null, objectMapper)
            ),
            new Tuple2<>(
                "2011-04-01T00Z/2011-04-01T01Z",
                new HashBasedNumberedShardSpec(1, 2, 1, 2, null, null, objectMapper)
            ),
            new Tuple2<>(
                "2011-04-02T00Z/2011-04-02T01Z",
                new HashBasedNumberedShardSpec(0, 2, 0, 2, null, null, objectMapper)
            ),
            new Tuple2<>(
                "2011-04-02T00Z/2011-04-02T01Z",
                new HashBasedNumberedShardSpec(1, 2, 1, 2, null, null, objectMapper)
            ),
            new Tuple2<>("2011-04-03T00Z/2011-04-03T01Z", new NumberedShardSpec(0, 2)),
            new Tuple2<>("2011-04-03T00Z/2011-04-03T01Z", new NumberedShardSpec(1, 2)),
            new Tuple2<>("2011-04-04T00Z/2011-04-04T01Z", new NumberedShardSpec(0, 1)),
            new Tuple2<>("2011-04-05T00Z/2011-04-05T01Z", new NumberedShardSpec(0, 1))
        ),
        intervalAndShard -> {
          final DataSegment segment = createDataSegment(
              baseDataSource,
              intervalAndShard._1,
              baseVersion,
              Lists.newArrayList("dim1", "dim2", "dim3", "dim4"),
              1024 * 1024,
              intervalAndShard._2,
              null,
              false
          );
          try {
            metadataStorageCoordinator.commitSegments(Sets.newHashSet(segment), null);
            announceSegmentForServer(druidServer, segment, zkPathsConfig, jsonMapper);
          }
          catch (IOException e) {
            return false;
          }
          return true;
        }
    );
    // insert derivative segments
    List<Boolean> derivativeResult = Lists.transform(
        ImmutableList.of(
            new Tuple3<>("2011-04-01/2011-04-02", 0, 1),
            new Tuple3<>("2011-04-02/2011-04-03", 0, 2),
            new Tuple3<>("2011-04-03/2011-04-04", 0, 1)
        ),
        intervalAndRange -> {
          Map<Short, BaseShardSpecsSpec> multiMaterializedSpecs = new HashMap<>();
          multiMaterializedSpecs.put(
              (short) 0,
              new BaseShardSpecsSpec(intervalAndRange._2, intervalAndRange._3, baseVersion)
          );
          final DataSegment segment = createDataSegment(
              dataSource,
              intervalAndRange._1,
              derivativeVersion1,
              Lists.newArrayList("dim1", "dim2", "dim3"),
              1024,
              null,
              new MaterializedSpec(MaterializedSpec.TYPE_DIFF_SEGMENT_GRAN, null, multiMaterializedSpecs, (short) 24),
              true
          );
          try {
            metadataStorageCoordinator.commitSegments(Sets.newHashSet(segment), null);
            announceSegmentForServer(druidServer, segment, zkPathsConfig, jsonMapper);
          }
          catch (IOException e) {
            return false;
          }
          return true;
        }
    );
    Assert.assertFalse(baseResult.contains(false));
    Assert.assertFalse(derivativeResult.contains(false));
    derivativesManager.start();
    while (DerivativeDataSourceManager.getAllDerivatives().isEmpty()) {
      TimeUnit.SECONDS.sleep(1L);
    }
    // build user query
    TopNQuery userQuery = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension("dim1")
        .metric("cost")
        .threshold(4)
        .intervals("2011-04-01/2011-04-06")
        .aggregators(new LongSumAggregatorFactory("cost", "cost"))
        .build();

    List<Query> expectedQueryAfterOptimizing = Lists.newArrayList(
        new TopNQueryBuilder()
            .dataSource(dataSource)
            .granularity(QueryRunnerTestHelper.ALL_GRAN)
            .dimension("dim1")
            .metric("cost")
            .threshold(4)
            .intervals(new MultipleSpecificSegmentSpec(Arrays.asList(
                new SegmentDescriptor(
                    Intervals.of("2011-04-02/2011-04-03"),
                    derivativeVersion1,
                    0
                ),
                new SegmentDescriptor(
                    Intervals.of("2011-04-03/2011-04-04"),
                    derivativeVersion1,
                    0
                ),
                new SegmentDescriptor(
                    Intervals.of("2011-04-01/2011-04-02"),
                    derivativeVersion1,
                    0
                )
            ), Collections.singletonList(Intervals.of("2011-04-01/2011-04-06"))))
            .aggregators(new LongSumAggregatorFactory("cost", "cost"))
            .build(),
        new TopNQueryBuilder()
            .dataSource(baseDataSource)
            .granularity(QueryRunnerTestHelper.ALL_GRAN)
            .dimension("dim1")
            .metric("cost")
            .threshold(4)
            .intervals(new MultipleSpecificSegmentSpec(Arrays.asList(
                new SegmentDescriptor(
                    Intervals.of("2011-04-03T00Z/2011-04-03T01Z"),
                    baseVersion,
                    1
                ),
                new SegmentDescriptor(
                    Intervals.of("2011-04-04T00Z/2011-04-04T01Z"),
                    baseVersion,
                    0
                ),
                new SegmentDescriptor(
                    Intervals.of("2011-04-05T00Z/2011-04-05T01Z"),
                    baseVersion,
                    0
                ),
                new SegmentDescriptor(
                    Intervals.of("2011-04-01T00Z/2011-04-01T01Z"),
                    baseVersion,
                    1
                )
            ), Collections.singletonList(Intervals.of("2011-04-01/2011-04-06"))))
            .aggregators(new LongSumAggregatorFactory("cost", "cost"))
            .build()
    );
    List<Query> optimize = optimizer.optimize(userQuery);
    Assert.assertEquals(expectedQueryAfterOptimizing, optimize);
    derivativesManager.stop();
  }

//  @Test(timeout = 60_000L)
  public void testOptimizeForMultiLevelMV() throws Exception
  {
    // dataSourceDay --> dataSourceTwoHour --> dataSourceHour --> baseDataSource
    //init datasource mapping
    DerivativeDataSourceManager mockClient = mockMultiDerivatives();
    optimizer = new DataSourceOptimizer(brokerServerView, mockClient);

    // insert datasource metadata
    DerivativeDataSourceMetadata metadata = new DerivativeDataSourceMetadata(
        baseDataSource,
        new ClientTaskGranularitySpec(
            Granularities.HOUR,
            Granularities.MINUTE,
            true
        ),
        Collections.<String>emptySet(),
        Collections.<String>emptySet()
    );
    metadataStorageCoordinator.insertDataSourceMetadata(dataSource, metadata);
     /*
     base datasource的intervals信息:
        2011-04-01T01Z/2011-04-01T02Z(_0,_1)(2个分片),
        2011-04-03T00Z/2011-04-03T01Z(_0,_1)(2个分片),
        2011-04-04T00Z/2011-04-04T01Z(_0)(1个分片),
        2011-04-05T00Z/2011-04-05T01Z(_0)(1个分片)
     各个层级物化视图物化情况：
        2011-04-04T00Z/2011-04-04T01Z_0                                   ----被hour、twohour粒度datasource物化
        2011-04-01T01Z/2011-04-01T02Z(_0,_1),2011-04-03T00Z/2011-04-03T01Z_0   ----被hour、twohour、dayhour粒度datasource物化
     没有被任何物化视图物化的列表：
        2011-04-01T00Z/2011-04-01T01Z(_0,_1), 2011-04-03T00Z/2011-04-03T01Z_1, 2011-04-05T00Z/2011-04-05T01Z_0
    */
    // insert base segments
    final List<DataSegment> announcedBaseSegments = new ArrayList<>();
    List<Boolean> baseResult = Lists.transform(
        ImmutableList.of(
            new Tuple2<>(
                "2011-04-01T00Z/2011-04-01T01Z",
                new HashBasedNumberedShardSpec(0, 2, 0, 2, null, null, objectMapper)
            ),
            new Tuple2<>(
                "2011-04-01T00Z/2011-04-01T01Z",
                new HashBasedNumberedShardSpec(1, 2, 1, 2, null, null, objectMapper)
            ),
            new Tuple2<>(
                "2011-04-01T01Z/2011-04-01T02Z",
                new HashBasedNumberedShardSpec(0, 2, 0, 2, null, null, objectMapper)
            ),
            new Tuple2<>(
                "2011-04-01T01Z/2011-04-01T02Z",
                new HashBasedNumberedShardSpec(1, 2, 1, 2, null, null, objectMapper)
            ),
            new Tuple2<>("2011-04-03T00Z/2011-04-03T01Z", new NumberedShardSpec(0, 2)),
            new Tuple2<>("2011-04-03T00Z/2011-04-03T01Z", new NumberedShardSpec(1, 2)),
            new Tuple2<>("2011-04-04T00Z/2011-04-04T01Z", new NumberedShardSpec(0, 1)),
            new Tuple2<>("2011-04-05T00Z/2011-04-05T01Z", new NumberedShardSpec(0, 1))
        ),
        intervalAndShard -> {
          final DataSegment segment = createDataSegment(
              baseDataSource,
              intervalAndShard._1,
              baseVersion,
              Lists.newArrayList("dim1", "dim2", "dim3", "dim4"),
              1024 * 1024,
              intervalAndShard._2,
              null,
              false
          );
          try {
            metadataStorageCoordinator.commitSegments(Sets.newHashSet(segment), null);
            announceSegmentForServer(druidServer, segment, zkPathsConfig, jsonMapper);
            announcedBaseSegments.add(segment);
          }
          catch (IOException e) {
            return false;
          }
          return true;
        }
    );

    // -------insert derivative segments-------
    //twohour,hour materialize base
    final List<DataSegment> announcedDerivativeSegments = new ArrayList<>();
    ImmutableList<Tuple4<String, Short, Integer, Integer>> derivativeIntervals = ImmutableList.of(
        new Tuple4<>("2011-04-01T00Z/2011-04-01T02Z", (short) 1, 0, 2),//分片号1，分片数2，分片起始位置0，分片结束位置1
        new Tuple4<>("2011-04-03T00Z/2011-04-03T02Z", (short) 0, 0, 1),
        new Tuple4<>("2011-04-04T00Z/2011-04-04T02Z", (short) 0, 0, 1)
    );
    List<Boolean> derivativeHourResult = getDerivativeResult(dataSourceHour, 2, baseVersion,
                                                             derivativeVersion1, announcedDerivativeSegments,
                                                             derivativeIntervals
    );
    List<Boolean> derivativeTwoHourResult = getDerivativeResult(dataSourceTwoHour, 2, baseVersion,
                                                                derivativeVersion1, announcedDerivativeSegments,
                                                                derivativeIntervals
    );
    //day materialize twohour
    short mapPartitions = 12;//1day(24hour)/twohour=12
    final List<DataSegment> announcedDerivativeDaySegments = new ArrayList<>();
    ImmutableList<Tuple4<String, Short, Integer, Integer>> derivativeDayIntervals = ImmutableList.of(
        new Tuple4<>("2011-04-01/2011-04-02", (short) 0, 0, 1),
        new Tuple4<>("2011-04-03/2011-04-04", (short) 0, 0, 1)
    );
    List<Boolean> derivativeDayResult2 = getDerivativeResult(dataSourceDay, mapPartitions, derivativeVersion1,
                                                             derivativeVersion2, announcedDerivativeDaySegments,
                                                             derivativeDayIntervals
    );
    Assert.assertFalse(baseResult.contains(false));
    Assert.assertFalse(derivativeHourResult.contains(false));
    Assert.assertFalse(derivativeTwoHourResult.contains(false));
    Assert.assertFalse(derivativeDayResult2.contains(false));
    derivativesManager.start();
    while (DerivativeDataSourceManager.getAllDerivatives().isEmpty()) {
      TimeUnit.SECONDS.sleep(1L);
    }
    // build user query
    TopNQuery userQuery = new TopNQueryBuilder()
        .dataSource(dataSourceDay)
        .granularity(QueryRunnerTestHelper.DAY_GRAN)
        .dimension("dim1")
        .metric("cost")
        .threshold(4)
        .intervals("2011-04-01/2011-04-06")
        .aggregators(new LongSumAggregatorFactory("cost", "cost"))
        .build();

    List<Query> expectedQueryAfterOptimizing = Lists.newArrayList(
        new TopNQueryBuilder()
            .dataSource(dataSourceDay)
            .granularity(QueryRunnerTestHelper.DAY_GRAN)
            .dimension("dim1")
            .metric("cost")
            .threshold(4)
            .intervals(new MultipleSpecificSegmentSpec(Arrays.asList(
                new SegmentDescriptor(
                    Intervals.of("2011-04-03/2011-04-04"),
                    derivativeVersion2,
                    0
                ),
                new SegmentDescriptor(
                    Intervals.of("2011-04-01/2011-04-02"),
                    derivativeVersion2,
                    0
                )
            ), Collections.singletonList(Intervals.of("2011-04-01/2011-04-06"))))
            .aggregators(new LongSumAggregatorFactory("cost", "cost"))
            .build(),
        new TopNQueryBuilder()
            .dataSource(dataSource)
            .granularity(QueryRunnerTestHelper.DAY_GRAN)
            .dimension("dim1")
            .metric("cost")
            .threshold(4)
            .intervals(new MultipleSpecificSegmentSpec(Collections.singletonList(
                new SegmentDescriptor(
                    Intervals.of("2011-04-04T00Z/2011-04-04T02Z"),
                    derivativeVersion1,
                    0
                )
            ), Collections.singletonList(Intervals.of("2011-04-01/2011-04-06"))))
            .aggregators(new LongSumAggregatorFactory("cost", "cost"))
            .build(),
        new TopNQueryBuilder()
            .dataSource(baseDataSource)
            .granularity(QueryRunnerTestHelper.DAY_GRAN)
            .dimension("dim1")
            .metric("cost")
            .threshold(4)
            .intervals(new MultipleSpecificSegmentSpec(Arrays.asList(
                new SegmentDescriptor(
                    Intervals.of("2011-04-03T00Z/2011-04-03T01Z"),
                    baseVersion,
                    1
                ),
                new SegmentDescriptor(
                    Intervals.of("2011-04-05T00Z/2011-04-05T01Z"),
                    baseVersion,
                    0
                ),
                new SegmentDescriptor(
                    Intervals.of("2011-04-01T00Z/2011-04-01T01Z"),
                    baseVersion,
                    1
                ),
                new SegmentDescriptor(
                    Intervals.of("2011-04-01T00Z/2011-04-01T01Z"),
                    baseVersion,
                    0
                )
            ), Collections.singletonList(Intervals.of("2011-04-01/2011-04-06"))))
            .aggregators(new LongSumAggregatorFactory("cost", "cost"))
            .build()
    );
    List<Query> optimize = optimizer.optimize(userQuery);
    Assert.assertEquals(expectedQueryAfterOptimizing, optimize);

    // check announced segments and unannounced segments
    Optional<? extends TimelineLookup<String, ServerSelector>> timeline1_start = brokerServerView.getTimeline(
        (new TableDataSource(baseDataSource)).getAnalysis(),
        true
    );
    int recoveryMaterializedSegments1 = timeline1_start.get()
                                                       .lookup(new Interval(DateTimes.EPOCH, DateTimes.MAX))
                                                       .size();

    announcedDerivativeSegments.forEach(segment -> {
      try {
        unannounceSegmentForServer(druidServer, segment, zkPathsConfig);
      }
      catch (Exception e) {
        e.printStackTrace();
      }
    });
    TimeUnit.MILLISECONDS.sleep(500L);
    Optional<? extends TimelineLookup<String, ServerSelector>> timeline1_end = brokerServerView.getTimeline(
        (new TableDataSource(baseDataSource)).getAnalysis(),
        true
    );
    int recoveryMaterializedSegments2 = timeline1_end.isPresent() ? timeline1_end.get()
                                                                                 .lookup(new Interval(
                                                                                     DateTimes.EPOCH,
                                                                                     DateTimes.MAX
                                                                                 ))
                                                                                 .size() : 0;

    // check announced segments and unannounced segments
    Optional<? extends TimelineLookup<String, ServerSelector>> timeline2_start = brokerServerView.getTimeline(
        JoinDataSource.forDataSource(new TableDataSource(baseDataSource)),
        true
    );
    int recoveryHourSegments3 = timeline2_start.get()
                                               .lookup(new Interval(
                                                   DateTimes.EPOCH,
                                                   DateTimes.MAX
                                               ))
                                               .size();
    announcedDerivativeDaySegments.forEach(segment -> {
      try {
        unannounceSegmentForServer(druidServer, segment, zkPathsConfig);
      }
      catch (Exception e) {
        e.printStackTrace();
      }
    });
    TimeUnit.MILLISECONDS.sleep(500L);
    Optional<? extends TimelineLookup<String, ServerSelector>> timeline2_end = brokerServerView.getTimeline(
        (new TableDataSource(baseDataSource)).getAnalysis(),
        true
    );
    int recoveryMaterializedSegments4 = timeline2_end.get().lookup(new Interval(DateTimes.EPOCH, DateTimes.MAX)).size();

    announcedBaseSegments.forEach(segment -> {
      try {
        unannounceSegmentForServer(druidServer, segment, zkPathsConfig);
        System.out.println(segment.getId());
      }
      catch (Exception e) {
        e.printStackTrace();
      }
    });
    TimeUnit.MILLISECONDS.sleep(100L);
    Assert.assertEquals(derivativeIntervals.size(), recoveryMaterializedSegments1);
    Assert.assertEquals(0, recoveryMaterializedSegments2);

    Assert.assertEquals(derivativeDayIntervals.size(), recoveryHourSegments3);
    Assert.assertEquals(0, recoveryMaterializedSegments4);
    derivativesManager.stop();
  }

  private List<Boolean> getDerivativeResult(
      final String derivativeDataSource,
      final int mapPartitions,
      final String baseVersion,
      final String derivativeVersion,
      final List<DataSegment> announcedDerivativeSegments,
      ImmutableList<Tuple4<String, Short, Integer, Integer>> derivativeIntervals
  )
  {
    List<Boolean> derivativeResult = Lists.transform(
        derivativeIntervals,
        intervalAndRange -> {
          Map<Short, BaseShardSpecsSpec> multiMaterializedSpecs = new HashMap<>();
          multiMaterializedSpecs.put(
              intervalAndRange._2,
              new BaseShardSpecsSpec(intervalAndRange._3, intervalAndRange._4, baseVersion)
          );
          final DataSegment segment = createDataSegment(
              derivativeDataSource,
              intervalAndRange._1,
              derivativeVersion,
              Lists.newArrayList("dim1", "dim2", "dim3"),
              1024,
              null,
              new MaterializedSpec(MaterializedSpec.TYPE_DIFF_SEGMENT_GRAN, null, multiMaterializedSpecs,
                                   (short) mapPartitions
              ),
              true
          );
          try {
            metadataStorageCoordinator.commitSegments(Sets.newHashSet(segment), null);
            announceSegmentForServer(druidServer, segment, zkPathsConfig, jsonMapper);
            announcedDerivativeSegments.add(segment);
          }
          catch (Exception e) {
            return false;
          }
          return true;
        }
    );
    return derivativeResult;
  }

  /**
   * Please refer to excel（./mv_optimizer_test_case.xlsx）for the analysis results
   *
   * @throws Exception
   */
  @Test()
  public void testOptimizeForDayAndHourMV() throws Exception
  {
    // build user query
    TopNQuery userQuery = new TopNQueryBuilder()
        .dataSource(baseDataSource)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension("dim1")
        .metric("cost")
        .threshold(4)
        .intervals("2011-04-01/2011-04-06")
        .aggregators(new LongSumAggregatorFactory("cost", "cost"))
        .build();
    //init
    DerivativeDataSourceManager mockClient = mockDerivatives(userQuery);
    optimizer = new DataSourceOptimizer(brokerServerView, mockClient);

    // insert datasource metadata
    DerivativeDataSourceMetadata metadata = new DerivativeDataSourceMetadata(
        baseDataSource,
        new ClientTaskGranularitySpec(
            Granularities.HOUR,
            Granularities.HOUR,
            true
        ),
        Collections.<String>emptySet(),
        Collections.<String>emptySet()
    );
    metadataStorageCoordinator.insertDataSourceMetadata(dataSource, metadata);
    // insert base datasource segments
    //被物化的intervals: 2011-04-01T01Z/2011-04-01T02Z, 2011-04-03T00Z/2011-04-03T01Z(只物化了第1个), 2011-04-04T00Z/2011-04-04T01Z --被hour粒度物化
    // 2011-04-01T01Z/2011-04-01T02Z,2011-04-03T00Z/2011-04-03T01Z(只物化了第1个)--被day、hour粒度物化
    //没有被任何物化视图物化的列表：
    //2011-04-01T00Z/2011-04-01T01Z ,2011-04-03T00Z/2011-04-03T01Z_1, 2011-04-05T00Z/2011-04-05T01Z
    List<Boolean> baseResult = Lists.transform(
        ImmutableList.of(
            new Tuple2<>(
                "2011-04-01T00Z/2011-04-01T01Z",
                new HashBasedNumberedShardSpec(0, 2, 0, 2, null, null, objectMapper)
            ),
            new Tuple2<>(
                "2011-04-01T00Z/2011-04-01T01Z",
                new HashBasedNumberedShardSpec(1, 2, 1, 2, null, null, objectMapper)
            ),
            new Tuple2<>(
                "2011-04-01T01Z/2011-04-01T02Z",
                new HashBasedNumberedShardSpec(0, 2, 0, 2, null, null, objectMapper)
            ),
            new Tuple2<>(
                "2011-04-01T01Z/2011-04-01T02Z",
                new HashBasedNumberedShardSpec(1, 2, 1, 2, null, null, objectMapper)
            ),
            new Tuple2<>("2011-04-03T00Z/2011-04-03T01Z", new NumberedShardSpec(0, 2)),
            new Tuple2<>("2011-04-03T00Z/2011-04-03T01Z", new NumberedShardSpec(1, 2)),
            new Tuple2<>("2011-04-04T00Z/2011-04-04T01Z", new NumberedShardSpec(0, 1)),
            new Tuple2<>("2011-04-05T00Z/2011-04-05T01Z", new NumberedShardSpec(0, 1))
        ),
        intervalAndShard -> {
          final DataSegment segment = createDataSegment(
              baseDataSource,
              intervalAndShard._1,
              baseVersion,
              Lists.newArrayList("dim1", "dim2", "dim3", "dim4"),
              1024 * 1024,
              intervalAndShard._2,
              null,
              false
          );
          try {
            metadataStorageCoordinator.commitSegments(Sets.newHashSet(segment), null);
            announceSegmentForServer(druidServer, segment, zkPathsConfig, jsonMapper);
          }
          catch (IOException e) {
            return false;
          }
          return true;
        }
    );
    // insert derivative segments
    //hour to hour
    List<Boolean> derivativeResult = Lists.transform(
        ImmutableList.of(
            //base segment buckets[startId,endId)
            new Tuple3<>("2011-04-01T00Z/2011-04-01T01Z", 0, 1),
            new Tuple3<>("2011-04-01T01Z/2011-04-01T02Z", 0, 2),
            new Tuple3<>("2011-04-03T00Z/2011-04-03T01Z", 0, 1),
            new Tuple3<>("2011-04-04T00Z/2011-04-04T01Z", 0, 1)
        ),
        intervalAndRange -> {
          final DataSegment segment = createDataSegment(
              dataSource,
              intervalAndRange._1,
              derivativeVersion1,
              Lists.newArrayList("dim1", "dim2", "dim3"),
              1024,
              null,
              new MaterializedSpec(
                  MaterializedSpec.TYPE_SAME_SEGMENT_GRAN,
                  new BaseShardSpecsSpec(intervalAndRange._2, intervalAndRange._3, baseVersion),
                  null,
                  (short) 1
              ),
              true
          );
          try {
            metadataStorageCoordinator.commitSegments(Sets.newHashSet(segment), null);
            announceSegmentForServer(druidServer, segment, zkPathsConfig, jsonMapper);
          }
          catch (IOException e) {
            return false;
          }
          return true;
        }
    );
    //day materialize hour
    short mapPartitions = 24;
    List<Boolean> derivativeResult2 = Lists.transform(
        ImmutableList.of(
            //物化2011-04-01/2011-04-02范围内第1个小时区间（即1点-2点区间）内的0、1号segment
            new Tuple4<>("2011-04-01/2011-04-02", (short) 1, 0, 2),
            new Tuple4<>("2011-04-03/2011-04-04", (short) 0, 0, 1)
        ),
        intervalAndRange -> {
          Map<Short, BaseShardSpecsSpec> multiMaterializedSpecs = new HashMap<>();
          multiMaterializedSpecs.put(
              intervalAndRange._2,
              new BaseShardSpecsSpec(intervalAndRange._3, intervalAndRange._4, derivativeVersion1)
          );
          final DataSegment segment = createDataSegment(
              dataSourceDay,
              intervalAndRange._1,
              derivativeVersion2,
              Lists.newArrayList("dim1", "dim2", "dim3"),
              1023,
              null,
              new MaterializedSpec(
                  MaterializedSpec.TYPE_DIFF_SEGMENT_GRAN,
                  null,
                  multiMaterializedSpecs,
                  mapPartitions
              ),
              true
          );
          try {
            metadataStorageCoordinator.commitSegments(Sets.newHashSet(segment), null);
            announceSegmentForServer(druidServer, segment, zkPathsConfig, jsonMapper);
          }
          catch (IOException e) {
            e.printStackTrace();
            return false;
          }
          return true;
        }
    );

    Assert.assertFalse(baseResult.contains(false));
    Assert.assertFalse(derivativeResult.contains(false));
    Assert.assertFalse(derivativeResult2.contains(false));
    derivativesManager.start();
    while (DerivativeDataSourceManager.getAllDerivatives().isEmpty()) {
      TimeUnit.SECONDS.sleep(1L);
    }


    List<Query> expectedQueryAfterOptimizing = Lists.newArrayList(
        new TopNQueryBuilder()
            .dataSource(dataSourceDay)
            .granularity(QueryRunnerTestHelper.ALL_GRAN)
            .dimension("dim1")
            .metric("cost")
            .threshold(4)
            .intervals(new MultipleSpecificSegmentSpec(Arrays.asList(
                new SegmentDescriptor(
                    Intervals.of("2011-04-01/2011-04-02"),
                    derivativeVersion2,
                    0
                ), new SegmentDescriptor(
                    Intervals.of("2011-04-03/2011-04-04"),
                    derivativeVersion2,
                    0
                )
            ), Collections.singletonList(Intervals.of("2011-04-01/2011-04-06"))))
            .aggregators(new LongSumAggregatorFactory("cost", "cost"))
            .build(),
        new TopNQueryBuilder()
            .dataSource(dataSource)
            .granularity(QueryRunnerTestHelper.ALL_GRAN)
            .dimension("dim1")
            .metric("cost")
            .threshold(4)
            .intervals(new MultipleSpecificSegmentSpec(Arrays.asList(
                new SegmentDescriptor(
                    Intervals.of("2011-04-01T00Z/2011-04-01T01Z"),
                    derivativeVersion1,
                    0
                ), new SegmentDescriptor(
                    Intervals.of("2011-04-04T00Z/2011-04-04T01Z"),
                    derivativeVersion1,
                    0
                )
            ), Collections.singletonList(Intervals.of("2011-04-01/2011-04-06"))))
            .aggregators(new LongSumAggregatorFactory("cost", "cost"))
            .build(),
        new TopNQueryBuilder()
            .dataSource(baseDataSource)
            .granularity(QueryRunnerTestHelper.ALL_GRAN)
            .dimension("dim1")
            .metric("cost")
            .threshold(4)
            .intervals(new MultipleSpecificSegmentSpec(Arrays.asList(
                new SegmentDescriptor(
                    Intervals.of("2011-04-03T00Z/2011-04-03T01Z"),
                    baseVersion,
                    1
                ),
                new SegmentDescriptor(
                    Intervals.of("2011-04-05T00Z/2011-04-05T01Z"),
                    baseVersion,
                    0
                ),
                new SegmentDescriptor(
                    Intervals.of("2011-04-01T00Z/2011-04-01T01Z"),
                    baseVersion,
                    1
                )
            ), Collections.singletonList(Intervals.of("2011-04-01/2011-04-06"))))
            .aggregators(new LongSumAggregatorFactory("cost", "cost"))
            .build()
    );
    List<Query> optimize = optimizer.optimize(userQuery);
    Assert.assertEquals(expectedQueryAfterOptimizing, optimize);
    derivativesManager.stop();
  }


  @Test(timeout = 60_000L)
  public void testOptimizeForAppendingMV() throws Exception
  {
    // build user query
    TopNQuery userQuery = new TopNQueryBuilder()
        .dataSource(dataSourceDay)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension("dim1")
        .metric("cost")
        .threshold(4)
        .intervals("2011-04-01/2011-04-06")
        .aggregators(new LongSumAggregatorFactory("cost", "cost"))
        .build();
    //init
    DerivativeDataSourceManager mockClient = mockDerivatives(userQuery);
    optimizer = new DataSourceOptimizer(brokerServerView, mockClient);

    // insert datasource metadata
    DerivativeDataSourceMetadata metadata = new DerivativeDataSourceMetadata(
        baseDataSource,
        new ClientTaskGranularitySpec(
            Granularities.DAY,
            Granularities.HOUR,
            true
        ),
        Collections.<String>emptySet(),
        Collections.<String>emptySet()
    );
    DerivativeDataSourceMetadata metadata2 = new DerivativeDataSourceMetadata(
        dataSource,
        new ClientTaskGranularitySpec(
            Granularities.HOUR,
            Granularities.MINUTE,
            true
        ),
        Collections.<String>emptySet(),
        Collections.<String>emptySet()
    );
    metadataStorageCoordinator.insertDataSourceMetadata(dataSource, metadata);
    metadataStorageCoordinator.insertDataSourceMetadata(dataSourceDay, metadata2);
    // insert base datasource segments
    //被物化的intervals: 2011-04-01T01Z/2011-04-01T02Z, 2011-04-03T00Z/2011-04-03T01Z(只物化了3个),
    // 2011-04-04T00Z/2011-04-04T01Z --被hour粒度物化
    // 2011-04-01T01Z/2011-04-01T02Z,2011-04-03T00Z/2011-04-03T01Z(只物化了3个)--被day、hour粒度物化
    //没有被任何物化视图物化的列表：
    //2011-04-01T00Z/2011-04-01T01Z ,2011-04-03T00Z/2011-04-03T01Z_3, 2011-04-05T00Z/2011-04-05T01Z
    List<Boolean> baseResult = Lists.transform(
        ImmutableList.of(
            new Tuple2<>("2011-04-01T00Z/2011-04-01T01Z", new NumberedShardSpec(0, 0)),
            new Tuple2<>("2011-04-01T00Z/2011-04-01T01Z", new NumberedShardSpec(1, 0)),
            new Tuple2<>("2011-04-01T01Z/2011-04-01T02Z", new NumberedShardSpec(0, 2)),
            new Tuple2<>("2011-04-01T01Z/2011-04-01T02Z", new NumberedShardSpec(1, 2)),
            new Tuple2<>("2011-04-03T00Z/2011-04-03T01Z", new NumberedShardSpec(0, 0)),
            new Tuple2<>("2011-04-03T00Z/2011-04-03T01Z", new NumberedShardSpec(1, 0)),
            new Tuple2<>("2011-04-03T00Z/2011-04-03T01Z", new NumberedShardSpec(2, 0)),
            new Tuple2<>("2011-04-03T00Z/2011-04-03T01Z", new NumberedShardSpec(3, 0)),
            new Tuple2<>("2011-04-04T00Z/2011-04-04T01Z", new NumberedShardSpec(0, 1)),
            new Tuple2<>("2011-04-05T00Z/2011-04-05T01Z", new NumberedShardSpec(0, 1))
        ),
        intervalAndShard -> {
          final DataSegment segment = createDataSegment(
              baseDataSource,
              intervalAndShard._1,
              baseVersion,
              Lists.newArrayList("dim1", "dim2", "dim3", "dim4"),
              1024 * 1024,
              intervalAndShard._2,
              null,
              false
          );
          try {
            metadataStorageCoordinator.commitSegments(Sets.newHashSet(segment), null);
            announceSegmentForServer(druidServer, segment, zkPathsConfig, jsonMapper);
          }
          catch (IOException e) {
            return false;
          }
          return true;
        }
    );
    // insert derivative segments
    //hour to hour
    List<Boolean> derivativeResult = Lists.transform(
        ImmutableList.of(
            new Tuple3<>("2011-04-01T01Z/2011-04-01T02Z", 0, 1),
            new Tuple3<>("2011-04-01T01Z/2011-04-01T02Z", 1, 2),
            new Tuple3<>("2011-04-03T00Z/2011-04-03T01Z", 0, 1),
            new Tuple3<>("2011-04-03T00Z/2011-04-03T01Z", 1, 3),
            new Tuple3<>("2011-04-04T00Z/2011-04-04T01Z", 0, 1)
        ),
        intervalAndRange -> {
          final DataSegment segment = createDataSegment(
              dataSource,
              intervalAndRange._1,
              derivativeVersion1,
              Lists.newArrayList("dim1", "dim2", "dim3"),
              1024,
              new NumberedShardSpec(intervalAndRange._2, 0),
              new MaterializedSpec(
                  MaterializedSpec.TYPE_SAME_SEGMENT_GRAN,
                  new BaseShardSpecsSpec(intervalAndRange._2, intervalAndRange._3, baseVersion),
                  null,
                  (short) 1
              ),
              true
          );
          try {
            metadataStorageCoordinator.commitSegments(Sets.newHashSet(segment), null);
            announceSegmentForServer(druidServer, segment, zkPathsConfig, jsonMapper);
          }
          catch (IOException e) {
            return false;
          }
          return true;
        }
    );
    //day materialize hour
    List<Boolean> derivativeResult2 = Lists.transform(
        ImmutableList.of(
            new Tuple4<>("2011-04-01/2011-04-02", (short) 1, 0, 1), //物化了1点到2点的第0号segment(即编号为0,1两个segment)
            new Tuple4<>("2011-04-01/2011-04-02", (short) 1, 1, 2), //物化了1点到2点的第1号segment
            new Tuple4<>("2011-04-03/2011-04-04", (short) 0, 0, 1),  //物化了0点到1点的1个segment
            new Tuple4<>("2011-04-03/2011-04-04", (short) 0, 1, 3)  //物化了0点到1点的1个segment
        ),
        intervalAndRange -> {
          Map<Short, BaseShardSpecsSpec> multiMaterializedSpecs = new HashMap<>();
          multiMaterializedSpecs.put(
              intervalAndRange._2,
              new BaseShardSpecsSpec(intervalAndRange._3, intervalAndRange._4, derivativeVersion1)
          );
          final DataSegment segment = createDataSegment(
              dataSourceDay,
              intervalAndRange._1,
              derivativeVersion2,
              Lists.newArrayList("dim1", "dim2", "dim3"),
              1023,
              new NumberedShardSpec(intervalAndRange._3, 0),
              new MaterializedSpec(MaterializedSpec.TYPE_DIFF_SEGMENT_GRAN, null, multiMaterializedSpecs, (short) 24),
              true
          );
          try {
            metadataStorageCoordinator.commitSegments(Sets.newHashSet(segment), null);
            announceSegmentForServer(druidServer, segment, zkPathsConfig, jsonMapper);
          }
          catch (IOException e) {
            return false;
          }
          return true;
        }
    );

    Assert.assertFalse(baseResult.contains(false));
    Assert.assertFalse(derivativeResult.contains(false));
    Assert.assertFalse(derivativeResult2.contains(false));
    derivativesManager.start();
    while (DerivativeDataSourceManager.getAllDerivatives().isEmpty()) {
      try {
        TimeUnit.SECONDS.sleep(1L);
      }
      catch (InterruptedException e) {
        e.printStackTrace();
      }
    }


    List<Query> expectedQueryAfterOptimizing = Lists.newArrayList(
        new TopNQueryBuilder()
            .dataSource(dataSourceDay)
            .granularity(QueryRunnerTestHelper.ALL_GRAN)
            .dimension("dim1")
            .metric("cost")
            .threshold(4)
            .intervals(new MultipleSpecificSegmentSpec(Arrays.asList(
                new SegmentDescriptor(
                    Intervals.of("2011-04-03/2011-04-04"),
                    derivativeVersion2,
                    0
                ),
                new SegmentDescriptor(
                    Intervals.of("2011-04-03/2011-04-04"),
                    derivativeVersion2,
                    1
                ),
                new SegmentDescriptor(
                    Intervals.of("2011-04-01/2011-04-02"),
                    derivativeVersion2,
                    0
                ),
                new SegmentDescriptor(
                    Intervals.of("2011-04-01/2011-04-02"),
                    derivativeVersion2,
                    1
                )
            ), Collections.singletonList(Intervals.of("2011-04-01/2011-04-06"))))
            .aggregators(new LongSumAggregatorFactory("cost", "cost"))
            .build(),
        new TopNQueryBuilder()
            .dataSource(dataSource)
            .granularity(QueryRunnerTestHelper.ALL_GRAN)
            .dimension("dim1")
            .metric("cost")
            .threshold(4)
            .intervals(new MultipleSpecificSegmentSpec(Collections.singletonList(
                new SegmentDescriptor(
                    Intervals.of("2011-04-04T00Z/2011-04-04T01Z"),
                    derivativeVersion1,
                    0
                )
            ), Collections.singletonList(Intervals.of("2011-04-01/2011-04-06"))))
            .aggregators(new LongSumAggregatorFactory("cost", "cost"))
            .build(),
        new TopNQueryBuilder()
            .dataSource(baseDataSource)
            .granularity(QueryRunnerTestHelper.ALL_GRAN)
            .dimension("dim1")
            .metric("cost")
            .threshold(4)
            .intervals(new MultipleSpecificSegmentSpec(Arrays.asList(
                new SegmentDescriptor(
                    Intervals.of("2011-04-03T00Z/2011-04-03T01Z"),
                    baseVersion,
                    3
                ),
                new SegmentDescriptor(
                    Intervals.of("2011-04-05T00Z/2011-04-05T01Z"),
                    baseVersion,
                    0
                ),
                new SegmentDescriptor(
                    Intervals.of("2011-04-01T00Z/2011-04-01T01Z"),
                    baseVersion,
                    1
                ),
                new SegmentDescriptor(
                    Intervals.of("2011-04-01T00Z/2011-04-01T01Z"),
                    baseVersion,
                    0
                )
            ), Collections.singletonList(Intervals.of("2011-04-01/2011-04-06"))))
            .aggregators(new LongSumAggregatorFactory("cost", "cost"))
            .build()
    );
    List<Query> optimize = optimizer.optimize(userQuery);
    Assert.assertEquals(expectedQueryAfterOptimizing, optimize);
    derivativesManager.stop();
  }

  @Test(timeout = 60_000L)
  public void testOptimizeForMultiOverwriteMV() throws Exception
  {
    // build user query
    TopNQuery userQuery = new TopNQueryBuilder()
        .dataSource(dataSourceDay)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension("dim1")
        .metric("cost")
        .threshold(4)
        .intervals("2011-04-01/2011-04-06")
        .aggregators(new LongSumAggregatorFactory("cost", "cost"))
        .build();
    //init
    DerivativeDataSourceManager mockClient = mockDerivatives(userQuery);
    optimizer = new DataSourceOptimizer(brokerServerView, mockClient);

    // insert datasource metadata
    DerivativeDataSourceMetadata metadata = new DerivativeDataSourceMetadata(
        baseDataSource,
        new ClientTaskGranularitySpec(
            Granularities.DAY,
            Granularities.MINUTE,
            true
        ),
        Collections.<String>emptySet(),
        Collections.<String>emptySet()
    );
    DerivativeDataSourceMetadata metadata2 = new DerivativeDataSourceMetadata(
        dataSource,
        new ClientTaskGranularitySpec(
            Granularities.HOUR,
            Granularities.MINUTE,
            true
        ),
        Collections.<String>emptySet(),
        Collections.<String>emptySet()
    );
    metadataStorageCoordinator.insertDataSourceMetadata(dataSource, metadata);
    metadataStorageCoordinator.insertDataSourceMetadata(dataSourceDay, metadata2);
    // insert base datasource segments
    //被物化的intervals: 2011-04-01T01Z/2011-04-01T02Z, 2011-04-03T00Z/2011-04-03T01Z(只物化了3个),
    // 2011-04-04T00Z/2011-04-04T01Z --被hour粒度物化
    // 2011-04-01T01Z/2011-04-01T02Z,2011-04-03T00Z/2011-04-03T01Z(只物化了3个)--被day、hour粒度物化
    //没有被任何物化视图物化的列表：
    //2011-04-01T00Z/2011-04-01T01Z ,2011-04-03T00Z/2011-04-03T01Z_3, 2011-04-05T00Z/2011-04-05T01Z
    List<Boolean> baseResult = Lists.transform(
        ImmutableList.of(
            new Tuple2<>("2011-04-01T00Z/2011-04-01T01Z", new NumberedShardSpec(0, 0)),
            new Tuple2<>("2011-04-01T00Z/2011-04-01T01Z", new NumberedShardSpec(1, 0)),
            new Tuple2<>("2011-04-01T01Z/2011-04-01T02Z", new NumberedShardSpec(0, 2)),
            new Tuple2<>("2011-04-01T01Z/2011-04-01T02Z", new NumberedShardSpec(1, 2)),
            new Tuple2<>("2011-04-03T00Z/2011-04-03T01Z", new NumberedShardSpec(0, 0)),
            new Tuple2<>("2011-04-03T00Z/2011-04-03T01Z", new NumberedShardSpec(1, 0)),
            new Tuple2<>("2011-04-03T00Z/2011-04-03T01Z", new NumberedShardSpec(2, 0)),
            new Tuple2<>("2011-04-03T00Z/2011-04-03T01Z", new NumberedShardSpec(3, 0)),
            new Tuple2<>("2011-04-04T00Z/2011-04-04T01Z", new NumberedShardSpec(0, 1)),
            new Tuple2<>("2011-04-05T00Z/2011-04-05T01Z", new NumberedShardSpec(0, 1))
        ),
        intervalAndShard -> {
          final DataSegment segment = createDataSegment(
              baseDataSource,
              intervalAndShard._1,
              baseVersion,
              Lists.newArrayList("dim1", "dim2", "dim3", "dim4"),
              1024 * 1024,
              intervalAndShard._2,
              null,
              false
          );
          try {
            metadataStorageCoordinator.commitSegments(Sets.newHashSet(segment), null);
            announceSegmentForServer(druidServer, segment, zkPathsConfig, jsonMapper);
          }
          catch (IOException e) {
            return false;
          }
          return true;
        }
    );
    // insert derivative segments
    //hour粒度由增量切换到全量，发生version变化
    //此interval发生了全量覆盖任务，day粒度针对所在天的物化均失效。
    String newIntervalVersion = "2011-04-03T00Z/2011-04-03T01Z";
    String newVersion = "newV1";
    List<Boolean> derivativeResult = Lists.transform(
        ImmutableList.of(
            new Tuple3<>("2011-04-01T01Z/2011-04-01T02Z", 0, 2),
            new Tuple3<>(newIntervalVersion, 0, 3),
            new Tuple3<>("2011-04-04T00Z/2011-04-04T01Z", 0, 1)
        ),
        intervalAndRange -> {
          final DataSegment segment = createDataSegment(
              dataSource,
              intervalAndRange._1,
              intervalAndRange._1.equals(newIntervalVersion) ? newVersion : derivativeVersion1,
              Lists.newArrayList("dim1", "dim2", "dim3"),
              1024,
              new NumberedShardSpec(intervalAndRange._2, 0),
              new MaterializedSpec(
                  MaterializedSpec.TYPE_SAME_SEGMENT_GRAN,
                  new BaseShardSpecsSpec(intervalAndRange._2, intervalAndRange._3, baseVersion),
                  null,
                  (short) 1
              ),
              true
          );
          try {
            metadataStorageCoordinator.commitSegments(Sets.newHashSet(segment), null);
            announceSegmentForServer(druidServer, segment, zkPathsConfig, jsonMapper);
          }
          catch (IOException e) {
            return false;
          }
          return true;
        }
    );
    //day增量物化hour，hour的2011-04-03T00到01产生了overwrite任务，version发生变更。
    List<Boolean> derivativeResult2 = Lists.transform(
        ImmutableList.of(
            new Tuple4<>("2011-04-01/2011-04-02", (short) 1, 0, 1), //物化了1点到2点的第0号segment(即编号为0,1两个segment)
            new Tuple4<>("2011-04-01/2011-04-02", (short) 1, 1, 2), //物化了1点到2点的第1号segment
            new Tuple4<>("2011-04-03/2011-04-04", (short) 0, 0, 2)  //物化了0点到1点的2个segment
        ),
        intervalAndRange -> {
          Map<Short, BaseShardSpecsSpec> multiMaterializedSpecs = new HashMap<>();
          multiMaterializedSpecs.put(
              intervalAndRange._2,
              new BaseShardSpecsSpec(intervalAndRange._3, intervalAndRange._4, derivativeVersion1)
          );
          final DataSegment segment = createDataSegment(
              dataSourceDay,
              intervalAndRange._1,
              derivativeVersion2,
              Lists.newArrayList("dim1", "dim2", "dim3"),
              1023,
              new NumberedShardSpec(intervalAndRange._3, 0),
              new MaterializedSpec(MaterializedSpec.TYPE_DIFF_SEGMENT_GRAN, null, multiMaterializedSpecs, (short) 24),
              true
          );
          try {
            metadataStorageCoordinator.commitSegments(Sets.newHashSet(segment), null);
            announceSegmentForServer(druidServer, segment, zkPathsConfig, jsonMapper);
          }
          catch (IOException e) {
            return false;
          }
          return true;
        }
    );

    Assert.assertFalse(baseResult.contains(false));
    Assert.assertFalse(derivativeResult.contains(false));
    Assert.assertFalse(derivativeResult2.contains(false));
    derivativesManager.start();
    while (DerivativeDataSourceManager.getAllDerivatives().isEmpty()) {
      try {
        TimeUnit.SECONDS.sleep(1L);
      }
      catch (InterruptedException e) {
        e.printStackTrace();
      }
    }


    List<Query> expectedQueryAfterOptimizing = Lists.newArrayList(
        new TopNQueryBuilder()
            .dataSource(dataSourceDay)
            .granularity(QueryRunnerTestHelper.ALL_GRAN)
            .dimension("dim1")
            .metric("cost")
            .threshold(4)
            .intervals(new MultipleSpecificSegmentSpec(Arrays.asList(
                new SegmentDescriptor(
                    Intervals.of("2011-04-01/2011-04-02"),
                    derivativeVersion2,
                    0
                ),
                new SegmentDescriptor(
                    Intervals.of("2011-04-01/2011-04-02"),
                    derivativeVersion2,
                    1
                )
            ), Collections.singletonList(Intervals.of("2011-04-01/2011-04-06"))))
            .aggregators(new LongSumAggregatorFactory("cost", "cost"))
            .build(),
        new TopNQueryBuilder()
            .dataSource(dataSource)
            .granularity(QueryRunnerTestHelper.ALL_GRAN)
            .dimension("dim1")
            .metric("cost")
            .threshold(4)
            .intervals(new MultipleSpecificSegmentSpec(Arrays.asList(
                new SegmentDescriptor(
                    Intervals.of(newIntervalVersion),
                    newVersion,
                    0
                ),
                new SegmentDescriptor(
                    Intervals.of("2011-04-04T00Z/2011-04-04T01Z"),
                    derivativeVersion1,
                    0
                )
            ), Collections.singletonList(Intervals.of("2011-04-01/2011-04-06"))))
            .aggregators(new LongSumAggregatorFactory("cost", "cost"))
            .build(),
        new TopNQueryBuilder()
            .dataSource(baseDataSource)
            .granularity(QueryRunnerTestHelper.ALL_GRAN)
            .dimension("dim1")
            .metric("cost")
            .threshold(4)
            .intervals(new MultipleSpecificSegmentSpec(Arrays.asList(
                new SegmentDescriptor(
                    Intervals.of("2011-04-03T00Z/2011-04-03T01Z"),
                    baseVersion,
                    3
                ),
                new SegmentDescriptor(
                    Intervals.of("2011-04-05T00Z/2011-04-05T01Z"),
                    baseVersion,
                    0
                ),
                new SegmentDescriptor(
                    Intervals.of("2011-04-01T00Z/2011-04-01T01Z"),
                    baseVersion,
                    1
                ),
                new SegmentDescriptor(
                    Intervals.of("2011-04-01T00Z/2011-04-01T01Z"),
                    baseVersion,
                    0
                )
            ), Collections.singletonList(Intervals.of("2011-04-01/2011-04-06"))))
            .aggregators(new LongSumAggregatorFactory("cost", "cost"))
            .build()
    );
    List<Query> optimize = optimizer.optimize(userQuery);
    Assert.assertEquals(expectedQueryAfterOptimizing, optimize);
    derivativesManager.stop();
  }

//  @Test(timeout = 60_000L)
  public void testOptimizeForHistorySegments() throws Exception
  {
    // build user query
    TopNQuery userQuery = new TopNQueryBuilder()
        .dataSource(dataSourceDay)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension("dim1")
        .metric("cost")
        .threshold(4)
        .intervals("2011-04-01/2011-04-06")
        .aggregators(new LongSumAggregatorFactory("cost", "cost"))
        .build();

    //init
    DerivativeDataSourceManager mockClient = mockDerivatives(userQuery);
    optimizer = new DataSourceOptimizer(brokerServerView, mockClient);

    // insert datasource metadata
    DerivativeDataSourceMetadata metadata = new DerivativeDataSourceMetadata(
        baseDataSource,
        new ClientTaskGranularitySpec(
            Granularities.HOUR,
            Granularities.MINUTE,
            true
        ),
        Collections.<String>emptySet(),
        Collections.<String>emptySet()
    );
    DerivativeDataSourceMetadata metadata2 = new DerivativeDataSourceMetadata(
        dataSource,
        new ClientTaskGranularitySpec(
            Granularities.DAY,
            Granularities.MINUTE,
            true
        ),
        Collections.<String>emptySet(),
        Collections.<String>emptySet()
    );
    DerivativeDataSourceMetadata metadata3 = new DerivativeDataSourceMetadata(
        dataSourceDay,
        new ClientTaskGranularitySpec(
            Granularities.DAY,
            Granularities.MINUTE,
            true
        ),
        Collections.<String>emptySet(),
        Collections.<String>emptySet()
    );
    metadataStorageCoordinator.insertDataSourceMetadata(baseDataSource, metadata);
    metadataStorageCoordinator.insertDataSourceMetadata(dataSource, metadata2);
    metadataStorageCoordinator.insertDataSourceMetadata(dataSourceDay, metadata3);
    // insert base datasource segments
    //被物化的intervals:
    // 2011-04-01T01Z/2011-04-01T02Z(0,1), 2011-04-03T00Z/2011-04-03T01Z(物化了3个),2011-04-04T00Z/2011-04-04T01Z --被hour粒度物化
    // 2011-04-01T01Z/2011-04-01T02Z(0,1)  --被day粒度物化
    //没有被任何物化视图物化的列表：
    // 2011-04-01T00Z/2011-04-01T01Z(0,1) ,2011-04-03T00Z/2011-04-03T01Z_3, 2011-04-05T00Z/2011-04-05T01Z
    List<Boolean> baseResult = Lists.transform(
        ImmutableList.of(
            new Tuple2<>("2011-04-01T00Z/2011-04-01T01Z", new NumberedShardSpec(0, 0)),
            new Tuple2<>("2011-04-01T00Z/2011-04-01T01Z", new NumberedShardSpec(1, 0)),
            new Tuple2<>("2011-04-01T01Z/2011-04-01T02Z", new NumberedShardSpec(0, 2)),
            new Tuple2<>("2011-04-01T01Z/2011-04-01T02Z", new NumberedShardSpec(1, 2)),
            new Tuple2<>("2011-04-03T00Z/2011-04-03T01Z", new NumberedShardSpec(0, 0)),
            new Tuple2<>("2011-04-03T00Z/2011-04-03T01Z", new NumberedShardSpec(1, 0)),
            new Tuple2<>("2011-04-03T00Z/2011-04-03T01Z", new NumberedShardSpec(2, 0)),
            new Tuple2<>("2011-04-03T00Z/2011-04-03T01Z", new NumberedShardSpec(3, 0)),
            new Tuple2<>("2011-04-04T00Z/2011-04-04T01Z", new NumberedShardSpec(0, 1)),
            new Tuple2<>("2011-04-05T00Z/2011-04-05T01Z", new NumberedShardSpec(0, 1))
        ),
        intervalAndShard -> {
          final DataSegment segment = createDataSegment(
              baseDataSource,
              intervalAndShard._1,
              baseVersion,
              Lists.newArrayList("dim1", "dim2", "dim3", "dim4"),
              1024 * 1024,
              intervalAndShard._2,
              null,
              false
          );
          try {
            metadataStorageCoordinator.commitSegments(Sets.newHashSet(segment), null);
            announceSegmentForServer(druidServer, segment, zkPathsConfig, jsonMapper);
          }
          catch (IOException e) {
            return false;
          }
          return true;
        }
    );
    // insert derivative segments
    //hour to hour
    final String overwriteInterval = "2011-04-03T00Z/2011-04-03T01Z";
    List<Boolean> derivativeResult = Lists.transform(
        ImmutableList.of(
            new Tuple3<>("2011-04-01T01Z/2011-04-01T02Z", 0, 1),
            new Tuple3<>("2011-04-01T01Z/2011-04-01T02Z", 1, 2),
            new Tuple3<>(overwriteInterval, 0, 3), // 全量物化了0,1,2 新增的第3个segment还没有物化
            new Tuple3<>("2011-04-04T00Z/2011-04-04T01Z", 0, 1)
        ),
        intervalAndRange -> {
          final DataSegment segment = createDataSegment(
              dataSource,
              intervalAndRange._1,
              derivativeVersion1,
              Lists.newArrayList("dim1", "dim2", "dim3"),
              10,
              new NumberedShardSpec(intervalAndRange._2, 0),
              overwriteInterval.equals(intervalAndRange._1) ? new MaterializedSpec(
                  MaterializedSpec.TYPE_SAME_SEGMENT_GRAN,
                  new BaseShardSpecsSpec(
                      intervalAndRange._2,
                      intervalAndRange._3,
                      baseVersion
                  ),
                  null,
                  (short) 1
              ) : null,
              true
          );
          try {
            metadataStorageCoordinator.commitSegments(Sets.newHashSet(segment), null);
            announceSegmentForServer(druidServer, segment, zkPathsConfig, jsonMapper);
          }
          catch (IOException e) {
            return false;
          }
          return true;
        }
    );

    //day materialize hour
    List<Boolean> derivativeResult2 = Lists.transform(
        ImmutableList.of(
            new Tuple4<>("2011-04-01/2011-04-02", (short) 1, 0, 1), //物化了1点到2点的第0号segment
            new Tuple4<>("2011-04-01/2011-04-02", (short) 1, 1, 2), //物化了1点到2点的第1号segment
            new Tuple4<>("2011-04-04/2011-04-05", (short) 0, 0, 1)
        ),
        intervalAndRange -> {
          final DataSegment segment = createDataSegment(
              dataSourceDay,
              intervalAndRange._1,
              derivativeVersion2,
              Lists.newArrayList("dim1", "dim2", "dim3"),
              1023,
              new NumberedShardSpec(intervalAndRange._3, 0),
              null,
              true
          );
          try {
            metadataStorageCoordinator.commitSegments(Sets.newHashSet(segment), null);
            announceSegmentForServer(druidServer, segment, zkPathsConfig, jsonMapper);
          }
          catch (IOException e) {
            return false;
          }
          return true;
        }
    );

    Assert.assertFalse(baseResult.contains(false));
    Assert.assertFalse(derivativeResult.contains(false));
    Assert.assertFalse(derivativeResult2.contains(false));
    derivativesManager.start();
    while (DerivativeDataSourceManager.getAllDerivatives().isEmpty()) {
      try {
        TimeUnit.SECONDS.sleep(1L);
      }
      catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    List<Query> expectedQueryAfterOptimizing = Lists.newArrayList(
        new TopNQueryBuilder()
            .dataSource(dataSourceDay)
            .granularity(QueryRunnerTestHelper.ALL_GRAN)
            .dimension("dim1")
            .metric("cost")
            .threshold(4)
            .intervals(new MultipleSpecificSegmentSpec(Arrays.asList(
                new SegmentDescriptor(
                    Intervals.of("2011-04-01/2011-04-02"),
                    derivativeVersion2,
                    0
                ),
                new SegmentDescriptor(
                    Intervals.of("2011-04-01/2011-04-02"),
                    derivativeVersion2,
                    1
                ),
                new SegmentDescriptor(
                    Intervals.of("2011-04-04/2011-04-05"),
                    derivativeVersion2,
                    0
                )
            ), Collections.singletonList(Intervals.of("2011-04-01/2011-04-06"))))
            .aggregators(new LongSumAggregatorFactory("cost", "cost"))
            .build(),
        new TopNQueryBuilder()
            .dataSource(dataSource)
            .granularity(QueryRunnerTestHelper.ALL_GRAN)
            .dimension("dim1")
            .metric("cost")
            .threshold(4)
            .intervals(new MultipleSpecificSegmentSpec(Collections.singletonList(
                new SegmentDescriptor(
                    Intervals.of("2011-04-03T00Z/2011-04-03T01Z"),
                    derivativeVersion1,
                    0
                )
            ), Collections.singletonList(Intervals.of("2011-04-01/2011-04-06"))))
            .aggregators(new LongSumAggregatorFactory("cost", "cost"))
            .build(),
        new TopNQueryBuilder()
            .dataSource(baseDataSource)
            .granularity(QueryRunnerTestHelper.ALL_GRAN)
            .dimension("dim1")
            .metric("cost")
            .threshold(4)
            .intervals(new MultipleSpecificSegmentSpec(Arrays.asList(
                new SegmentDescriptor(
                    Intervals.of("2011-04-03T00Z/2011-04-03T01Z"),
                    baseVersion,
                    3
                ),
                new SegmentDescriptor(
                    Intervals.of("2011-04-05T00Z/2011-04-05T01Z"),
                    baseVersion,
                    0
                )
            ), Collections.singletonList(Intervals.of("2011-04-01/2011-04-06"))))
            .aggregators(new LongSumAggregatorFactory("cost", "cost"))
            .build()
    );
    List<Query> optimize = optimizer.optimize(userQuery);
    Assert.assertEquals(expectedQueryAfterOptimizing, optimize);
    derivativesManager.stop();
  }

//  @Test(timeout = 60_000L)
  public void testOptimizeForNotPushDownToOriginBase() throws Exception
  {
    // build user query
    String queryIntervals = "2011-04-01T01/2011-04-01T02";
    TopNQuery userQuery = new TopNQueryBuilder()
        .dataSource(dataSourceDay)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension("dim1")
        .metric("cost")
        .threshold(4)
        .intervals(queryIntervals)
        .aggregators(new LongSumAggregatorFactory("cost", "cost"))
        .build();
    //init
    DerivativeDataSourceManager mockClient = mockDerivatives(userQuery);
    optimizer = new DataSourceOptimizer(brokerServerView, mockClient);

    // insert datasource metadata
    DerivativeDataSourceMetadata metadata = new DerivativeDataSourceMetadata(
        baseDataSource,
        new ClientTaskGranularitySpec(
            Granularities.HOUR,
            Granularities.MINUTE,
            true
        ),
        Collections.<String>emptySet(),
        Collections.<String>emptySet()
    );
    DerivativeDataSourceMetadata metadata2 = new DerivativeDataSourceMetadata(
        dataSource,
        new ClientTaskGranularitySpec(
            Granularities.DAY,
            Granularities.MINUTE,
            true
        ),
        Collections.<String>emptySet(),
        Collections.<String>emptySet()
    );
    DerivativeDataSourceMetadata metadata3 = new DerivativeDataSourceMetadata(
        dataSourceDay,
        new ClientTaskGranularitySpec(
            Granularities.DAY,
            Granularities.MINUTE,
            true
        ),
        Collections.<String>emptySet(),
        Collections.<String>emptySet()
    );
    metadataStorageCoordinator.insertDataSourceMetadata(baseDataSource, metadata);
    metadataStorageCoordinator.insertDataSourceMetadata(dataSource, metadata2);
    metadataStorageCoordinator.insertDataSourceMetadata(dataSourceDay, metadata3);
    // insert base datasource segments
    //被物化的intervals:
    // 2011-04-01T01Z/2011-04-01T02Z(0,1), 2011-04-03T00Z/2011-04-03T01Z(物化了3个),2011-04-04T00Z/2011-04-04T01Z --被hour粒度物化
    // 2011-04-01T01Z/2011-04-01T02Z(0,1)  --被day粒度物化
    //没有被任何物化视图物化的列表：
    // 2011-04-01T00Z/2011-04-01T01Z(0,1) ,2011-04-03T00Z/2011-04-03T01Z_3, 2011-04-05T00Z/2011-04-05T01Z
    List<Boolean> baseResult = Lists.transform(
        ImmutableList.of(
            new Tuple2<>("2011-04-01T00Z/2011-04-01T01Z", new NumberedShardSpec(0, 0)),
            new Tuple2<>("2011-04-01T00Z/2011-04-01T01Z", new NumberedShardSpec(1, 0)),
            new Tuple2<>("2011-04-01T01Z/2011-04-01T02Z", new NumberedShardSpec(0, 2)),
            new Tuple2<>("2011-04-01T01Z/2011-04-01T02Z", new NumberedShardSpec(1, 2)),
            new Tuple2<>("2011-04-03T00Z/2011-04-03T01Z", new NumberedShardSpec(0, 0)),
            new Tuple2<>("2011-04-03T00Z/2011-04-03T01Z", new NumberedShardSpec(1, 0)),
            new Tuple2<>("2011-04-03T00Z/2011-04-03T01Z", new NumberedShardSpec(2, 0)),
            new Tuple2<>("2011-04-03T00Z/2011-04-03T01Z", new NumberedShardSpec(3, 0)),
            new Tuple2<>("2011-04-04T00Z/2011-04-04T01Z", new NumberedShardSpec(0, 1)),
            new Tuple2<>("2011-04-05T00Z/2011-04-05T01Z", new NumberedShardSpec(0, 1))
        ),
        intervalAndShard -> {
          final DataSegment segment = createDataSegment(
              baseDataSource,
              intervalAndShard._1,
              baseVersion,
              Lists.newArrayList("dim1", "dim2", "dim3", "dim4"),
              1024 * 1024,
              intervalAndShard._2,
              null,
              false
          );
          try {
            metadataStorageCoordinator.commitSegments(Sets.newHashSet(segment), null);
            announceSegmentForServer(druidServer, segment, zkPathsConfig, jsonMapper);
          }
          catch (IOException e) {
            return false;
          }
          return true;
        }
    );
    // insert derivative segments
    //hour to hour:
    final String overwriteInterval = "2011-04-03T00Z/2011-04-03T01Z";
    List<Boolean> derivativeResult = Lists.transform(
        ImmutableList.of(
            new Tuple3<>("2011-04-01T01Z/2011-04-01T02Z", 0, 1),
            new Tuple3<>("2011-04-01T01Z/2011-04-01T02Z", 1, 2),
            new Tuple3<>(overwriteInterval, 0, 3), // 全量物化了0,1,2 新增的第3个segment还没有物化
            new Tuple3<>("2011-04-04T00Z/2011-04-04T01Z", 0, 1)
        ),
        intervalAndRange -> {
          final DataSegment segment = createDataSegment(
              dataSource,
              intervalAndRange._1,
              derivativeVersion1,
              Lists.newArrayList("dim1", "dim2", "dim3"),
              10,
              new NumberedShardSpec(intervalAndRange._2, 0),
              overwriteInterval.equals(intervalAndRange._1) ? new MaterializedSpec(
                  MaterializedSpec.TYPE_SAME_SEGMENT_GRAN,
                  new BaseShardSpecsSpec(
                      intervalAndRange._2,
                      intervalAndRange._3,
                      baseVersion
                  ),
                  null,
                  (short) 1
              ) : null,
              true
          );
          try {
            metadataStorageCoordinator.commitSegments(Sets.newHashSet(segment), null);
            announceSegmentForServer(druidServer, segment, zkPathsConfig, jsonMapper);
          }
          catch (IOException e) {
            return false;
          }
          return true;
        }
    );

    //day materialize hour
    List<Boolean> derivativeResult2 = Lists.transform(
        ImmutableList.of(
            new Tuple4<>("2011-04-01/2011-04-02", (short) 1, 0, 1), //物化了1点到2点的第0号segment
            new Tuple4<>("2011-04-01/2011-04-02", (short) 1, 1, 2) //物化了1点到2点的第1号segment
        ),
        intervalAndRange -> {
          final DataSegment segment = createDataSegment(
              dataSourceDay,
              intervalAndRange._1,
              derivativeVersion2,
              Lists.newArrayList("dim1", "dim2", "dim3"),
              1023,
              new NumberedShardSpec(intervalAndRange._3, 0),
              null,
              true
          );
          try {
            metadataStorageCoordinator.commitSegments(Sets.newHashSet(segment), null);
            announceSegmentForServer(druidServer, segment, zkPathsConfig, jsonMapper);
          }
          catch (IOException e) {
            return false;
          }
          return true;
        }
    );

    Assert.assertFalse(baseResult.contains(false));
    Assert.assertFalse(derivativeResult.contains(false));
    Assert.assertFalse(derivativeResult2.contains(false));
    derivativesManager.start();
    while (DerivativeDataSourceManager.getAllDerivatives().isEmpty()) {
      try {
        TimeUnit.SECONDS.sleep(1L);
      }
      catch (InterruptedException e) {
        e.printStackTrace();
      }
    }


    List<Query> expectedQueryAfterOptimizing = Collections.singletonList(
        new TopNQueryBuilder()
            .dataSource(dataSourceDay)
            .granularity(QueryRunnerTestHelper.ALL_GRAN)
            .dimension("dim1")
            .metric("cost")
            .threshold(4)
            .intervals(new MultipleSpecificSegmentSpec(Arrays.asList(
                new SegmentDescriptor(
                    Intervals.of("2011-04-01/2011-04-02"),
                    derivativeVersion2,
                    0
                ),
                new SegmentDescriptor(
                    Intervals.of("2011-04-01/2011-04-02"),
                    derivativeVersion2,
                    1
                )
            ), Collections.singletonList(Intervals.of(queryIntervals))))
            .aggregators(new LongSumAggregatorFactory("cost", "cost"))
            .build()
    );
    List<Query> optimize = optimizer.optimize(userQuery);
    // 对 list1 进行排序
    List<Query> sortedList1 = expectedQueryAfterOptimizing.stream().sorted().collect(Collectors.toList());
    // 对 list2 进行排序
    List<Query> sortedList2 = optimize.stream().sorted().collect(Collectors.toList());
    Assert.assertEquals(sortedList1, sortedList2);

    String queryIntervals2 = "2011-04-04/2011-04-05";
    TopNQuery userQuery2 = new TopNQueryBuilder()
        .dataSource(dataSourceDay)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension("dim1")
        .metric("cost")
        .threshold(4)
        .intervals(queryIntervals2)
        .aggregators(new LongSumAggregatorFactory("cost", "cost"))
        .build();

    List<Query> expectedQueryAfterOptimizing2 = Collections.singletonList(
        new TopNQueryBuilder()
            .dataSource(dataSource)
            .granularity(QueryRunnerTestHelper.ALL_GRAN)
            .dimension("dim1")
            .metric("cost")
            .threshold(4)
            .intervals(new MultipleSpecificSegmentSpec(Collections.singletonList(
                new SegmentDescriptor(
                    Intervals.of("2011-04-04T00Z/2011-04-04T01Z"),
                    derivativeVersion1,
                    0
                )
            ), Collections.singletonList(Intervals.of(queryIntervals2))))
            .aggregators(new LongSumAggregatorFactory("cost", "cost"))
            .build()
    );
    List<Query> optimize2 = optimizer.optimize(userQuery2);
    Assert.assertEquals(expectedQueryAfterOptimizing2, optimize2);
    derivativesManager.stop();
  }

  private DataSegment createDataSegment(
      String name,
      String intervalStr,
      String version,
      List<String> dims,
      long size,
      @Nullable ShardSpec shardSpec,
      @Nullable MaterializedSpec materializedSegment,
      boolean isMaterialized
  )
  {
    DataSegment.Builder ds = DataSegment.builder()
                                        .dataSource(name)
                                        .interval(Intervals.of(intervalStr))
                                        .loadSpec(
                                            ImmutableMap.of(
                                                "type",
                                                "local",
                                                "path",
                                                "somewhere"
                                            )
                                        )
                                        .version(version)
                                        .dimensions(dims)
                                        .metrics(ImmutableList.of("cost"))
                                        .shardSpec(shardSpec == null ? NoneShardSpec.instance() : shardSpec)
                                        .binaryVersion(9)
                                        .size(size);
    if (isMaterialized) {
      ds.storeMaterializedSegment(materializedSegment);
    }
    return ds.build();
  }

  private void setupViews(DerivativeDataSourceManager mockClient) throws Exception
  {
    baseView = new BatchServerInventoryView(zkPathsConfig, curator, jsonMapper, Predicates.alwaysTrue(), "test")
    {
      @Override
      public void registerSegmentCallback(Executor exec, final SegmentCallback callback)
      {
        super.registerSegmentCallback(
            exec,
            new SegmentCallback()
            {
              @Override
              public CallbackAction segmentAdded(DruidServerMetadata server, DataSegment segment)
              {
                return callback.segmentAdded(server, segment);
              }

              @Override
              public CallbackAction segmentRemoved(DruidServerMetadata server, DataSegment segment)
              {
                return callback.segmentRemoved(server, segment);
              }

              @Override
              public CallbackAction segmentViewInitialized()
              {
                return callback.segmentViewInitialized();
              }

              @Override
              public CallbackAction segmentSchemasAnnounced(SegmentSchemas segmentSchemas)
              {
                return CallbackAction.CONTINUE;
              }
            }
        );
      }
    };
    DirectDruidClientFactory druidClientFactory = new DirectDruidClientFactory(
        new NoopServiceEmitter(),
        EasyMock.createMock(QueryToolChestWarehouse.class),
        EasyMock.createMock(QueryWatcher.class),
        getSmileMapper(),
        EasyMock.createMock(HttpClient.class)
    );
    brokerServerView = new BrokerServerView(
        EasyMock.createMock(QueryToolChestWarehouse.class),
        EasyMock.createMock(QueryWatcher.class),
        getSmileMapper(),
        EasyMock.createMock(HttpClient.class),
        druidClientFactory,
        baseView,
        new HighestPriorityTierSelectorStrategy(new RandomServerSelectorStrategy()),
        new NoopServiceEmitter(),
        new BrokerSegmentWatcherConfig(),
        mockClient
    );
    baseView.start();
  }

  private ObjectMapper getSmileMapper()
  {
    final SmileFactory smileFactory = new SmileFactory();
    smileFactory.configure(SmileGenerator.Feature.ENCODE_BINARY_AS_7BIT, false);
    smileFactory.delegateToTextual(true);
    final ObjectMapper retVal = new DefaultObjectMapper(smileFactory, null);
    retVal.getFactory().setCodec(retVal);
    return retVal;
  }
}
