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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.druid.client.materializedview.ClientTaskGranularitySpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.indexing.common.RetryPolicyConfig;
import org.apache.druid.indexing.common.RetryPolicyFactory;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskQueue;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManagerConfig;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import org.apache.druid.metadata.MetadataSupervisorManager;
import org.apache.druid.metadata.SqlSegmentsMetadataManager;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.expression.LookupEnabledTestExprMacroTable;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.metadata.SegmentSchemaManager;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.segment.realtime.firehose.NoopChatHandlerProvider;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.timeline.BaseShardSpecsSpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.MaterializedSpec;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MaterializedViewSupervisorSpecTest
{
  static final String BASE_DATA_SOURCE = "base";
  private static final String DATA_SOURCE = "test";
  static final RetryPolicyFactory RETRY_POLICY_FACTORY = new RetryPolicyFactory(new RetryPolicyConfig());
  private final Map<String, Object> context = new HashMap<>();
  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private final ObjectMapper objectMapper = TestHelper.makeJsonMapper();
  private NativeBatchMaterializedViewSupervisorSpec hourSpec;
  private NativeBatchMaterializedViewSupervisorSpec daySpec;
  private TaskStorage taskStorage;
  private TaskMaster taskMaster;
  private IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  private MetadataSupervisorManager metadataSupervisorManager;
  private SqlSegmentsMetadataManager sqlSegmentsMetadataManager;
  private TaskQueue taskQueue;
  private MaterializedViewSupervisor supervisor;
  private SegmentCacheManagerFactory segmentCacheManagerFactory;
  private SegmentSchemaManager segmentSchemaManager;
  private PolicyConfig policyConfig;
  private List<DataSegment> dataSegments = new ArrayList<>();
  @Before
  public void setup()
  {
    TestDerbyConnector derbyConnector = derbyConnectorRule.getConnector();
    derbyConnector.createDataSourceTable();
    derbyConnector.createSegmentTable();
    segmentCacheManagerFactory = new SegmentCacheManagerFactory(objectMapper);
    objectMapper.registerSubtypes(new NamedType(NativeBatchMaterializedViewSupervisorSpec.class, "materialized_view"));
    objectMapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(TaskMaster.class, null)
            .addValue(TaskStorage.class, null)
            .addValue(ExprMacroTable.class.getName(), LookupEnabledTestExprMacroTable.INSTANCE)
            .addValue(ObjectMapper.class, objectMapper)
            .addValue(MetadataSupervisorManager.class, null)
            .addValue(SqlSegmentsMetadataManager.class, null)
            .addValue(IndexerMetadataStorageCoordinator.class, null)
            .addValue(MaterializedViewTaskConfig.class, new MaterializedViewTaskConfig())
            .addValue(AuthorizerMapper.class, EasyMock.createMock(AuthorizerMapper.class))
            .addValue(ChatHandlerProvider.class, new NoopChatHandlerProvider())
            .addValue(SupervisorStateManagerConfig.class, new SupervisorStateManagerConfig())
            .addValue(SegmentCacheManagerFactory.class, segmentCacheManagerFactory)
            .addValue(RetryPolicyFactory.class, new RetryPolicyFactory(new RetryPolicyConfig()))
            .addValue(ParallelIndexTuningConfig.class, ParallelIndexTuningConfig.defaultConfig())

    );

    taskStorage = EasyMock.createMock(TaskStorage.class);
    taskMaster = EasyMock.createMock(TaskMaster.class);
    segmentSchemaManager = new SegmentSchemaManager(
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        objectMapper,
        derbyConnector
    );
    indexerMetadataStorageCoordinator = new IndexerSQLMetadataStorageCoordinator(
        objectMapper,
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        derbyConnector,
        segmentSchemaManager,
        CentralizedDatasourceSchemaConfig.create()
    );
    metadataSupervisorManager = EasyMock.createMock(MetadataSupervisorManager.class);
    sqlSegmentsMetadataManager = EasyMock.createMock(SqlSegmentsMetadataManager.class);

    hourSpec = getSpec(Granularities.HOUR);
    daySpec = getSpec(Granularities.DAY);

    dataSegments.add(new DataSegment(
        "base",
        Intervals.of("2015-01-01T00Z/2015-01-01T01Z"),
        "test_version01",
        ImmutableMap.of(),
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("m1"),
        new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null, null),
        9,
        1024
    ));
    dataSegments.add(new DataSegment(
        "base",
        Intervals.of("2015-01-01T23Z/2015-01-02T00Z"),
        "test_version02",
        ImmutableMap.of(),
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("m1"),
        new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null, null),
        9,
        1024
    ));
  }

  private NativeBatchMaterializedViewSupervisorSpec getSpec(Granularity segmentGranularity)
  {
    return new NativeBatchMaterializedViewSupervisorSpec(
        BASE_DATA_SOURCE,
        new DimensionsSpec(Collections.singletonList(new StringDimensionSchema("user"))),
        new AggregatorFactory[]{new LongSumAggregatorFactory("m1", "m1")},
        new ClientTaskGranularitySpec(segmentGranularity, segmentGranularity, null),
        null,
        ParallelIndexTuningConfig.defaultConfig(),
        DATA_SOURCE,
        policyConfig,
        context,
        false,
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
  }

  @Test
  public void testSupervisorSerialization() throws IOException
  {
    String supervisorStr = "{\n" +
                           "  \"type\" : \"materialized_view\",\n" +
                           "  \"baseDataSource\": \"base\",\n" +
                           "  \"dataSource\": \"test\",\n" +
                           "  \"dimensionsSpec\":{\n" +
                           "            \"dimensions\" : [\n" +
                           "              \"user\"\n" +
                           "            ]\n" +
                           "          },\n" +
                           "    \"metricsSpec\" : [\n" +
                           "        {\n" +
                           "          \"name\" : \"m1\",\n" +
                           "          \"type\" : \"longSum\",\n" +
                           "          \"fieldName\" : \"m1\"\n" +
                           "        }\n" +
                           "      ],\n" +
                           "  \"tuningConfig\": {\n" +
                           "      \"type\" : \"index_parallel\"\n" +
                           "  },\n" +
                           "  \"granularitySpec\": {\n" +
                           "      \"queryGranularity\" : \"hour\"\n" +
                           "  }\n" +
                           "}";
    NativeBatchMaterializedViewSupervisorSpec expected = getSpec(Granularities.HOUR);
    NativeBatchMaterializedViewSupervisorSpec spec = objectMapper.readValue(
        supervisorStr,
        NativeBatchMaterializedViewSupervisorSpec.class
    );
    Assert.assertEquals(expected.getBaseDataSource(), spec.getBaseDataSource());
    Assert.assertEquals(expected.getId(), spec.getId());
    Assert.assertEquals(expected.getDataSourceName(), spec.getDataSourceName());
    Assert.assertEquals(expected.getDimensions(), spec.getDimensions());
    Assert.assertEquals(expected.getMetrics(), spec.getMetrics());
  }

  @Test
  public void testSuspendResuume() throws IOException
  {
    String supervisorStr = "{\n" +
                           "  \"type\" : \"materialized_view\",\n" +
                           "  \"baseDataSource\": \"wikiticker\",\n" +
                           "  \"dataSource\": \"test\",\n" +
                           "  \"dimensionsSpec\":{\n" +
                           "            \"dimensions\" : [\n" +
                           "              \"isUnpatrolled\",\n" +
                           "              \"metroCode\",\n" +
                           "              \"namespace\",\n" +
                           "              \"page\",\n" +
                           "              \"regionIsoCode\",\n" +
                           "              \"regionName\",\n" +
                           "              \"user\"\n" +
                           "            ]\n" +
                           "          },\n" +
                           "    \"metricsSpec\" : [\n" +
                           "        {\n" +
                           "          \"name\" : \"count\",\n" +
                           "          \"type\" : \"count\"\n" +
                           "        },\n" +
                           "        {\n" +
                           "          \"name\" : \"added\",\n" +
                           "          \"type\" : \"longSum\",\n" +
                           "          \"fieldName\" : \"added\"\n" +
                           "        }\n" +
                           "      ],\n" +
                           "  \"tuningConfig\": {\n" +
                           "      \"type\" : \"index_parallel\"\n" +
                           "  },\n" +
                           "  \"granularitySpec\": {\n" +
                           "      \"queryGranularity\" : \"hour\"\n" +
                           "  }\n" +
                           "}";

    NativeBatchMaterializedViewSupervisorSpec spec = objectMapper.readValue(
        supervisorStr,
        NativeBatchMaterializedViewSupervisorSpec.class
    );
    Assert.assertFalse(spec.isSuspended());

    String suspendedSerialized = objectMapper.writeValueAsString(spec.createSuspendedSpec());
    NativeBatchMaterializedViewSupervisorSpec suspendedSpec = objectMapper.readValue(
        suspendedSerialized,
        NativeBatchMaterializedViewSupervisorSpec.class
    );
    Assert.assertTrue(suspendedSpec.isSuspended());

    String runningSerialized = objectMapper.writeValueAsString(spec.createRunningSpec());
    NativeBatchMaterializedViewSupervisorSpec runningSpec = objectMapper.readValue(
        runningSerialized,
        NativeBatchMaterializedViewSupervisorSpec.class
    );
    Assert.assertFalse(runningSpec.isSuspended());
  }

  @Test
  public void testCompareSegmentGranType()
  {

    byte diffGran = daySpec.compareSegmentGranType(dataSegments);
    byte sameGran = hourSpec.compareSegmentGranType(dataSegments);
    Assert.assertEquals(diffGran == MaterializedSpec.TYPE_DIFF_SEGMENT_GRAN, true);
    Assert.assertEquals(sameGran == MaterializedSpec.TYPE_SAME_SEGMENT_GRAN, true);

    // TWO HOUR segment
    dataSegments.add(new DataSegment(
        "base",
        Intervals.of("2022-01-01T01Z/2022-01-01T03Z"),
        "test_version02",
        ImmutableMap.of(),
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("m1"),
        new HashBasedNumberedShardSpec(0, 1, 0, 1, null, null, null),
        9,
        1024
    ));

    byte diffGran2 = daySpec.compareSegmentGranType(dataSegments);
    byte sameGran2 = hourSpec.compareSegmentGranType(dataSegments);
    Assert.assertEquals(diffGran2 == MaterializedSpec.TYPE_UNSUPPORT_GRAN, true);
    Assert.assertEquals(sameGran2 == MaterializedSpec.TYPE_UNSUPPORT_GRAN, true);

  }

  @Test
  public void testGenerateMaterializedSegmentForOriginType()
  {
    // test different segment granularity and origin partitionIds store
    dataSegments = createOriginStoreAndDiffGranDataSegments();
    String exceptionStr = null;
    try {
      daySpec.generateMaterializedSegment(dataSegments);
    }
    catch (Exception e) {
      exceptionStr = e.getMessage();
    }
    String expectedException = "WTF? not support type[-1], bucause baseDataSource["
                               + daySpec.getBaseDataSource()
                               + "] segments exists different segment granularity or larger than materializedview granularity";
    Assert.assertEquals(expectedException, exceptionStr);


    // test hour segment granularity and origin partitionIds store
    dataSegments = createOriginStoreAndHourGranDataSegments();
    daySpec.generateMaterializedSegment(dataSegments);
    final Map<String, Object> storeMaterializedSegmentMap = (Map<String, Object>) context.get(Tasks.CONTEXT_KEY_STORE_MATERIALIZED_SEGMENTS);
    MaterializedSpec materializedSpec = objectMapper.convertValue(
        storeMaterializedSegmentMap,
        new TypeReference<MaterializedSpec>()
        {
        }
    );

    Map<Short, BaseShardSpecsSpec> multiSegmentGrans = new HashMap<>();
    multiSegmentGrans.put(
        (short) 1, // intervalId
        new BaseShardSpecsSpec(0, 3, "test_version01")
    );
    multiSegmentGrans.put(
        (short) 2, // intervalId
        new BaseShardSpecsSpec(0, 2, "test_version01")
    );
    MaterializedSpec expectedMaterializedSpec = new MaterializedSpec(
        MaterializedSpec.TYPE_DIFF_SEGMENT_GRAN,
        null,
        multiSegmentGrans,
        (short) 24
    );

    Assert.assertEquals(
        true,
        materializedSpec.getBaseShardSpecsSpec() == null && materializedSpec.getSourceBaseShardSpecsSpecs() != null
    );
    Map<Short, BaseShardSpecsSpec> expected = expectedMaterializedSpec.getSourceBaseShardSpecsSpecs();
    Map<Short, BaseShardSpecsSpec> actual = materializedSpec.getSourceBaseShardSpecsSpecs();
    for (Map.Entry<Short, BaseShardSpecsSpec> entry : expected.entrySet()) {
      Assert.assertEquals(true, entry.getValue().equals(actual.get(entry.getKey())));
    }
  }

  @Test
  public void testGenerateDayMaterializedSegmentForRangeAndOriginType()
  {
    // test hour segment granularity and origin & range partitionIds store
    int segmentNums = 12;
    dataSegments = createAutoStoreAndHourGranDataSegments(segmentNums, false);
    daySpec.generateMaterializedSegment(dataSegments);
    final Map<String, Object> storeMaterializedSegmentMap = (Map<String, Object>) context.get(Tasks.CONTEXT_KEY_STORE_MATERIALIZED_SEGMENTS);
    MaterializedSpec materializedSpec = objectMapper.convertValue(
        storeMaterializedSegmentMap,
        new TypeReference<MaterializedSpec>()
        {
        }
    );
    Assert.assertEquals(true, materializedSpec.getBaseShardSpecsSpec() == null);
    Assert.assertEquals(true, materializedSpec.getSourceBaseShardSpecsSpecs() != null);

    Map<Short, BaseShardSpecsSpec> multiSegmentGrans = new HashMap<>();
    multiSegmentGrans.put(
        (short) 1, // intervalId
        new BaseShardSpecsSpec(0, segmentNums, "test_version01")
    );
    multiSegmentGrans.put(
        (short) 2, // intervalId
        new BaseShardSpecsSpec(0, 2, "test_version01")
    );
    MaterializedSpec expectedMaterializedSpec = new MaterializedSpec(
        MaterializedSpec.TYPE_DIFF_SEGMENT_GRAN,
        null,
        multiSegmentGrans,
        (short) 24
    );


    Assert.assertEquals(
        true,
        materializedSpec.getBaseShardSpecsSpec() == null && materializedSpec.getSourceBaseShardSpecsSpecs() != null
    );
    Map<Short, BaseShardSpecsSpec> expected = expectedMaterializedSpec.getSourceBaseShardSpecsSpecs();
    Map<Short, BaseShardSpecsSpec> actual = materializedSpec.getSourceBaseShardSpecsSpecs();
    for (Map.Entry<Short, BaseShardSpecsSpec> entry : expected.entrySet()) {
      Assert.assertEquals(entry.getValue(), actual.get(entry.getKey()));
    }
  }

  @Test
  public void testGenerateHourMaterializedSegmentForRangeAndOriginType()
  {
    // test hour segment granularity and origin & range partitionIds store
    int segmentNums = 12;
    dataSegments = createAutoStoreAndHourGranDataSegments(segmentNums, true);
    hourSpec.generateMaterializedSegment(dataSegments);
    final Map<String, Object> storeMaterializedSegmentMap =
        (Map<String, Object>) context.get(Tasks.CONTEXT_KEY_STORE_MATERIALIZED_SEGMENTS);
    MaterializedSpec materializedSpec = objectMapper.convertValue(
        storeMaterializedSegmentMap,
        new TypeReference<MaterializedSpec>()
        {
        }
    );
    Assert.assertEquals(true, materializedSpec.getBaseShardSpecsSpec() != null);
    Assert.assertEquals(true, materializedSpec.getSourceBaseShardSpecsSpecs() == null);

    MaterializedSpec expectedMaterializedSpec = new MaterializedSpec(
        MaterializedSpec.TYPE_SAME_SEGMENT_GRAN,
        new BaseShardSpecsSpec(0, segmentNums, "test_version01"),
        null,
        (short) 1
    );
    Assert.assertEquals(objectMapper.convertValue(
        expectedMaterializedSpec,
        new TypeReference<Map<String, Object>>()
        {
        }
    ), objectMapper.convertValue(
        materializedSpec,
        new TypeReference<Map<String, Object>>()
        {
        }
    ));
  }

  @Test
  public void testComputeMultiSegmentGranSpec()
  {
    Pair<Short, Map<Short, BaseShardSpecsSpec>> hourGrans = hourSpec.computeMultiSegmentGranSpec(dataSegments);
    Pair<Short, Map<Short, BaseShardSpecsSpec>> dayGrans = daySpec.computeMultiSegmentGranSpec(dataSegments);


    Set<Short> expectedDayMapBuckets = Sets.newHashSet((short) 0, (short) 23);
    Assert.assertEquals(true, hourGrans.lhs == 1 && dayGrans.lhs == 24);
    Assert.assertEquals(expectedDayMapBuckets, dayGrans.rhs.keySet());


    dataSegments = createHourGranDataSegments();
    Pair<Short, Map<Short, BaseShardSpecsSpec>> twoHourGrans = getSpec(Granularities.TWO_HOUR).computeMultiSegmentGranSpec(
        dataSegments);
    Pair<Short, Map<Short, BaseShardSpecsSpec>> sixexHourGrans = getSpec(Granularities.SIX_HOUR).computeMultiSegmentGranSpec(
        dataSegments);

    //two hour
    Set<Short> expectTwoMapBuckets = Sets.newHashSet((short) 0, (short) 1);
    Assert.assertEquals(expectTwoMapBuckets, twoHourGrans.rhs.keySet());
    //total mapBuckets
    Assert.assertEquals(2, (int) twoHourGrans.lhs);


    dataSegments = createWeekGranFromDayGranDataSegments();
    Pair<Short, Map<Short, BaseShardSpecsSpec>> weekGrans = getSpec(Granularities.WEEK).computeMultiSegmentGranSpec(
        dataSegments);
    Set<Short> expectWeekMapBuckets = Sets.newHashSet((short) 0, (short) 3);
    //total mapBuckets
    Assert.assertEquals(7, (int) weekGrans.lhs);
    Assert.assertEquals(expectWeekMapBuckets, weekGrans.rhs.keySet());


    Pair<Short, Map<Short, BaseShardSpecsSpec>> monthGrans = getSpec(Granularities.MONTH).computeMultiSegmentGranSpec(
        dataSegments);
    Pair<Short, Map<Short, BaseShardSpecsSpec>> quarterGrans = getSpec(Granularities.QUARTER).computeMultiSegmentGranSpec(
        dataSegments);

  }

  private List<DataSegment> createHourGranDataSegments()
  {
    dataSegments = new ArrayList<>();
    dataSegments.add(new DataSegment(
        "base",
        Intervals.of("2022-01-01T00Z/2022-01-01T01Z"),
        "test_version01",
        ImmutableMap.of(),
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("m1"),
        new HashBasedNumberedShardSpec(0, 3, 0, 1, null, null, null),
        9,
        1024
    ));
    dataSegments.add(new DataSegment(
        "base",
        Intervals.of("2022-01-01T01Z/2022-01-01T02Z"),
        "test_version01",
        ImmutableMap.of(),
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("m1"),
        new HashBasedNumberedShardSpec(1, 3, 0, 1, null, null, null),
        9,
        1024
    ));
    return dataSegments;
  }

  public List<DataSegment> createWeekGranFromDayGranDataSegments()
  {
    dataSegments = new ArrayList<>();
    dataSegments.add(new DataSegment(
        "base",
        Intervals.of("2022-01-31T00Z/2022-02-01T00Z"),
        "test_version01",
        ImmutableMap.of(),
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("m1"),
        new HashBasedNumberedShardSpec(0, 3, 0, 1, null, null, null),
        9,
        1024
    ));
    dataSegments.add(new DataSegment(
        "base",
        Intervals.of("2022-02-03T00Z/2022-02-04T00Z"),
        "test_version01",
        ImmutableMap.of(),
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("m1"),
        new HashBasedNumberedShardSpec(1, 3, 0, 1, null, null, null),
        9,
        1024
    ));
    return dataSegments;
  }

  private List<DataSegment> createOriginStoreAndDiffGranDataSegments()
  {
    dataSegments = new ArrayList<>();
    // hour interval:2022-01-01T01Z/2022-01-01T02Z has three segment
    dataSegments.add(new DataSegment(
        "base",
        Intervals.of("2022-01-01T01Z/2022-01-01T02Z"),
        "test_version01",
        ImmutableMap.of(),
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("m1"),
        new HashBasedNumberedShardSpec(0, 3, 0, 1, null, null, null),
        9,
        1024
    ));
    dataSegments.add(new DataSegment(
        "base",
        Intervals.of("2022-01-01T01Z/2022-01-01T02Z"),
        "test_version01",
        ImmutableMap.of(),
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("m1"),
        new HashBasedNumberedShardSpec(1, 3, 0, 1, null, null, null),
        9,
        1024
    ));
    dataSegments.add(new DataSegment(
        "base",
        Intervals.of("2022-01-01T01Z/2022-01-01T02Z"),
        "test_version01",
        ImmutableMap.of(),
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("m1"),
        new HashBasedNumberedShardSpec(2, 3, 0, 1, null, null, null),
        9,
        1024
    ));

    // two hour interval:2022-01-01T02Z/2022-01-01T04Z has two segment
    dataSegments.add(new DataSegment(
        "base",
        Intervals.of("2022-01-01T02Z/2022-01-01T04Z"),
        "test_version01",
        ImmutableMap.of(),
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("m1"),
        new HashBasedNumberedShardSpec(0, 2, 0, 1, null, null, null),
        9,
        1024
    ));
    dataSegments.add(new DataSegment(
        "base",
        Intervals.of("2022-01-01T02Z/2022-01-01T04Z"),
        "test_version01",
        ImmutableMap.of(),
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("m1"),
        new HashBasedNumberedShardSpec(1, 2, 0, 1, null, null, null),
        9,
        1024
    ));

    return dataSegments;
  }

  private List<DataSegment> createOriginStoreAndHourGranDataSegments()
  {
    dataSegments = new ArrayList<>();
    // hour interval:2022-01-01T01Z/2022-01-01T02Z has three segment
    dataSegments.add(new DataSegment(
        "base",
        Intervals.of("2022-01-01T01Z/2022-01-01T02Z"),
        "test_version01",
        ImmutableMap.of(),
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("m1"),
        new HashBasedNumberedShardSpec(0, 3, 0, 1, null, null, null),
        9,
        1024
    ));
    dataSegments.add(new DataSegment(
        "base",
        Intervals.of("2022-01-01T01Z/2022-01-01T02Z"),
        "test_version01",
        ImmutableMap.of(),
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("m1"),
        new HashBasedNumberedShardSpec(1, 3, 0, 1, null, null, null),
        9,
        1024
    ));
    dataSegments.add(new DataSegment(
        "base",
        Intervals.of("2022-01-01T01Z/2022-01-01T02Z"),
        "test_version01",
        ImmutableMap.of(),
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("m1"),
        new HashBasedNumberedShardSpec(2, 3, 0, 1, null, null, null),
        9,
        1024
    ));

    // hour interval:2022-01-01T02Z/2022-01-01T03Z has two segment
    dataSegments.add(new DataSegment(
        "base",
        Intervals.of("2022-01-01T02Z/2022-01-01T03Z"),
        "test_version01",
        ImmutableMap.of(),
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("m1"),
        new HashBasedNumberedShardSpec(0, 2, 0, 1, null, null, null),
        9,
        1024
    ));
    dataSegments.add(new DataSegment(
        "base",
        Intervals.of("2022-01-01T02Z/2022-01-01T03Z"),
        "test_version01",
        ImmutableMap.of(),
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("m1"),
        new HashBasedNumberedShardSpec(1, 2, 0, 1, null, null, null),
        9,
        1024
    ));

    return dataSegments;
  }

  static List<DataSegment> createAutoStoreAndHourGranDataSegments(int segmentNums, boolean onlyOneHourPartition)
  {
    List<DataSegment> dataSegments = new ArrayList<>();
    // hour interval:2022-01-01T01Z/2022-01-01T02Z has more than MAX_STORE_ORIGIN_VALUES segments
    for (int i = 0; i < segmentNums; i++) {
      dataSegments.add(new DataSegment(
          "base",
          Intervals.of("2022-01-01T01Z/2022-01-01T02Z"),
          "test_version01",
          ImmutableMap.of(),
          ImmutableList.of("dim1", "dim2"),
          ImmutableList.of("m1"),
          new HashBasedNumberedShardSpec(
              i,
              segmentNums,
              0,
              1,
              null,
              null,
              null
          ),
          9,
          1024
      ));

    }

    if (onlyOneHourPartition == false) {
      // hour interval:2022-01-01T02Z/2022-01-01T03Z has two segment
      dataSegments.add(new DataSegment(
          "base",
          Intervals.of("2022-01-01T02Z/2022-01-01T03Z"),
          "test_version01",
          ImmutableMap.of(),
          ImmutableList.of("dim1", "dim2"),
          ImmutableList.of("m1"),
          new HashBasedNumberedShardSpec(0, 2, 0, 1, null, null, null),
          9,
          1024
      ));
      dataSegments.add(new DataSegment(
          "base",
          Intervals.of("2022-01-01T02Z/2022-01-01T03Z"),
          "test_version01",
          ImmutableMap.of(),
          ImmutableList.of("dim1", "dim2"),
          ImmutableList.of("m1"),
          new HashBasedNumberedShardSpec(1, 2, 0, 1, null, null, null),
          9,
          1024
      ));
    }
    return dataSegments;
  }

}
