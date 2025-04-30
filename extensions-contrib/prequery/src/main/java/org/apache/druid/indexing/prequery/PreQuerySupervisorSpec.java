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

package org.apache.druid.indexing.prequery;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.druid.client.materializedview.ClientTaskGranularitySpec;
import org.apache.druid.client.materializedview.DerivativeDataSourceMetadata;
import org.apache.druid.client.materializedview.InputDataSourceSpec;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.indexer.partitions.SecondaryPartitionType;
import org.apache.druid.indexing.common.RetryPolicyFactory;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.indexing.common.task.materializedview.MaterializedViewTask;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.Supervisor;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManagerConfig;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.MetadataSupervisorManager;
import org.apache.druid.metadata.SqlSegmentsMetadataManager;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.indexing.TuningConfig;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.timeline.BaseShardSpecsSpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.MaterializedSpec;
import org.apache.druid.utils.CollectionUtils;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PreQuerySupervisorSpec implements SupervisorSpec
{
  private static final EmittingLogger log = new EmittingLogger(PreQuerySupervisorSpec.class);
  private static final String SUPERVISOR_TYPE = "PreQuery";
  private final ParallelIndexTuningConfig tuningConfig;
  private final String prequeryDatasource;
  private final Map<String, Object> context;
  private final PolicyConfig policyConfig;
  private final ObjectMapper objectMapper;
  private final MetadataSupervisorManager metadataSupervisorManager;
  private final IndexerMetadataStorageCoordinator metadataStorageCoordinator;
  private final SqlSegmentsMetadataManager sqlSegmentsMetadataManager;
  private final TaskMaster taskMaster;
  private final TaskStorage taskStorage;
  private final PreQueryTaskConfig config;
  private final AuthorizerMapper authorizerMapper;
  private final ChatHandlerProvider chatHandlerProvider;
  private final SupervisorStateManagerConfig supervisorStateManagerConfig;
  private final boolean suspended;
  private final SegmentCacheManagerFactory segmentCacheManagerFactory;
  private final RetryPolicyFactory retryPolicyFactory;
  private final ParallelIndexTuningConfig actualTuningConfig;
  private final InputDataSourceSpec inputDataSourceSpec;
  private final Set<String> metrics;
  private final Set<String> dimensions;

  public PreQuerySupervisorSpec(
      @JsonProperty("inputSpec") InputDataSourceSpec inputDataSourceSpec,
      @JsonProperty("tuningConfig") @Nullable ParallelIndexTuningConfig tuningConfig,
      @JsonProperty("policyConfig") @Nullable PolicyConfig policyConfig,
      @JsonProperty("context") @Nullable Map<String, Object> context,
      @JsonProperty("suspended") @Nullable Boolean suspended,
      @JacksonInject ObjectMapper objectMapper,
      @JacksonInject TaskMaster taskMaster,
      @JacksonInject TaskStorage taskStorage,
      @JacksonInject MetadataSupervisorManager metadataSupervisorManager,
      @JacksonInject SqlSegmentsMetadataManager sqlSegmentsMetadataManager,
      @JacksonInject IndexerMetadataStorageCoordinator metadataStorageCoordinator,
      @JacksonInject PreQueryTaskConfig config,
      @JacksonInject AuthorizerMapper authorizerMapper,
      @JacksonInject ChatHandlerProvider chatHandlerProvider,
      @JacksonInject SupervisorStateManagerConfig supervisorStateManagerConfig,
      @JacksonInject SegmentCacheManagerFactory segmentCacheManagerFactory,
      @JacksonInject RetryPolicyFactory retryPolicyFactory
  )
  {
    Preconditions.checkArgument(
        inputDataSourceSpec!=null&&inputDataSourceSpec.getDataSource()!=null,
        "inputDataSourceSpec cannot be null or empty. Please provide a inputDataSourceSpec."
    );
    this.inputDataSourceSpec = inputDataSourceSpec;
    this.policyConfig = policyConfig == null ? new PolicyConfig(null, null, null, null, null, null, null) : policyConfig;
    this.tuningConfig = tuningConfig == null ? ParallelIndexTuningConfig.defaultConfig() : tuningConfig;
    this.config = config;
    this.prequeryDatasource = inputDataSourceSpec.getDataSource()+"_prequery";
    this.context = context == null ? new HashMap<>() : context;
    this.objectMapper = objectMapper;
    this.taskMaster = taskMaster;
    this.taskStorage = taskStorage;
    this.metadataSupervisorManager = metadataSupervisorManager;
    this.sqlSegmentsMetadataManager = sqlSegmentsMetadataManager;
    this.metadataStorageCoordinator = metadataStorageCoordinator;
    this.authorizerMapper = authorizerMapper;
    this.chatHandlerProvider = chatHandlerProvider;
    this.supervisorStateManagerConfig = supervisorStateManagerConfig;
    this.suspended = suspended != null ? suspended : false;
    this.segmentCacheManagerFactory = segmentCacheManagerFactory;
    this.retryPolicyFactory = retryPolicyFactory;
    this.tuningConfig.setMaxNumSegmentsToMerge(config.getMaxNumSegmentsToMerge());
    this.actualTuningConfig = this.tuningConfig;
    //update actualTuningConfig
    if (this.tuningConfig.getPartitionsSpec() == null
        || this.tuningConfig.getPartitionsSpec().getType() == SecondaryPartitionType.LINEAR) {
      setAppendingSubmitMode(true);
    } else {
      setAppendingSubmitMode(false);
    }

    this.metrics = new HashSet<>();
    if (inputDataSourceSpec.getMetricsSpec() != null) {
      for (AggregatorFactory aggregatorFactory : inputDataSourceSpec.getMetricsSpec()) {
        metrics.add(aggregatorFactory.getName());
      }
    }
    this.dimensions = new HashSet<>();
    if (inputDataSourceSpec.getDimensionsSpec() != null) {
      for (DimensionSchema schema : inputDataSourceSpec.getDimensionsSpec().getDimensions()) {
        dimensions.add(schema.getName());
      }
    }
  }

  public Task createTask(
      List<DataSegment> segments,
      boolean appendToExisting,
      DerivativeDataSourceMetadata baseDataSourceMetadata,
      String deritiveDataSource
  )
  {
    // partitionsSpec must support overwrite interval, choose backup partitionsSpec if necessary
    setAppendingSubmitMode(appendToExisting);

    //generate materializedSegment,and save to context
    generateMaterializedSegment(segments,baseDataSourceMetadata.getGranularitySpec());

    //获取dimensionSpec
    DimensionsSpec dimensionsSpec = takeDimensionsSpec(inputDataSourceSpec.getDimensionsSpec(),
                                                    baseDataSourceMetadata.getDimensions());
    //获取metricsSpec
    AggregatorFactory[] metricsSpec = takeMetricsSpec(inputDataSourceSpec.getMetricsSpec(),
                                                   baseDataSourceMetadata.getMetrics());
    // generate MaterializedViewTask
    MaterializedViewTask task = null;
    try {
      task = new MaterializedViewTask.Builder(
          baseDataSourceMetadata.getBaseDataSource(),
          deritiveDataSource,
          segmentCacheManagerFactory,
          retryPolicyFactory
      )
          .segments(segments, appendToExisting)
          .tuningConfig(actualTuningConfig)
          .dimensionsSpec(dimensionsSpec)
          .metricsSpec(metricsSpec)
          .granularitySpec(baseDataSourceMetadata.getGranularitySpec())
          //.dimFilter(dimFilter)
          .context(context)
          .build();
    }
    catch (Throwable e) {
      log.error(e, "create MaterializedViewTask{tuningConfig[%s]} exception", tuningConfig);
    }
    finally {
      log.info("create task{[%s], appendToExisting[%s]} over!", task, appendToExisting);
    }
    return task;
  }

  private AggregatorFactory[] takeMetricsSpec(AggregatorFactory[] metricsSpec, Set<String> metrics) {
    List<AggregatorFactory> subMetrics = new ArrayList<>();
    for(AggregatorFactory aggregatorFactory : metricsSpec){
      if(metrics.contains(aggregatorFactory.getName())){
        subMetrics.add(aggregatorFactory);
      }
    }
    return subMetrics.toArray(new AggregatorFactory[0]);
  }

  private DimensionsSpec takeDimensionsSpec(DimensionsSpec dimensionsSpec, Set<String> dimensions) {
    List<String> subDimensions = new ArrayList<>(dimensions);
    List<String> allDimensions = new ArrayList<>(dimensionsSpec.getDimensionNames());
    //取交集
    allDimensions.retainAll(subDimensions);
    return new DimensionsSpec.Builder().setDefaultSchemaDimensions(allDimensions).build();
  }

  public boolean forceOverwrite()
  {
    return tuningConfig.isForceGuaranteedRollup();
  }

  public boolean isOverwritePartition()
  {
    if (tuningConfig.getPartitionsSpec() == null) {
      return false;
    }
    return tuningConfig.getPartitionsSpec().isForceGuaranteedRollupCompatibleType();
  }


  private ParallelIndexTuningConfig setAppendingSubmitMode(boolean appendingMode)
  {
    //appending submit
    if (appendingMode) {
      //change forceGuaranteedRollup & partitionsSpec to support appending
      actualTuningConfig.setForceGuaranteedRollup(false);
      actualTuningConfig.setPartitionsSpec(config.getBackupAppendingPartitionsSpec());
      //由于目前增量物化要求每次只产生一个segment，暂只默认使用1个worker
      actualTuningConfig.setMaxNumConcurrentSubTasks(1);
    } else {
      //change forceGuaranteedRollup to support overwrite
      actualTuningConfig.setForceGuaranteedRollup(true);
      actualTuningConfig.setMaxNumSegmentsToMerge(tuningConfig.getMaxNumSegmentsToMerge());
      if (!isOverwritePartition()) {
        //current appending partitionsSpec,but need submit overwrite task
        actualTuningConfig.setPartitionsSpec(policyConfig.getTargetRowsPerSegmentForOverwrite() == null
                                             ? config.getBackupOverwritePartitionsSpec()
                                             : PreQueryTaskConfig.getDefaultPartitionsSpec(
                                                 true,
                                                 policyConfig.getTargetRowsPerSegmentForOverwrite()
                                             ));
      }
    }
    return actualTuningConfig;
  }

  @VisibleForTesting
  void generateMaterializedSegment(List<DataSegment> segments, ClientTaskGranularitySpec granularitySpec)
  {
    int minId = segments.stream().mapToInt(ds -> ds.getId().getPartitionNum()).min().orElseThrow(() -> new ISE("Segments is empty!"));
    int maxId = segments.stream().mapToInt(ds -> ds.getId().getPartitionNum()).max().orElseThrow(() -> new ISE("Segments is empty!"));

    byte type = compareSegmentGranType(segments,granularitySpec);
    BaseShardSpecsSpec baseShardSpecsSpec = null;
    Pair<Short, Map<Short, BaseShardSpecsSpec>> multiSegmentGrans = null;
    short mapBuckets = 1;

    if (type == MaterializedSpec.TYPE_SAME_SEGMENT_GRAN) {
      baseShardSpecsSpec = new BaseShardSpecsSpec(minId, maxId + 1, segments.get(0).getVersion());
    } else if (type == MaterializedSpec.TYPE_DIFF_SEGMENT_GRAN) {
      multiSegmentGrans = computeMultiSegmentGranSpec(segments,granularitySpec);
      mapBuckets = multiSegmentGrans.lhs;
    } else {
      throw new IAE(
          "WTF? not support type[%s], bucause baseDataSource segments exists different segment granularity "
          + "or larger than materializedview granularity",
          type
      );
    }

    MaterializedSpec materializedSpec = new MaterializedSpec(
        type,
        baseShardSpecsSpec,
        multiSegmentGrans == null ? null : multiSegmentGrans.rhs,
        mapBuckets
    );
    context.put(Tasks.CONTEXT_KEY_STORE_MATERIALIZED_SEGMENTS, objectMapper.convertValue(
        materializedSpec,
        new TypeReference<Map<String, Object>>()
        {
        }
    ));
  }

  @VisibleForTesting
  Pair<Short, Map<Short, BaseShardSpecsSpec>> computeMultiSegmentGranSpec(List<DataSegment> segments,
                                                                          ClientTaskGranularitySpec granularitySpec
  )
  {
    // intervalId -> Piar<Version, <minId,maxId,minorVersion>>
    Map<Short, Pair<String, MutablePair<Integer, Integer>>> multiSegmentGrans = new HashMap<>();
    Interval materializedInterval = granularitySpec.getSegmentGranularity()
                                                        .bucket(segments.get(0).getId().getIntervalStart());
    short mapingBuckets = computeMapingBuckets(materializedInterval, segments.get(0).getId().getInterval());
    for (DataSegment dataSegment : segments) {
      long startTime = materializedInterval.getStartMillis();
      Interval srcInterval = dataSegment.getInterval();
      // compute intervalId that equals to position of srcInterval belong to materializedInterval.
      short intervalId = 0;
      while (startTime + srcInterval.toDurationMillis() <= srcInterval.getStartMillis()) {
        intervalId += 1;
        startTime += srcInterval.toDurationMillis();
      }

      Pair<String, MutablePair<Integer, Integer>> segmentGranPair = multiSegmentGrans.computeIfAbsent(
          intervalId,
          k -> new Pair<>(
              dataSegment.getVersion(),
              new MutablePair<>(Integer.MAX_VALUE, Integer.MIN_VALUE)
          )
      );
      segmentGranPair.rhs.setLeft(Math.min(segmentGranPair.rhs.getLeft(), dataSegment.getId().getPartitionNum()));
      segmentGranPair.rhs.setRight(Math.max(segmentGranPair.rhs.getRight(), dataSegment.getId().getPartitionNum()));
    }
    return new Pair<>(mapingBuckets, CollectionUtils.mapValues(multiSegmentGrans, entry -> new BaseShardSpecsSpec(
        entry.rhs.getLeft(), entry.rhs.getRight() + 1, entry.lhs
    )));
  }

  private short computeMapingBuckets(Interval materializedInterval, Interval interval)
  {
    return (short) (materializedInterval.toDurationMillis() / interval.toDurationMillis());
  }

  @VisibleForTesting
  public byte compareSegmentGranType(List<DataSegment> segments, ClientTaskGranularitySpec granularitySpec)
  {
    byte type = MaterializedSpec.TYPE_SAME_SEGMENT_GRAN;
    long beforeDuration = -1;
    for (DataSegment dataSegment : segments) {
      if (beforeDuration != -1 && beforeDuration != dataSegment.getInterval().toDurationMillis()) {
        return MaterializedSpec.TYPE_UNSUPPORT_GRAN;
      }
      beforeDuration = dataSegment.getInterval().toDurationMillis();
    }

    for (DataSegment dataSegment : segments) {
      Interval materializedInterval = granularitySpec.getSegmentGranularity()
                                                          .bucket(dataSegment.getId().getIntervalStart());
      if (materializedInterval.toDurationMillis() < dataSegment.getInterval().toDurationMillis()) {
        return MaterializedSpec.TYPE_UNSUPPORT_GRAN;
      }
      if (!materializedInterval.isEqual(dataSegment.getInterval())) {
        type = MaterializedSpec.TYPE_DIFF_SEGMENT_GRAN;
        break;
      }
    }
    return type;
  }

  @JsonProperty("policyConfig")
  public PolicyConfig getPolicyConfig()
  {
    return policyConfig;
  }

  @Override
  @JsonProperty("suspended")
  public boolean isSuspended()
  {
    return suspended;
  }

  @Override
  @JsonProperty("type")
  public String getType()
  {
    return SUPERVISOR_TYPE;
  }

  @Override
  @JsonProperty("source")
  public String getSource()
  {
    return null;
  }

  @Override
  public String getId()
  {
    return StringUtils.format("%s-%s", SUPERVISOR_TYPE,prequeryDatasource);
  }

  @Override
  public Supervisor createSupervisor()
  {
    return new PreQuerySupervisor(
        taskMaster,
        taskStorage,
        metadataSupervisorManager,
        sqlSegmentsMetadataManager,
        metadataStorageCoordinator,
        config,
        this,
        policyConfig
    );
  }

  @Override
  public List<String> getDataSources()
  {
    return ImmutableList.of(prequeryDatasource);
  }

  @Override
  public SupervisorSpec createSuspendedSpec()
  {
    return new PreQuerySupervisorSpec(
        inputDataSourceSpec,
        tuningConfig,
        policyConfig,
        context,
        true,
        objectMapper,
        taskMaster,
        taskStorage,
        metadataSupervisorManager,
        sqlSegmentsMetadataManager,
        metadataStorageCoordinator,
        config,
        authorizerMapper,
        chatHandlerProvider,
        supervisorStateManagerConfig,
        segmentCacheManagerFactory,
        retryPolicyFactory
    );
  }

  @Override
  public SupervisorSpec createRunningSpec()
  {
    return new PreQuerySupervisorSpec(
        inputDataSourceSpec,
        tuningConfig,
        policyConfig,
        context,
        false,
        objectMapper,
        taskMaster,
        taskStorage,
        metadataSupervisorManager,
        sqlSegmentsMetadataManager,
        metadataStorageCoordinator,
        config,
        authorizerMapper,
        chatHandlerProvider,
        supervisorStateManagerConfig,
        segmentCacheManagerFactory,
        retryPolicyFactory
    );
  }

  public Set<String> getDimensions()
  {
    return dimensions;
  }

  public Set<String> getMetrics()
  {
    return metrics;
  }

  @JsonProperty("inputDataSourceSpec")
  public InputDataSourceSpec getInputDataSourceSpec(){
    return inputDataSourceSpec;
  }
  @JsonProperty("prequeryDataSource")
  public String getPrequeryDatasource(){
    return prequeryDatasource;
  }

  @JsonProperty("dimensionsSpec")
  public DimensionsSpec getDimensionsSpec()
  {
    return inputDataSourceSpec.getDimensionsSpec();
  }

  @JsonProperty("metricsSpec")
  public AggregatorFactory[] getMetricsSpec()
  {
    return inputDataSourceSpec.getMetricsSpec();
  }

  @JsonProperty("tuningConfig")
  public TuningConfig getTuningConfig()
  {
    return tuningConfig;
  }

  @JsonProperty("context")
  public Map<String, Object> getContext()
  {
    return context;
  }

  @Override
  public String toString()
  {
    return "MaterializedViewSupervisorSpec{" +
           "baseDataSource=" + inputDataSourceSpec.getDataSource() +
           '}';
  }

  public SupervisorStateManagerConfig getSupervisorStateManagerConfig() {
    return supervisorStateManagerConfig;
  }
}
