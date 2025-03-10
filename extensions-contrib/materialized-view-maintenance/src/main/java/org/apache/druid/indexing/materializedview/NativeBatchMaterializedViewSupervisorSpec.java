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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.druid.client.materializedview.ClientTaskGranularitySpec;
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
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.timeline.BaseShardSpecsSpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.MaterializedSpec;
import org.apache.druid.utils.CollectionUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

public class NativeBatchMaterializedViewSupervisorSpec extends MaterializedViewSupervisorSpec
{
  private static final EmittingLogger log = new EmittingLogger(NativeBatchMaterializedViewSupervisorSpec.class);
  private static final String SUPERVISOR_TYPE = "materialized_view";
  private final String baseDataSource;
  private final ParallelIndexTuningConfig tuningConfig;
  private final String dataSourceName;
  private final Map<String, Object> context;
  private final PolicyConfig policyConfig;
  private final ObjectMapper objectMapper;
  private final MetadataSupervisorManager metadataSupervisorManager;
  private final IndexerMetadataStorageCoordinator metadataStorageCoordinator;
  private final SqlSegmentsMetadataManager sqlSegmentsMetadataManager;
  private final TaskMaster taskMaster;
  private final TaskStorage taskStorage;
  private final MaterializedViewTaskConfig config;
  private final AuthorizerMapper authorizerMapper;
  private final ChatHandlerProvider chatHandlerProvider;
  private final SupervisorStateManagerConfig supervisorStateManagerConfig;
  private final boolean suspended;
  private final SegmentCacheManagerFactory segmentCacheManagerFactory;
  private final RetryPolicyFactory retryPolicyFactory;
  private final ParallelIndexTuningConfig actualTuningConfig;

  public NativeBatchMaterializedViewSupervisorSpec(
      @JsonProperty("baseDataSource") String baseDataSource,
      @JsonProperty("dimensionsSpec") @Nullable DimensionsSpec dimensionsSpec,
      @JsonProperty("metricsSpec") @Nullable AggregatorFactory[] aggregators,
      @JsonProperty("granularitySpec") ClientTaskGranularitySpec granularitySpec,
      @JsonProperty("filter") @Nullable DimFilter dimFilter,
      @JsonProperty("tuningConfig") @Nullable ParallelIndexTuningConfig tuningConfig,
      @JsonProperty("dataSource") String dataSourceName,
      @JsonProperty("policyConfig") @Nullable PolicyConfig policyConfig,
      @JsonProperty("context") @Nullable Map<String, Object> context,
      @JsonProperty("suspended") @Nullable Boolean suspended,
      @JacksonInject ObjectMapper objectMapper,
      @JacksonInject TaskMaster taskMaster,
      @JacksonInject TaskStorage taskStorage,
      @JacksonInject MetadataSupervisorManager metadataSupervisorManager,
      @JacksonInject SqlSegmentsMetadataManager sqlSegmentsMetadataManager,
      @JacksonInject IndexerMetadataStorageCoordinator metadataStorageCoordinator,
      @JacksonInject MaterializedViewTaskConfig config,
      @JacksonInject AuthorizerMapper authorizerMapper,
      @JacksonInject ChatHandlerProvider chatHandlerProvider,
      @JacksonInject SupervisorStateManagerConfig supervisorStateManagerConfig,
      @JacksonInject SegmentCacheManagerFactory segmentCacheManagerFactory,
      @JacksonInject RetryPolicyFactory retryPolicyFactory
  )
  {
    super(baseDataSource, dimensionsSpec, aggregators, granularitySpec, dimFilter, tuningConfig,
          dataSourceName, context, suspended, supervisorStateManagerConfig
    );
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(baseDataSource),
        "baseDataSource cannot be null or empty. Please provide a baseDataSource."
    );
    this.baseDataSource = baseDataSource;
    this.policyConfig = policyConfig == null ? new PolicyConfig(null, null, null, null, null, null, null) : policyConfig;
    this.tuningConfig = tuningConfig == null ? ParallelIndexTuningConfig.defaultConfig() : tuningConfig;
    this.config = config;
    this.dataSourceName = dataSourceName;
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
  }

  @Override
  public Task createTask(
      @Nullable Interval interval,
      @Nullable String version,
      List<DataSegment> segments,
      boolean appendToExisting
  )
  {
    // partitionsSpec must support overwrite interval, choose backup partitionsSpec if necessary
    setAppendingSubmitMode(appendToExisting);

    //generate materializedSegment,and save to context
    generateMaterializedSegment(segments);

    // generate MaterializedViewTask
    MaterializedViewTask task = null;
    try {
      task = new MaterializedViewTask.Builder(
          baseDataSource,
          dataSourceName,
          segmentCacheManagerFactory,
          retryPolicyFactory
      )
          .segments(segments, appendToExisting)
          .tuningConfig(actualTuningConfig)
          .dimensionsSpec(getDimensionsSpec())
          .metricsSpec(getMetricsSpec())
          .granularitySpec(granularitySpec)
          .dimFilter(dimFilter)
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

  @Override
  public boolean forceOverwrite()
  {
    return tuningConfig.isForceGuaranteedRollup();
  }

  @Override
  public boolean isOverwritePartition()
  {
    if (tuningConfig.getPartitionsSpec() == null) {
      return false;
    }
    return tuningConfig.getPartitionsSpec().isForceGuaranteedRollupCompatibleType();
  }

  @Override
  public boolean isReachMVSegmentGran(SortedMap<Interval, Pair<String, List<DataSegment>>> sortedBaseIntervalSegments,
                                      DataSegment curInputDataSegment
  )
  {
    if(sortedBaseIntervalSegments.isEmpty()){
      return false;
    }
    DataSegment baseFirstSeg = sortedBaseIntervalSegments.get(sortedBaseIntervalSegments.firstKey()).rhs.get(0);
    Interval materializedInterval = getGranularitySpec().getSegmentGranularity()
                                                        .bucket(baseFirstSeg.getId().getIntervalStart());
    if (!materializedInterval.isEqual(curInputDataSegment.getInterval())) {
      return true;
    }
    return false;
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
                                             : MaterializedViewTaskConfig.getDefaultPartitionsSpec(
                                                 true,
                                                 policyConfig.getTargetRowsPerSegmentForOverwrite()
                                             ));
      }
    }
    return actualTuningConfig;
  }

  @VisibleForTesting
  void generateMaterializedSegment(List<DataSegment> segments)
  {
    int minId = segments.stream().mapToInt(ds -> ds.getId().getPartitionNum()).min().orElseThrow(() -> new ISE("Segments is empty!"));
    int maxId = segments.stream().mapToInt(ds -> ds.getId().getPartitionNum()).max().orElseThrow(() -> new ISE("Segments is empty!"));

    byte type = compareSegmentGranType(segments);
    BaseShardSpecsSpec baseShardSpecsSpec = null;
    Pair<Short, Map<Short, BaseShardSpecsSpec>> multiSegmentGrans = null;
    short mapBuckets = 1;

    if (type == MaterializedSpec.TYPE_SAME_SEGMENT_GRAN) {
      baseShardSpecsSpec = new BaseShardSpecsSpec(minId, maxId + 1, segments.get(0).getVersion());
    } else if (type == MaterializedSpec.TYPE_DIFF_SEGMENT_GRAN) {
      multiSegmentGrans = computeMultiSegmentGranSpec(segments);
      mapBuckets = multiSegmentGrans.lhs;
    } else {
      throw new IAE(
          "WTF? not support type[%s], bucause baseDataSource[%s] segments exists different segment granularity "
          + "or larger than materializedview granularity",
          type,
          baseDataSource
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
  Pair<Short, Map<Short, BaseShardSpecsSpec>> computeMultiSegmentGranSpec(List<DataSegment> segments)
  {
    // intervalId -> Piar<Version, <minId,maxId,minorVersion>>
    Map<Short, Pair<String, MutablePair<Integer, Integer>>> multiSegmentGrans = new HashMap<>();
    Interval materializedInterval = getGranularitySpec().getSegmentGranularity()
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
  public byte compareSegmentGranType(List<DataSegment> segments)
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
      Interval materializedInterval = getGranularitySpec().getSegmentGranularity()
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
    return getBaseDataSource();
  }

  @Override
  public String getId()
  {
    return StringUtils.format("MVSupervisor-%s", dataSourceName);
  }

  @Override
  public Supervisor createSupervisor()
  {
    return new MaterializedViewSupervisor(
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
    return ImmutableList.of(dataSourceName);
  }

  @Override
  public SupervisorSpec createSuspendedSpec()
  {
    return new NativeBatchMaterializedViewSupervisorSpec(
        baseDataSource,
        getDimensionsSpec(),
        getMetricsSpec(),
        granularitySpec,
        dimFilter,
        tuningConfig,
        dataSourceName,
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
    return new NativeBatchMaterializedViewSupervisorSpec(
        baseDataSource,
        getDimensionsSpec(),
        getMetricsSpec(),
        granularitySpec,
        dimFilter,
        tuningConfig,
        dataSourceName,
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

  @Override
  public String toString()
  {
    return "MaterializedViewSupervisorSpec{" +
           "baseDataSource=" + baseDataSource +
           '}';
  }
}
