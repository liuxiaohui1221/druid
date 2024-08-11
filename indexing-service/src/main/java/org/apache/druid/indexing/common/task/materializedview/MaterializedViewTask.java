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

package org.apache.druid.indexing.common.task.materializedview;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.client.indexing.ClientCompactionTaskGranularitySpec;
import org.apache.druid.client.indexing.ClientCompactionTaskTransformSpec;
import org.apache.druid.client.materializedview.ClientTaskGranularitySpec;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionSchema.MultiValueHandling;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.Checks;
import org.apache.druid.indexer.Property;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.RetryPolicyFactory;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.RetrieveUsedSegmentsAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.AbstractBatchIndexTask;
import org.apache.druid.indexing.common.task.CompactionIOConfig;
import org.apache.druid.indexing.common.task.CompactionTask;
import org.apache.druid.indexing.common.task.CurrentSubTaskHolder;
import org.apache.druid.indexing.common.task.IndexTask.IndexTuningConfig;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexIOConfig;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexIngestionSpec;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTask;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.indexing.firehose.WindowedSegmentId;
import org.apache.druid.indexing.input.DruidInputSource;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.GranularityType;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.DimensionHandler;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.TuningConfig;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.realtime.firehose.ChatHandler;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentTimeline;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.timeline.partition.PartitionHolder;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MaterializedViewTask extends AbstractBatchIndexTask implements ChatHandler
{
  private static final Logger log = new Logger(MaterializedViewTask.class);
  /**
   * The CompactionTask creates and runs multiple IndexTask instances. When the {@link AppenderatorsManager}
   * is asked to clean up, it does so on a per-task basis keyed by task ID. However, the subtask IDs of the
   * CompactionTask are not externally visible. This context flag is used to ensure that all the appenderators
   * created for the CompactionTasks's subtasks are tracked under the ID of the parent CompactionTask.
   * The CompactionTask may change in the future and no longer require this behavior (e.g., reusing the same
   * Appenderator across subtasks, or allowing the subtasks to use the same ID). The CompactionTask is also the only
   * task type that currently creates multiple appenderators. Thus, a context flag is used to handle this case
   * instead of a more general approach such as new methods on the Task interface.
   */
  public static final String CTX_KEY_APPENDERATOR_TRACKING_TASK_ID = "appenderatorTrackingTaskId";

  private static final String TYPE = "index_materialized_view";
  private static final Clock UTC_CLOCK = Clock.systemUTC();
  private final String baseDataSource;
  private final String dataSource;
  private final MaterializedViewIOConfig ioConfig;
  @Nullable
  private final DimensionsSpec dimensionsSpec;
  @Nullable
  private final AggregatorFactory[] metricsSpec;
  @Nullable
  private final ClientCompactionTaskTransformSpec transformSpec;
  @Nullable
  private final ClientTaskGranularitySpec granularitySpec;
  @Nullable
  private final ParallelIndexTuningConfig tuningConfig;
  @JsonIgnore
  private final SegmentProvider segmentProvider;
  @JsonIgnore
  private final PartitionConfigurationManager partitionConfigurationManager;
  @JsonIgnore
  private final SegmentCacheManagerFactory segmentCacheManagerFactory;
  @JsonIgnore
  private final CurrentSubTaskHolder currentSubTaskHolder = new CurrentSubTaskHolder(
      (taskObject, config) -> {
        final ParallelIndexSupervisorTask indexTask = (ParallelIndexSupervisorTask) taskObject;
        indexTask.stopGracefully(config);
      }
  );

  @JsonCreator
  public MaterializedViewTask(
      @JsonProperty("id") @Nullable final String id,
      @JsonProperty("resource") @Nullable final TaskResource taskResource,
      @JsonProperty("baseDataSource") final String baseDataSource,
      @JsonProperty("dataSource") final String dataSource,
      @JsonProperty("interval") @Deprecated @Nullable final Interval interval,
      @JsonProperty("segments") @Deprecated @Nullable final List<WindowedSegmentId> segments,
      @JsonProperty("ioConfig") @Nullable MaterializedViewIOConfig ioConfig,
      @JsonProperty("dimensions") @Nullable final DimensionsSpec dimensions,
      @JsonProperty("dimensionsSpec") @Nullable final DimensionsSpec dimensionsSpec,
      @JsonProperty("metricsSpec") @Nullable final AggregatorFactory[] metricsSpec,
      @JsonProperty("transformSpec") @Nullable final ClientCompactionTaskTransformSpec transformSpec,
      @JsonProperty("granularitySpec") @Nullable final ClientTaskGranularitySpec granularitySpec,
      @JsonProperty("tuningConfig") @Nullable final TuningConfig tuningConfig,
      @JsonProperty("context") @Nullable final Map<String, Object> context,
      @JacksonInject SegmentCacheManagerFactory segmentCacheManagerFactory
  )
  {
    super(getOrMakeId(id, TYPE, dataSource), null, taskResource, dataSource, context,-1,computeCompactionIngestionMode(ioConfig));

    Checks.checkOneNotNullOrEmpty(
        ImmutableList.of(
            new Property<>("ioConfig", ioConfig),
            new Property<>("interval", interval),
            new Property<>("segments", segments)
        )
    );

    this.baseDataSource = baseDataSource;
    this.dataSource = dataSource;
    if (ioConfig != null) {
      this.ioConfig = ioConfig;
    } else if (interval != null) {
      this.ioConfig = new MaterializedViewIOConfig(
          new MaterializedIntervalSpec(Collections.singleton(interval), null),
          false
      );
    } else {
      // We already checked segments is not null or empty above.
      //noinspection ConstantConditions
      this.ioConfig = new MaterializedViewIOConfig(new SpecificMaterializedSegmentsSpec(segments), null);
    }

    this.dimensionsSpec = dimensionsSpec == null ? dimensions : dimensionsSpec;
    this.metricsSpec = metricsSpec;
    this.transformSpec = transformSpec;
    this.granularitySpec = granularitySpec;
    this.tuningConfig = tuningConfig != null ? getTuningConfig(tuningConfig) : null;

    this.segmentProvider = new SegmentProvider(baseDataSource, this.ioConfig.getInputSpec());
    this.partitionConfigurationManager = new PartitionConfigurationManager(this.tuningConfig);
    this.segmentCacheManagerFactory = segmentCacheManagerFactory;
  }

  @VisibleForTesting
  static ParallelIndexTuningConfig getTuningConfig(TuningConfig tuningConfig)
  {
    if (tuningConfig instanceof ParallelIndexTuningConfig) {
      return (ParallelIndexTuningConfig) tuningConfig;
    } else if (tuningConfig instanceof IndexTuningConfig) {
      final IndexTuningConfig indexTuningConfig = (IndexTuningConfig) tuningConfig;
      return new ParallelIndexTuningConfig(
          null,
          indexTuningConfig.getMaxRowsPerSegment(),
          indexTuningConfig.getAppendableIndexSpec(),
          indexTuningConfig.getMaxRowsPerSegment(),
          indexTuningConfig.getMaxBytesInMemory(),
          indexTuningConfig.isSkipBytesInMemoryOverheadCheck(),
          indexTuningConfig.getMaxTotalRows(),
          indexTuningConfig.getNumShards(),
          null,
          indexTuningConfig.getPartitionsSpec(),
          indexTuningConfig.getIndexSpec(),
          indexTuningConfig.getIndexSpecForIntermediatePersists(),
          indexTuningConfig.getMaxPendingPersists(),
          indexTuningConfig.isForceGuaranteedRollup(),
          indexTuningConfig.isReportParseExceptions(),
          indexTuningConfig.getPushTimeout(),
          indexTuningConfig.getSegmentWriteOutMediumFactory(),
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
          indexTuningConfig.getMaxSavedParseExceptions(),
          indexTuningConfig.getMaxParseExceptions(),
          indexTuningConfig.getAwaitSegmentAvailabilityTimeoutMillis(),
          indexTuningConfig.getMaxColumnsToMerge(),
          indexTuningConfig.getNumPersistThreads()
      );
    } else {
      throw new ISE(
          "Unknown tuningConfig type: [%s], Must be either [%s] or [%s]",
          tuningConfig.getClass().getName(),
          ParallelIndexTuningConfig.class.getName(),
          IndexTuningConfig.class.getName()
      );
    }
  }

  @VisibleForTesting
  public CurrentSubTaskHolder getCurrentSubTaskHolder()
  {
    return currentSubTaskHolder;
  }

  @JsonProperty
  public MaterializedViewIOConfig getIoConfig()
  {
    return ioConfig;
  }

  @JsonProperty
  public String getBaseDataSource()
  {
    return baseDataSource;
  }

  @Override
  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  @Nullable
  public DimensionsSpec getDimensionsSpec()
  {
    return dimensionsSpec;
  }

  @JsonProperty
  @Nullable
  public AggregatorFactory[] getMetricsSpec()
  {
    return metricsSpec;
  }

  @JsonProperty
  @Nullable
  public ClientTaskGranularitySpec getGranularitySpec()
  {
    return granularitySpec;
  }

  @Nullable
  @JsonProperty
  public ParallelIndexTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Override
  public int getPriority()
  {
    return getContextValue(Tasks.PRIORITY_KEY, Tasks.DEFAULT_BATCH_INDEX_TASK_PRIORITY);
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    final List<DataSegment> segments = segmentProvider.findSegments(taskActionClient);
    return determineLockGranularityAndTryLockWithSegments(taskActionClient, segments, segmentProvider::checkSegments);
  }

  @Override
  public boolean requireLockExistingSegments()
  {
    return true;
  }

  @Override
  public List<DataSegment> findSegmentsToLock(TaskActionClient taskActionClient, List<Interval> intervals)
      throws IOException
  {
    return ImmutableList.copyOf(
        taskActionClient.submit(new RetrieveUsedSegmentsAction(getDataSource(), null, intervals, Segments.ONLY_VISIBLE))
    );
  }

  @Override
  public boolean isPerfectRollup()
  {
    return tuningConfig != null && tuningConfig.isForceGuaranteedRollup();
  }

  @Nullable
  @Override
  public Granularity getSegmentGranularity()
  {
    return granularitySpec == null ? null : granularitySpec.getSegmentGranularity();
  }

  @Override
  public TaskStatus runTask(TaskToolbox toolbox) throws Exception
  {
//    final List<ParallelIndexIngestionSpec> ingestionSpecs = createIngestionSchema(
//        baseDataSource,
//        dataSource,
//        UTC_CLOCK,
//        toolbox,
//        getTaskLockHelper().getLockGranularityToUse(),
//        segmentProvider,
//        partitionConfigurationManager,
//        dimensionsSpec,
//        metricsSpec,
//        transformSpec,
//        granularitySpec,
//        toolbox.getCoordinatorClient(),
//        segmentCacheManagerFactory,
//        ioConfig.appendToExisting(),
//        ioConfig.isDropExisting()
//    );
    final List<ParallelIndexIngestionSpec> ingestionSpecs = createIngestionSchema(
        baseDataSource,
        dataSource,
        UTC_CLOCK,
        toolbox,
        getTaskLockHelper().getLockGranularityToUse(),
        ioConfig,
        segmentProvider,
        partitionConfigurationManager,
        dimensionsSpec,
        transformSpec,
        metricsSpec,
        granularitySpec,
        toolbox.getCoordinatorClient(),
        segmentCacheManagerFactory,
        getMetricBuilder()
    );
    final List<ParallelIndexSupervisorTask> indexTaskSpecs = IntStream
        .range(0, ingestionSpecs.size())
        .mapToObj(i -> {
          // The ID of SubtaskSpecs is used as the base sequenceName in segment allocation protocol.
          // The indexing tasks generated by the compaction task should use different sequenceNames
          // so that they can allocate valid segment IDs with no duplication.
          ParallelIndexIngestionSpec ingestionSpec = ingestionSpecs.get(i);
          final String baseSequenceName = createIndexTaskSpecId(i);
          return newTask(baseSequenceName, ingestionSpec);
        })
        .collect(Collectors.toList());

    if (indexTaskSpecs.isEmpty()) {
      log.warn("Can't find segments from inputSpec[%s], nothing to do.", ioConfig.getInputSpec());
      return TaskStatus.failure(getId(),"");
    } else {
      registerResourceCloserOnAbnormalExit(currentSubTaskHolder);
      final int totalNumSpecs = indexTaskSpecs.size();
      log.info("Generated [%d] mv task specs", totalNumSpecs);

      int failCnt = 0;
      for (ParallelIndexSupervisorTask eachSpec : indexTaskSpecs) {
        final String json = toolbox.getJsonMapper().writerWithDefaultPrettyPrinter().writeValueAsString(eachSpec);
        if (!currentSubTaskHolder.setTask(eachSpec)) {
          log.info("Task is asked to stop. Finish as failed.");
          return TaskStatus.failure(getId(),"");
        }
        try {
          if (eachSpec.isReady(toolbox.getTaskActionClient())) {
            log.info("Running indexSpec: " + json);
            final TaskStatus eachResult = eachSpec.run(toolbox);
            if (!eachResult.isSuccess()) {
              failCnt++;
              log.warn("Failed to run indexSpec: [%s].\nTrying the next indexSpec.", json);
            }
          } else {
            failCnt++;
            log.warn("indexSpec is not ready: [%s].\nTrying the next indexSpec.", json);
          }
        }
        catch (Exception e) {
          failCnt++;
          log.warn(e, "Failed to run indexSpec: [%s].\nTrying the next indexSpec.", json);
        }
      }

      log.info("Run [%d] specs, [%d] succeeded, [%d] failed", totalNumSpecs, totalNumSpecs - failCnt, failCnt);
      return failCnt == 0 ? TaskStatus.success(getId()) : TaskStatus.failure(getId(),"");
    }
  }

  @VisibleForTesting
  ParallelIndexSupervisorTask newTask(String taskId, ParallelIndexIngestionSpec ingestionSpec)
  {
    return new ParallelIndexSupervisorTask(
        taskId,
        getGroupId(),
        getTaskResource(),
        ingestionSpec,
        createContextForSubtask()
    );
  }

  @VisibleForTesting
  Map<String, Object> createContextForSubtask()
  {
    final Map<String, Object> newContext = new HashMap<>(getContext());
    newContext.put(CTX_KEY_APPENDERATOR_TRACKING_TASK_ID, getId());
    // Set the priority of the task.
    newContext.put(Tasks.PRIORITY_KEY, getPriority());
    // Store materializedSegment
    newContext.putAll(getContext());
    return newContext;
  }

  private String createIndexTaskSpecId(int i)
  {
    return StringUtils.format("%s_%d", getId(), i);
  }
  private static List<TimelineObjectHolder<String, DataSegment>> retrieveRelevantTimelineHolders(
      TaskToolbox toolbox,
      MaterializedViewTask.SegmentProvider segmentProvider,
      LockGranularity lockGranularityInUse
  ) throws IOException
  {
    final List<DataSegment> usedSegments =
        segmentProvider.findSegments(toolbox.getTaskActionClient());
    segmentProvider.checkSegments(lockGranularityInUse, usedSegments);
    final List<TimelineObjectHolder<String, DataSegment>> timelineSegments = new ArrayList<>();
    SegmentTimeline segmentTimeline = SegmentTimeline.forSegments(usedSegments);
    for (Interval interval : segmentProvider.intervals){
      timelineSegments.addAll(segmentTimeline.lookup(interval));
    }
    return timelineSegments;
  }
  /**
   * Generate {@link ParallelIndexIngestionSpec} from input segments.
   *
   * @return an empty list if input segments don't exist. Otherwise, a generated ingestionSpec.
   */
  @VisibleForTesting
  static List<ParallelIndexIngestionSpec> createIngestionSchema(
      final String baseDataSource,
      final String dataSource,
      final Clock clock,
      final TaskToolbox toolbox,
      final LockGranularity lockGranularityInUse,
      final MaterializedViewIOConfig ioConfig,
      final SegmentProvider segmentProvider,
      final MaterializedViewTask.PartitionConfigurationManager partitionConfigurationManager,
      @Nullable final DimensionsSpec dimensionsSpec,
      @Nullable final ClientCompactionTaskTransformSpec transformSpec,
      @Nullable final AggregatorFactory[] metricsSpec,
      @Nullable final ClientTaskGranularitySpec granularitySpec,
      final CoordinatorClient coordinatorClient,
      final SegmentCacheManagerFactory segmentCacheManagerFactory,
      final ServiceMetricEvent.Builder metricBuilder
  ) throws IOException
  {
    final List<TimelineObjectHolder<String, DataSegment>> timelineSegments = retrieveRelevantTimelineHolders(
        toolbox,
        segmentProvider,
        lockGranularityInUse
    );

    if (timelineSegments.size() == 0) {
      return Collections.emptyList();
    }

    final ParallelIndexTuningConfig tuningConfig = partitionConfigurationManager.computeTuningConfig();

    if (granularitySpec == null || granularitySpec.getSegmentGranularity() == null) {
      final List<ParallelIndexIngestionSpec> specs = new ArrayList<>();

      // original granularity
      final Map<Interval, List<DataSegment>> intervalToSegments = new TreeMap<>(
          Comparators.intervalsByStartThenEnd()
      );

      for (final DataSegment dataSegment : VersionedIntervalTimeline.getAllObjects(timelineSegments)) {
        intervalToSegments.computeIfAbsent(dataSegment.getInterval(), k -> new ArrayList<>())
                          .add(dataSegment);
      }

      // unify overlapping intervals to ensure overlapping segments compacting in the same indexSpec
      List<NonnullPair<Interval, List<DataSegment>>> intervalToSegmentsUnified = new ArrayList<>();
      Interval union = null;
      List<DataSegment> segments = new ArrayList<>();
      for (Map.Entry<Interval, List<DataSegment>> entry : intervalToSegments.entrySet()) {
        Interval cur = entry.getKey();
        if (union == null) {
          union = cur;
          segments.addAll(entry.getValue());
        } else if (union.overlaps(cur)) {
          union = Intervals.utc(union.getStartMillis(), Math.max(union.getEndMillis(), cur.getEndMillis()));
          segments.addAll(entry.getValue());
        } else {
          intervalToSegmentsUnified.add(new NonnullPair<>(union, segments));
          union = cur;
          segments = new ArrayList<>(entry.getValue());
        }
      }

      intervalToSegmentsUnified.add(new NonnullPair<>(union, segments));

      for (NonnullPair<Interval, List<DataSegment>> entry : intervalToSegmentsUnified) {
        final Interval interval = entry.lhs;
        final List<DataSegment> segmentsToCompact = entry.rhs;
        // If granularitySpec is not null, then set segmentGranularity. Otherwise,
        // creates new granularitySpec and set segmentGranularity
        Granularity segmentGranularityToUse = GranularityType.fromPeriod(interval.toPeriod()).getDefaultGranularity();
        final DataSchema dataSchema = createDataSchema(
            clock,
            toolbox.getEmitter(),
            metricBuilder,
            dataSource,
            interval,
            lazyFetchSegments(segmentsToCompact, toolbox.getSegmentCacheManager(), toolbox.getIndexIO()),
            dimensionsSpec,
            transformSpec,
            metricsSpec,
            granularitySpec == null
            ? new ClientTaskGranularitySpec(segmentGranularityToUse, null, null)
            : granularitySpec.withSegmentGranularity(segmentGranularityToUse)
        );

        specs.add(
            new ParallelIndexIngestionSpec(
                dataSchema,
                createIoConfig(
                    baseDataSource,
                    toolbox,
                    dataSchema,
                    null,
                    segmentProvider.getCandidateMaterializedSegments(),
                    coordinatorClient,
                    segmentCacheManagerFactory,
                    ioConfig
                ),
                tuningConfig
            )
        );
      }

      return specs;
    } else {
      // given segment granularity
      final DataSchema dataSchema = createDataSchema(
          clock,
          toolbox.getEmitter(),
          metricBuilder,
          dataSource,
          JodaUtils.umbrellaInterval(
              Iterables.transform(
                  VersionedIntervalTimeline.getAllObjects(timelineSegments),
                  DataSegment::getInterval
              )
          ),
          lazyFetchSegments(
              VersionedIntervalTimeline.getAllObjects(timelineSegments),
              toolbox.getSegmentCacheManager(),
              toolbox.getIndexIO()
          ),
          dimensionsSpec,
          transformSpec,
          metricsSpec,
          granularitySpec
      );

      return Collections.singletonList(
          new ParallelIndexIngestionSpec(
              dataSchema,
              createIoConfig(
                  baseDataSource,
                  toolbox,
                  dataSchema,
                  null,
                  segmentProvider.getCandidateMaterializedSegments(),
                  coordinatorClient,
                  segmentCacheManagerFactory,
                  ioConfig
              ),
              tuningConfig
          )
      );
    }
  }

  /**
   * Lazily fetch and load {@link QueryableIndex}, skipping tombstones.
   */
  private static Iterable<Pair<DataSegment, Supplier<ResourceHolder<QueryableIndex>>>> lazyFetchSegments(
      Iterable<DataSegment> dataSegments,
      SegmentCacheManager segmentCacheManager,
      IndexIO indexIO
  )
  {
    return Iterables.transform(
        Iterables.filter(dataSegments, dataSegment -> !dataSegment.isTombstone()),
        dataSegment -> fetchSegment(dataSegment, segmentCacheManager, indexIO)
    );
  }
  // Broken out into a separate function because Some tools can't infer the
  // pair type, but if the type is given explicitly, IntelliJ inspections raises
  // an error. Creating a function keeps everyone happy.
  private static Pair<DataSegment, Supplier<ResourceHolder<QueryableIndex>>> fetchSegment(
      DataSegment dataSegment,
      SegmentCacheManager segmentCacheManager,
      IndexIO indexIO
  )
  {
    return Pair.of(
        dataSegment,
        () -> {
          try {
            final Closer closer = Closer.create();
            final File file = segmentCacheManager.getSegmentFiles(dataSegment);
            closer.register(() -> segmentCacheManager.cleanup(dataSegment));
            final QueryableIndex queryableIndex = closer.register(indexIO.loadIndex(file));
            return new ResourceHolder<QueryableIndex>()
            {
              @Override
              public QueryableIndex get()
              {
                return queryableIndex;
              }

              @Override
              public void close()
              {
                try {
                  closer.close();
                }
                catch (IOException e) {
                  throw new RuntimeException(e);
                }
              }
            };
          }
          catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
    );
  }
  private static ParallelIndexIOConfig createIoConfig(
      String baseDataSource,
      TaskToolbox toolbox,
      DataSchema dataSchema,
      @Nullable Interval interval,
      List<WindowedSegmentId> segmentIds,
      CoordinatorClient coordinatorClient,
      SegmentCacheManagerFactory segmentLoaderFactory,
      final MaterializedViewIOConfig ioConfig
  )
  {
    return new ParallelIndexIOConfig(
        null,
        new DruidInputSource(
            baseDataSource,
            interval,
            interval == null ? segmentIds : null,
            null,
            dataSchema.getDimensionsSpec().getDimensionNames(),
            Arrays.stream(dataSchema.getAggregators()).map(AggregatorFactory::getName).collect(Collectors.toList()),
            toolbox.getIndexIO(),
            coordinatorClient,
            segmentLoaderFactory,
            toolbox.getConfig()
        ),
        null,
        ioConfig.appendToExisting(),
        ioConfig.isDropExisting()
    );
  }

  private static DataSchema createDataSchema(
      Clock clock,
      ServiceEmitter emitter,
      ServiceMetricEvent.Builder metricBuilder,
      String dataSource,
      Interval totalInterval,
      Iterable<Pair<DataSegment, Supplier<ResourceHolder<QueryableIndex>>>> segments,
      @Nullable DimensionsSpec dimensionsSpec,
      @Nullable ClientCompactionTaskTransformSpec transformSpec,
      @Nullable AggregatorFactory[] metricsSpec,
      @Nonnull ClientTaskGranularitySpec granularitySpec
  )
  {
    // Check index metadata & decide which values to propagate (i.e. carry over) for rollup & queryGranularity
    final ExistingSegmentAnalyzer existingSegmentAnalyzer = new ExistingSegmentAnalyzer(
        segments,
        granularitySpec.isRollup() == null,
        granularitySpec.getQueryGranularity() == null,
        dimensionsSpec == null,
        metricsSpec == null
    );
    long start = clock.millis();
    try {
      existingSegmentAnalyzer.fetchAndProcessIfNeeded();
    }
    finally {
      if (emitter != null) {
        emitter.emit(metricBuilder.setMetric("compact/segmentAnalyzer/fetchAndProcessMillis", clock.millis() - start));
      }
    }

    final Granularity queryGranularityToUse;
    if (granularitySpec.getQueryGranularity() == null) {
      queryGranularityToUse = existingSegmentAnalyzer.getQueryGranularity();
      log.info("Generate compaction task spec with segments original query granularity [%s]", queryGranularityToUse);
    } else {
      queryGranularityToUse = granularitySpec.getQueryGranularity();
      log.info(
          "Generate compaction task spec with new query granularity overrided from input [%s]",
          queryGranularityToUse
      );
    }

    final GranularitySpec uniformGranularitySpec = new UniformGranularitySpec(
        Preconditions.checkNotNull(granularitySpec.getSegmentGranularity()),
        queryGranularityToUse,
        granularitySpec.isRollup() == null ? existingSegmentAnalyzer.getRollup() : granularitySpec.isRollup(),
        Collections.singletonList(totalInterval)
    );

    // find unique dimensions
    final DimensionsSpec finalDimensionsSpec;
    if (dimensionsSpec == null) {
      finalDimensionsSpec = existingSegmentAnalyzer.getDimensionsSpec();
    } else {
      finalDimensionsSpec = dimensionsSpec;
    }

    final AggregatorFactory[] finalMetricsSpec;
    if (metricsSpec == null) {
      finalMetricsSpec = existingSegmentAnalyzer.getMetricsSpec();
    } else {
      finalMetricsSpec = metricsSpec;
    }

    return new DataSchema(
        dataSource,
        new TimestampSpec(ColumnHolder.TIME_COLUMN_NAME, "millis", null),
        finalDimensionsSpec,
        finalMetricsSpec,
        uniformGranularitySpec,
        transformSpec == null ? null : new TransformSpec(transformSpec.getFilter(), null)
    );
  }

  private static AggregatorFactory[] createMetricsSpec(
      List<NonnullPair<QueryableIndex, DataSegment>> queryableIndexAndSegments
  )
  {
    final List<AggregatorFactory[]> aggregatorFactories = queryableIndexAndSegments
        .stream()
        .map(pair -> pair.lhs.getMetadata().getAggregators()) // We have already done null check on index.getMetadata()
        .collect(Collectors.toList());
    final AggregatorFactory[] mergedAggregators = AggregatorFactory.mergeAggregators(aggregatorFactories);

    if (mergedAggregators == null) {
      throw new ISE("Failed to merge aggregators[%s]", aggregatorFactories);
    }
    return mergedAggregators;
  }

  @VisibleForTesting
  static class SegmentProvider
  {
    private final String baseDataSource;
    private final MaterializedViewInputSpec inputSpec;
    private final Set<Interval> intervals;

    SegmentProvider(String baseDataSource, MaterializedViewInputSpec inputSpec)
    {
      this.baseDataSource = Preconditions.checkNotNull(baseDataSource);
      this.inputSpec = inputSpec;
      this.intervals = inputSpec.findInterval(baseDataSource);
    }

    List<DataSegment> findSegments(TaskActionClient actionClient) throws IOException
    {
      List<DataSegment> materializedSegments = new ArrayList<>();
      for (Interval interval : intervals) {
        List<DataSegment> latestSegments = new ArrayList<>(
            actionClient.submit(new RetrieveUsedSegmentsAction(baseDataSource, interval, null, Segments.ONLY_VISIBLE))
        );
        for (DataSegment dataSegment : latestSegments) {
          WindowedSegmentId windowedSegmentId = new WindowedSegmentId(
              dataSegment.getId().toString(),
              Collections.singletonList(dataSegment.getInterval()),
              dataSegment.getSize()
          );
          if (getCandidateMaterializedSegments().contains(windowedSegmentId)) {
            materializedSegments.add(dataSegment);
          }
        }
      }
      if (getCandidateMaterializedSegments().size() != materializedSegments.size()) {
        throw new ISE(
            "Specified segments in the spec are different from the current used segments. "
            + "Possibly specified segments exists unused segment or current used segments was compacted."
        );
      }
      return materializedSegments;
    }

    private List<WindowedSegmentId> getCandidateMaterializedSegments()
    {
      return inputSpec.getSegments();
    }

    void checkSegments(LockGranularity lockGranularityInUse, List<DataSegment> latestSegments)
    {
      if (!inputSpec.validateSegments(lockGranularityInUse, latestSegments)) {
        throw new ISE(
            "Specified segments in the spec are different from the current used segments. "
            + "Possibly new segments would have been added or some segments have been unpublished."
        );
      }
    }
  }

  @VisibleForTesting
  static class PartitionConfigurationManager
  {
    @Nullable
    private final ParallelIndexTuningConfig tuningConfig;

    PartitionConfigurationManager(@Nullable ParallelIndexTuningConfig tuningConfig)
    {
      this.tuningConfig = tuningConfig;
    }

    @Nullable
    ParallelIndexTuningConfig computeTuningConfig()
    {
      ParallelIndexTuningConfig newTuningConfig = tuningConfig == null
                                          ? ParallelIndexTuningConfig.defaultConfig()
                                          : tuningConfig;
      PartitionsSpec partitionsSpec = newTuningConfig.getGivenOrDefaultPartitionsSpec();
      if (partitionsSpec instanceof DynamicPartitionsSpec) {
        final DynamicPartitionsSpec dynamicPartitionsSpec = (DynamicPartitionsSpec) partitionsSpec;
        partitionsSpec = new DynamicPartitionsSpec(
            dynamicPartitionsSpec.getMaxRowsPerSegment(),
            // Setting maxTotalRows to Long.MAX_VALUE to respect the computed maxRowsPerSegment.
            // If this is set to something too small, compactionTask can generate small segments
            // which need to be compacted again, which in turn making auto compaction stuck in the same interval.
            dynamicPartitionsSpec.getMaxTotalRowsOr(DynamicPartitionsSpec.DEFAULT_COMPACTION_MAX_TOTAL_ROWS)
        );
      }
      return newTuningConfig.withPartitionsSpec(partitionsSpec);
    }
  }

  public static class Builder
  {
    private final String baseDataSource;
    private final String dataSource;
    private final SegmentCacheManagerFactory segmentCacheManagerFactory;
    private final RetryPolicyFactory retryPolicyFactory;

    private MaterializedViewIOConfig ioConfig;
    @Nullable
    private DimensionsSpec dimensionsSpec;
    @Nullable
    private AggregatorFactory[] metricsSpec;
    @Nullable
    private ClientCompactionTaskTransformSpec transformSpec;
    @Nullable
    private ClientTaskGranularitySpec granularitySpec;
    @Nullable
    private TuningConfig tuningConfig;
    @Nullable
    private Map<String, Object> context;
    @Nullable
    private String taskId;

    public Builder(
        String baseDataSource,
        String dataSource,
        SegmentCacheManagerFactory segmentCacheManagerFactory,
        RetryPolicyFactory retryPolicyFactory
    )
    {
      this.baseDataSource = baseDataSource;
      this.dataSource = dataSource;
      this.segmentCacheManagerFactory = segmentCacheManagerFactory;
      this.retryPolicyFactory = retryPolicyFactory;
    }

    public Builder taskId(String taskId)
    {
      this.taskId = taskId;
      return this;
    }

    public Builder interval(Interval interval)
    {
      return inputSpec(new MaterializedIntervalSpec(Collections.singleton(interval), null), false);
    }
    public Builder segments(List<DataSegment> segments, boolean appendToExisting)
    {
      return inputSpec(SpecificMaterializedSegmentsSpec.fromSegments(segments), appendToExisting);
    }

    public Builder inputSpec(MaterializedViewInputSpec inputSpec, boolean appendToExisting)
    {
      this.ioConfig = new MaterializedViewIOConfig(inputSpec, appendToExisting);
      return this;
    }

    public Builder dimensionsSpec(DimensionsSpec dimensionsSpec)
    {
      this.dimensionsSpec = dimensionsSpec;
      return this;
    }

    public Builder metricsSpec(AggregatorFactory[] metricsSpec)
    {
      this.metricsSpec = metricsSpec;
      return this;
    }
    public Builder transformSpec(ClientCompactionTaskTransformSpec transformSpec)
    {
      this.transformSpec = transformSpec;
      return this;
    }
    public Builder granularitySpec(ClientTaskGranularitySpec granularitySpec)
    {
      this.granularitySpec = granularitySpec;
      return this;
    }

    public Builder tuningConfig(TuningConfig tuningConfig)
    {
      this.tuningConfig = tuningConfig;
      return this;
    }

    public Builder context(Map<String, Object> context)
    {
      this.context = context;
      return this;
    }

    public MaterializedViewTask build()
    {
      return new MaterializedViewTask(
          taskId,
          null,
          baseDataSource,
          dataSource,
          null,
          null,
          ioConfig,
          null,
          dimensionsSpec,
          metricsSpec,
          transformSpec,
          granularitySpec,
          tuningConfig,
          context,
          segmentCacheManagerFactory
      );
    }
  }
}
