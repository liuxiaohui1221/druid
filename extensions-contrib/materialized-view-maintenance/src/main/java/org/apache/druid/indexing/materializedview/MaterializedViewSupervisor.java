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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.client.materializedview.DerivativeDataSourceMetadata;
import org.apache.druid.client.materializedview.MaterializedViewUtils;
import org.apache.druid.common.guava.SettableSupplier;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.Supervisor;
import org.apache.druid.indexing.overlord.supervisor.SupervisorReport;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManager;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.LagStats;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.MetadataSupervisorManager;
import org.apache.druid.metadata.SqlSegmentsMetadataManager;
import org.apache.druid.timeline.BaseShardSpecsSpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.MaterializedDataSegment;
import org.apache.druid.timeline.MaterializedSpec;
import org.apache.druid.utils.CollectionUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.LocalDateTime;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * 物化视图分区内触发全量物化情况：
 * 1.forceRollup为true，物化视图分区内有对应新增base segment，触发物化视图区间全量物化
 * 2.enableSecondRegionOverwrite为true（即启用第二个物化区间范围内的segment进行全量物化），物化视图分区处于全量物化区间内（即第二个物化区间）内有对应新增base
 * segment，触发物化视图区间全量物化.
 * 3.物化视图分区内,base segment的version与物化视图segment的version不一致，触发物化视图区间全量物化。
 */
public class MaterializedViewSupervisor implements Supervisor
{
  private static final EmittingLogger log = new EmittingLogger(MaterializedViewSupervisor.class);
  private static final int DEFAULT_MAX_TASK_COUNT = 1;
  // there is a lag between derivatives and base dataSource, to prevent repeatedly building for some delay data. 
  private static final long DEFAULT_MIN_DATA_LAG_MS = TimeUnit.DAYS.toMillis(1);

  private final long defaultCacheTimeMs;
  private final MetadataSupervisorManager metadataSupervisorManager;
  private final IndexerMetadataStorageCoordinator metadataStorageCoordinator;
  private final SqlSegmentsMetadataManager sqlSegmentsMetadataManager;
  private final MaterializedViewSupervisorSpec spec;
  private final TaskMaster taskMaster;
  private final TaskStorage taskStorage;
  private final MaterializedViewTaskConfig config;
  private final PolicyConfig policyConfig;
  private final SupervisorStateManager stateManager;
  private final String dataSource;
  private final String supervisorId;
  private final int maxTaskCount;
  private final Period skipPeriodFromLatest;
  private final Period ingestionTimeRange;
  private final long inputMaxSizeForAppendingTask;
  private final Set<Task> runningTaskSets = new HashSet<>();
  private final Map<Interval, Task> runningTasks = new HashMap<>();
  private final Map<Interval, String> runningVersion = new HashMap<>();
  // taskLock is used to synchronize runningTask and runningVersion
  private final Object taskLock = new Object();
  // stateLock is used to synchronize materializedViewSupervisor's status
  private final Object stateLock = new Object();
  private boolean started = false;
  private ListenableFuture<?> future = null;
  private ListeningScheduledExecutorService exec = null;
  // In the missing intervals, baseDataSource has data but derivedDataSource does not, which means
  // data in these intervals of derivedDataSource needs to be rebuilt.
  private Set<Interval> missInterval = new HashSet<>();
  // record current max ingestion end time Ms
  private final SettableSupplier<Long> secondIngestionEndTimeMs = new SettableSupplier<>(Long.MAX_VALUE);
  private final SettableSupplier<Long> maxIngestionEndTimeMs = new SettableSupplier<>(Long.MAX_VALUE);
  private final SettableSupplier<Long> minIngestionStartTimeMs = new SettableSupplier<>(0L);
  private final DateTime maxIngestionEndTimeMsForOverwriteHadoop;
  private final DateTime minIngestionStartTimeMsForOverwriteHadoop;
  private final Map<Interval, AtomicLong> cacheIntervalTaskStartTimes;

  public MaterializedViewSupervisor(
      TaskMaster taskMaster,
      TaskStorage taskStorage,
      MetadataSupervisorManager metadataSupervisorManager,
      SqlSegmentsMetadataManager sqlSegmentsMetadataManager,
      IndexerMetadataStorageCoordinator metadataStorageCoordinator,
      MaterializedViewTaskConfig config,
      MaterializedViewSupervisorSpec spec,
      PolicyConfig policyConfig
  )
  {
    this.taskMaster = taskMaster;
    this.taskStorage = taskStorage;
    this.metadataStorageCoordinator = metadataStorageCoordinator;
    this.sqlSegmentsMetadataManager = sqlSegmentsMetadataManager;
    this.metadataSupervisorManager = metadataSupervisorManager;
    this.config = config;
    this.spec = spec;
    this.stateManager = new SupervisorStateManager(spec.getSupervisorStateManagerConfig(), spec.isSuspended());
    this.dataSource = spec.getDataSourceName();
    this.policyConfig = policyConfig;
    this.supervisorId = StringUtils.format("MVSupervisor-%s", dataSource);
    this.maxTaskCount = spec.getContext().containsKey("maxTaskCount")
        ? Integer.parseInt(String.valueOf(spec.getContext().get("maxTaskCount")))
        : DEFAULT_MAX_TASK_COUNT;
    this.skipPeriodFromLatest = policyConfig.getSkipPeriodFromLatest();
    this.ingestionTimeRange = policyConfig.getIngestDuration();
    this.inputMaxSizeForAppendingTask = policyConfig.getInputMaxSizeForAppendingTask();
    this.defaultCacheTimeMs = config.getTaskCheckDuration().toStandardDuration().getMillis() + 1000;
    this.cacheIntervalTaskStartTimes = new ConcurrentHashMap<>();
    this.maxIngestionEndTimeMsForOverwriteHadoop = getNow();
    this.minIngestionStartTimeMsForOverwriteHadoop = this.maxIngestionEndTimeMsForOverwriteHadoop.minus(config.getHadoopIntervalCheckDuration());
    log.info(
        "Compute ingestion overwrite hadoop time range[%s,%s],maxTaskCount:%s,skipPeriodFromLatest:%s,"
        + "ingestionTimeRange:%s",
        minIngestionStartTimeMsForOverwriteHadoop, maxIngestionEndTimeMsForOverwriteHadoop,maxTaskCount,
        skipPeriodFromLatest,ingestionTimeRange
    );
  }

  @Override
  public void start()
  {
    synchronized (stateLock) {
      Preconditions.checkState(!started, "already started");

      DataSourceMetadata metadata = metadataStorageCoordinator.retrieveDataSourceMetadata(dataSource);
      if (null == metadata) {
        metadataStorageCoordinator.insertDataSourceMetadata(
            dataSource,
            new DerivativeDataSourceMetadata(
                spec.getBaseDataSource(),
                spec.getGranularitySpec(),
                spec.getDimensions(),
                spec.getMetrics(),
                null
            )
        );
      }
      exec = MoreExecutors.listeningDecorator(Execs.scheduledSingleThreaded(StringUtils.encodeForFormat(supervisorId)));
      final Duration delay = config.getTaskCheckDuration().toStandardDuration();
      future = exec.scheduleWithFixedDelay(
          MaterializedViewSupervisor.this::run,
          0,
          delay.getMillis(),
          TimeUnit.MILLISECONDS
      );
      started = true;
    }
  }

  @VisibleForTesting
  public void run()
  {
    try {
      if (spec.isSuspended()) {
        log.info(
            "Materialized view supervisor[%s:%s] is suspended",
            spec.getId(),
            spec.getDataSourceName()
        );
        return;
      }

      DataSourceMetadata metadata = metadataStorageCoordinator.retrieveDataSourceMetadata(dataSource);
      if (metadata instanceof DerivativeDataSourceMetadata
          && spec.getBaseDataSource().equals(((DerivativeDataSourceMetadata) metadata).getBaseDataSource())) {
        checkSegmentsAndSubmitTasks();
      } else {
        log.error(
            "Failed to start %s. Metadata in database(%s) is different from new dataSource metadata(%s)",
            supervisorId,
            metadata,
            spec
        );
      }
    }
    catch (Exception e) {
      stateManager.recordThrowableEvent(e);
      log.makeAlert(e, StringUtils.format("uncaught exception in %s.", supervisorId)).emit();
    }
    finally {
      stateManager.markRunFinished();
    }
  }

  @Override
  public void stop(boolean stopGracefully)
  {
    synchronized (stateLock) {
      Preconditions.checkState(started, "not started");

      stateManager.maybeSetState(SupervisorStateManager.BasicState.STOPPING);

      // stop all schedulers and threads
      if (stopGracefully) {
        synchronized (taskLock) {
          future.cancel(false);
          future = null;
          exec.shutdownNow();
          exec = null;
          clearTasks();
        }
      } else {
        future.cancel(true);
        future = null;
        exec.shutdownNow();
        exec = null;
        synchronized (taskLock) {
          clearTasks();
        }
      }
      started = false;
    }

  }

  @Override
  public SupervisorReport getStatus()
  {
    return new MaterializedViewSupervisorReport(
        dataSource,
        DateTimes.nowUtc(),
        spec.isSuspended(),
        spec.getBaseDataSource(),
        spec.getDimensions(),
        spec.getMetrics(),
        JodaUtils.condenseIntervals(missInterval),
        stateManager.isHealthy(),
        stateManager.getSupervisorState().getBasicState(),
        stateManager.getExceptionEvents()
    );
  }

  @Override
  public SupervisorStateManager.State getState()
  {
    return stateManager.getSupervisorState();
  }

  @Override
  public Boolean isHealthy()
  {
    return stateManager.isHealthy();
  }

  @Override
  public void reset(DataSourceMetadata dataSourceMetadata)
  {
    if (dataSourceMetadata == null) {
      // if oldMetadata is different from spec, tasks and segments will be removed when reset.
      DataSourceMetadata oldMetadata = metadataStorageCoordinator.retrieveDataSourceMetadata(dataSource);
      if (oldMetadata instanceof DerivativeDataSourceMetadata) {
        if (!((DerivativeDataSourceMetadata) oldMetadata).getBaseDataSource().equals(spec.getBaseDataSource())) {
          synchronized (taskLock) {
            clearTasks();
            clearSegments();
          }
        }
      }
      commitDataSourceMetadata(
          new DerivativeDataSourceMetadata(
              spec.getBaseDataSource(),
              spec.getGranularitySpec(),
              spec.getDimensions(),
              spec.getMetrics(),
              null
          )
      );
    } else {
      throw new IAE("DerivedDataSourceMetadata is not allowed to reset to a new DerivedDataSourceMetadata");
    }
  }

  @Override
  public void resetOffsets(DataSourceMetadata resetDataSourceMetadata)
  {
    throw new UnsupportedOperationException("Reset offsets not supported in MaterializedViewSupervisor");
  }

  @Override
  public void checkpoint(int taskGroupId, DataSourceMetadata checkpointMetadata)
  {
    // do nothing
  }

  @Override
  public LagStats computeLagStats()
  {
    throw new UnsupportedOperationException("Compute Lag Stats not supported in MaterializedViewSupervisor");
  }

  @Override
  public int getActiveTaskGroupsCount()
  {
    throw new UnsupportedOperationException("Get Active Task Groups Count is not supported in MaterializedViewSupervisor");
  }

  /**
   * Find intervals in which derived dataSource should rebuild the segments.
   * Choose the latest intervals to create new Task and submit it.
   */
  @VisibleForTesting
  void checkSegmentsAndSubmitTasks()
  {
    synchronized (taskLock) {
      List<Interval> intervalsToRemove = new ArrayList<>();
      for (Map.Entry<Interval, Task> entry : runningTasks.entrySet()) {
        Optional<TaskStatus> taskStatus = taskStorage.getStatus(entry.getValue().getId());
        if ((!taskStatus.isPresent() || !taskStatus.get().isRunnable()) && reachCacheTimeout(entry.getKey())) {
          runningTaskSets.remove(entry.getValue());
          intervalsToRemove.add(entry.getKey());
        }
      }
      for (Interval interval : intervalsToRemove) {
        runningTasks.remove(interval);
        runningVersion.remove(interval);
      }

      if (runningTaskSets.size() == maxTaskCount) {
        //if the number of running tasks reach the max task count, supervisor won't submit new tasks.
        return;
      }
      Pair<SortedMap<Interval, Pair<Boolean, String>>, Map<Interval, List<DataSegment>>> toBuildIntervalAndBaseSegments =
          checkSegments();
      if (toBuildIntervalAndBaseSegments == null) {
        return;
      }
      SortedMap<Interval, Pair<Boolean, String>> sortedToBuildVersion = toBuildIntervalAndBaseSegments.lhs;
      Map<Interval, List<DataSegment>> baseSegments = toBuildIntervalAndBaseSegments.rhs;
      missInterval = sortedToBuildVersion.keySet();

      submitTasks(sortedToBuildVersion, baseSegments);

      clearIntervalCacheTimeout(cacheIntervalTaskStartTimes);
    }
  }

  /**
   * 首次添加interval时重新计时，达到缓存时间时清除
   *
   * @param inputInterval
   * @return
   */
  @VisibleForTesting
  public boolean reachCacheTimeout(Interval inputInterval)
  {
    AtomicLong intervalStartTime = cacheIntervalTaskStartTimes.get(inputInterval);
    if (intervalStartTime == null) {
      log.warn("Do not cache interval[%s] for caches[%s]", inputInterval, cacheIntervalTaskStartTimes.size());
      cacheIntervalTaskStartTimes.put(inputInterval, new AtomicLong(System.currentTimeMillis()));
      return false;
    }
    boolean reachTimeout = System.currentTimeMillis() - intervalStartTime.get() > defaultCacheTimeMs;
    if (reachTimeout) {
      cacheIntervalTaskStartTimes.remove(inputInterval);
    }
    return reachTimeout;
  }

  @VisibleForTesting
  public void clearIntervalCacheTimeout(Map<Interval, AtomicLong> cacheIntervalTaskStartTimes)
  {
    Iterator<Map.Entry<Interval, AtomicLong>> iterator = cacheIntervalTaskStartTimes.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<Interval, AtomicLong> next = iterator.next();
      boolean reachTimeout = System.currentTimeMillis() - next.getValue().get() > defaultCacheTimeMs;
      if (reachTimeout) {
        iterator.remove();
      }
    }
  }

  @VisibleForTesting
  public MaterializedViewTaskConfig getConfig()
  {
    return config;
  }

  @VisibleForTesting
  Pair<Map<Interval, Task>, Map<Interval, String>> getRunningTasks()
  {
    return new Pair<>(runningTasks, runningVersion);
  }

  /**
   * Find infomation about the intervals in which derived dataSource data should be rebuilt.
   * The infomation includes the version and DataSegments list of a interval.
   * The intervals include: in the interval,
   *  1) baseDataSource has data, but the derivedDataSource does not;
   *  2) version of derived segments isn't the max(created_date) of all base segments;
   *
   *  Drop the segments of the intervals in which derivedDataSource has data, but baseDataSource does not.
   *
   * @return the left part of Pair: interval -> version, and the right part: interval -> DataSegment list.
   *          Version and DataSegment list can be used to create HadoopIndexTask.
   *          Derived datasource data in all these intervals need to be rebuilt.
   */
  @VisibleForTesting
  Set<Task> getRunningTaskSets()
  {
    return runningTaskSets;
  }

  /**
   * 1.不同interval物化: baseDatasource存在，但物化视图对应不存在
   * 2.相同interval物化：
   * 根据version比较并选出需要overwrite模式提交物化的baseDatasource的interval及对应所有segment列表
   * 相同version下，比较并选出没有物化的baseDatasource的segment列表及对应interval。
   * todo 比较并找出overwrite模式不同的minorVersion对应的interval列表及对应segment列表
   *
   * @return
   */
  @VisibleForTesting
  Pair<SortedMap<Interval, Pair<Boolean, String>>, Map<Interval, List<DataSegment>>> checkSegments()
  {
    Map<Interval, Pair<Boolean, String>> toBuildHistoryMvInterval = new HashMap<>();
    // Pair<interval -> version, interval -> list<DataSegment>>
    Collection<DataSegment> derivativeSegmentsCollection =
        metadataStorageCoordinator.retrieveAllUsedSegments(dataSource, Segments.ONLY_VISIBLE);
    Pair<Map<Interval, String>, Map<Interval, List<DataSegment>>> derivativeSegmentsSnapshot =
        getMaterializedVersionAndBaseSegments(derivativeSegmentsCollection, toBuildHistoryMvInterval);

    // Pair<interval -> version, interval -> list<DataSegment>>
    Collection<DataSegment> baseSegmentsCollection =
        metadataStorageCoordinator.retrieveAllUsedSegments(spec.getBaseDataSource(), Segments.ONLY_VISIBLE);
    if (baseSegmentsCollection.size() == 0) {
      return null;
    }

    Map<Interval, Pair<Boolean, String>> toBuildBaseIntervalFromOlderMvInterval = new HashMap<>();
    Pair<Map<Interval, String>, Map<Interval, List<DataSegment>>> baseSegmentsSnapshot =
        getVersionAndBaseSegments(
            baseSegmentsCollection,
            toBuildHistoryMvInterval,
            toBuildBaseIntervalFromOlderMvInterval,
            spec.getGranularitySpec().getSegmentGranularity()
        );
    log.info(
        "Found older interval and candidate overwrite toBuildInterval[%s]",
        toBuildBaseIntervalFromOlderMvInterval
    );

    // baseSegments are used to create BatchTask
    Map<Interval, List<DataSegment>> baseSegments = baseSegmentsSnapshot.rhs;
    // already materialized segments need to filter it
    Map<Interval, List<DataSegment>> derivativeSegments = derivativeSegmentsSnapshot.rhs;
    // use max created_date of base segments as the version of derivative segments
    Map<Interval, String> baseVersion = baseSegmentsSnapshot.lhs;
    Map<Interval, String> derivativeVersion = derivativeSegmentsSnapshot.lhs;
    SortedMap<Interval, Pair<Boolean, String>> sortedToBuildInterval =
        new TreeMap<>(Comparators.intervalsByStartThenEnd().reversed());

    MapDifference<Interval, String> difference = Maps.difference(baseVersion, derivativeVersion);
    // find new base intervals
    // interval-> Pair<isoverwrite then true, version>
    Map<Interval, Pair<Boolean, String>> toBuildInterval = new HashMap<>(CollectionUtils.mapValues(
        difference.entriesOnlyOnLeft(), v -> new Pair<>(spec.forceOverwrite(), v)
    ));
    log.info("Found new toBuildInterval[%s]", toBuildInterval);

    // diff version must overwrite
    // check interval's version (different version need overwrite)
    // if Materialized view interval exists overwrite base interval in MVinterval,
    // then need included all interval and segments (even if some parts already materialized)
    Map<Interval, MapDifference.ValueDifference<String>> diffIntervalVersions =
        new HashMap<>(difference.entriesDiffering());
    // some diff version interval need suppliment all other interval in mvInterval

    Map<Interval, Pair<Boolean, String>> supplementVersionToBuildIntervals = supplementBaseIntervalsInMvIntervalForOverwrite(
        diffIntervalVersions,
        baseVersion,
        derivativeVersion,
        baseSegments
    );
    toBuildInterval.putAll(supplementVersionToBuildIntervals);
    log.info("Found diff version and supplement toBuildInterval[%s]", supplementVersionToBuildIntervals);

    // common interva's version : need check segments whether exists new segments,
    // if yes then add all(force overwrite) or add new.
    Map<Interval, String> commonIntervalVersions = new HashMap<>(difference.entriesInCommon());

    if (spec.forceOverwrite()) {
      //check new segments and force overwrite
      overwriteCheckNewSegments(
          toBuildInterval,
          baseVersion,
          baseSegments,
          derivativeVersion,
          derivativeSegments,
          commonIntervalVersions
      );
    } else {
      //check new segments and auto chose mode(appending or overwrite)
      appendingCheckNewSegments(
          toBuildInterval,
          baseVersion,
          baseSegments,
          derivativeSegments,
          commonIntervalVersions
      );
    }

    Map<Interval, Pair<Boolean, String>> toBuildFilteredBaseIntervalFromMvInterval = new HashMap<>();
    for (Map.Entry<Interval, Pair<Boolean, String>> entry : toBuildBaseIntervalFromOlderMvInterval.entrySet()) {
      if (isIncludedHadoopIngestionPeriod(entry.getKey())) {
        toBuildFilteredBaseIntervalFromMvInterval.put(entry.getKey(), entry.getValue());
      } else {
        toBuildInterval.remove(entry.getKey());
      }
    }
    sortedToBuildInterval.putAll(toBuildInterval);
    sortedToBuildInterval.putAll(toBuildFilteredBaseIntervalFromMvInterval);

    // if some intervals are in running tasks and the versions are the same, remove it from toBuildInterval
    // if some intervals are in running tasks, but the versions are different, stop the task.
    final List<Interval> intervalsToRemove = new ArrayList<>();
    runningVersion.forEach((interval, version) -> {
      if (sortedToBuildInterval.containsKey(interval)) {
        if (sortedToBuildInterval.get(interval).rhs.equals(version)) {
          sortedToBuildInterval.remove(interval);
        } else {
          if (taskMaster.getTaskQueue().isPresent()) {
            if (runningTasks.containsKey(interval)) {
              taskMaster.getTaskQueue().get().shutdown(runningTasks.get(interval).getId(), "version mismatch");
              runningTaskSets.remove(runningTasks.get(interval));
              cacheIntervalTaskStartTimes.remove(interval);
            }
            intervalsToRemove.add(interval);
          }
        }
      }
    });
    for (Interval interval : intervalsToRemove) {
      runningTasks.remove(interval);
      runningVersion.remove(interval);
    }

    log.info(
        "Found candidate intervals[%s],including hadoop interval[%s]",
        sortedToBuildInterval,
        toBuildBaseIntervalFromOlderMvInterval
    );
    return new Pair<>(sortedToBuildInterval, baseSegments);
  }

  private void appendingCheckNewSegments(
      Map<Interval, Pair<Boolean, String>> toBuildInterval,
      Map<Interval, String> baseVersion,
      Map<Interval, List<DataSegment>> baseSegments,
      Map<Interval, List<DataSegment>> derivativeSegments,
      Map<Interval, String> commonVersion
  )
  {
    // find added segments need to materialized
    for (Map.Entry<Interval, String> commonEntry : commonVersion.entrySet()) {
      final String versionOfBase = baseVersion.get(commonEntry.getKey());
      boolean existsOverwrite = existsOverwriteInSameGranularity(toBuildInterval, commonEntry.getKey())
                                || policyConfig.isEnableSecondPeriodOverwrite()
                                   && isIncludedSecondPeriod(commonEntry.getKey());
      // filter already materialized segments,and added segments need to materialized (added mode submit)
      boolean hasAddedSegments = checkAddedOrRemoveCommonSegmentsInInterval(
          baseSegments.get(commonEntry.getKey()),
          derivativeSegments.get(commonEntry.getKey()),
          existsOverwrite
      );
      if (hasAddedSegments) {
        log.debug(
            "Found interval[%s] exist new segments.",
            commonEntry.getKey()
        );
        if (existsOverwrite) {
          toBuildInterval.put(commonEntry.getKey(), new Pair<>(existsOverwrite, versionOfBase));
        } else {
          toBuildInterval.putIfAbsent(commonEntry.getKey(), new Pair<>(existsOverwrite, versionOfBase));
        }
      } else {
        // if exists overwrite mode interval in MVInterval, then can't remove this
        if (!existsOverwrite) {
          // remove totally same segments
          toBuildInterval.remove(commonEntry.getKey());
          baseSegments.remove(commonEntry.getKey());
        }
      }
    }
  }

  private boolean existsOverwriteInSameGranularity(
      Map<Interval, Pair<Boolean, String>> toBuildInterval,
      Interval baseInterval
  )
  {
    Interval mvInterval = getMVInterval(baseInterval);
    for (Map.Entry<Interval, Pair<Boolean, String>> entry : toBuildInterval.entrySet()) {
      if (mvInterval.contains(entry.getKey()) && entry.getValue().lhs) {
        return true;
      }
    }
    return false;
  }

  /**
   * overwrite mode check new segments in common interval and version
   *
   * @param toBuildInterval
   * @param baseSegments
   * @param derivativeSegments
   * @param commonIntervalVersions
   */
  private void overwriteCheckNewSegments(
      Map<Interval, Pair<Boolean, String>> toBuildInterval,
      Map<Interval, String> baseVersions,
      Map<Interval, List<DataSegment>> baseSegments,
      Map<Interval, String> derivativeVersions,
      Map<Interval, List<DataSegment>> derivativeSegments,
      Map<Interval, String> commonIntervalVersions
  )
  {
    // check new segments in common interval version
    Set<Interval> inCompleteMvIntervalFromCommon = new HashSet<>();
    Set<Interval> commonMvIntervals = new HashSet<>();
    for (Map.Entry<Interval, String> commonEntry : commonIntervalVersions.entrySet()) {
      Interval mvInterval = getMVInterval(commonEntry.getKey());
      commonMvIntervals.add(mvInterval);
      // compare where new segments with same interval version
      if (checkAddedOrRemoveCommonSegmentsInInterval(
          baseSegments.get(commonEntry.getKey()),
          derivativeSegments.get(commonEntry.getKey()),
          true
      )) {
        // new segments
        inCompleteMvIntervalFromCommon.add(mvInterval);
      }
    }

    // check incomplete from common mvInterval
    inCompleteMvIntervalFromCommon.addAll(checkIncompleteVersions(
        commonMvIntervals,
        baseVersions,
        derivativeVersions
    ));

    // supplement baseInterval(common interval but new segments or new interval) to toBuuildInterval
    for (Map.Entry<Interval, String> baseVer : baseVersions.entrySet()) {
      if (inCompleteMvIntervalFromCommon.contains(getMVInterval(baseVer.getKey()))) {
        toBuildInterval.put(baseVer.getKey(), new Pair<>(true, baseVer.getValue()));
      }
    }
  }

  @VisibleForTesting
  Set<Interval> checkIncompleteVersions(
      Set<Interval> commonMvIntervals,
      Map<Interval, String> baseVersions,
      Map<Interval, String> derivativeVersions
  )
  {
    Set<Interval> inCompleteMvIntervalVersions = new HashSet<>();
    for (Interval mvInterval : commonMvIntervals) {
      int baseCount = 0, derivativeCount = 0;
      for (Interval bas : baseVersions.keySet()) {
        if (mvInterval.contains(bas)) {
          baseCount++;
        }
      }
      for (Interval der : derivativeVersions.keySet()) {
        if (mvInterval.contains(der)) {
          derivativeCount++;
        }
      }
      if (baseCount != derivativeCount) {
        inCompleteMvIntervalVersions.add(mvInterval);
      }
    }
    return inCompleteMvIntervalVersions;
  }

  private Map<Interval, Pair<Boolean, String>> supplementBaseIntervalsInMvIntervalForOverwrite(
      Map<Interval, MapDifference.ValueDifference<String>> diffIntervalVersions,
      Map<Interval, String> baseVersion,
      Map<Interval, String> derivativeVersion,
      Map<Interval, List<DataSegment>> baseSegments
  )
  {
    Map<Interval, Pair<Boolean, String>> diffVersionAndSupplementVersionToBuildIntervals = new HashMap<>();
    for (Map.Entry<Interval, MapDifference.ValueDifference<String>> entry : diffIntervalVersions.entrySet()) {
      if (!isIncludedIngestionPeriod(entry.getKey())) {
        log.info(
            "Filter overwrite interval[%s],because it don't be included in ingestion time range[%s,%s]",
            entry.getKey(),
            new DateTime(minIngestionStartTimeMs.get(), DateTimeZone.UTC),
            new DateTime(maxIngestionEndTimeMs.get(), DateTimeZone.UTC)
        );
        continue;
      }
      // different version need overwrite mode submit
      final String versionOfBase = baseVersion.get(entry.getKey());
      final String versionOfDerivative = derivativeVersion.get(entry.getKey());
      final int baseCount = baseSegments.get(entry.getKey()).size();
      // baseSegment may be upgrade version caused by compacted
      int usedCount = metadataStorageCoordinator
          .retrieveUsedSegmentsForInterval(spec.getBaseDataSource(), entry.getKey(), Segments.ONLY_VISIBLE).size();
      if (baseCount == usedCount) {
        log.info(
            "[%s] Need overwrite materialized,because interval[%s]'s version[%s] is different from old version[%s]",
            spec.getBaseDataSource(),
            entry.getKey(),
            versionOfBase,
            versionOfDerivative
        );

        // overwrite mvInterval condition: different baseInterval version need supplement other baseInterval(included already materialized) in mvInterval,
        // that means other interval & segments with this interval same belong to MVinterval also need to build again.
        // otherwise,it will always repeat materialize this mvInterval if druid overshadowned already materialized other baseInterval.
        diffVersionAndSupplementVersionToBuildIntervals.put(entry.getKey(), new Pair<>(true, versionOfBase));

      }
    }

    // supplement interval
    Map<Interval, Pair<Boolean, String>> supplementVersionToBuildIntervals = supplementIntervalInSameGranularity(
        baseVersion,
        diffVersionAndSupplementVersionToBuildIntervals.keySet()
    );
    diffVersionAndSupplementVersionToBuildIntervals.putAll(supplementVersionToBuildIntervals);
    return diffVersionAndSupplementVersionToBuildIntervals;
  }

  private Interval getMVInterval(Interval baseInterval)
  {
    return spec.getGranularitySpec()
               .getSegmentGranularity()
               .bucket(baseInterval.getStart());
  }

  private Map<Interval, Pair<Boolean, String>> supplementIntervalInSameGranularity(
      Map<Interval, String> baseVersion,
      Set<Interval> baseDiffVersions
  )
  {
    Map<Interval, Pair<Boolean, String>> toBuildIntervalSupplement = new HashMap<>();
    for (Interval baseInterval : baseDiffVersions) {
      Interval materializedInterval = getMVInterval(baseInterval);
      for (Map.Entry<Interval, String> entry : baseVersion.entrySet()) {
        if (materializedInterval.contains(entry.getKey())) {
          toBuildIntervalSupplement.put(entry.getKey(), new Pair<>(true, entry.getValue()));
        }
      }
    }
    return toBuildIntervalSupplement;
  }

  @VisibleForTesting
  boolean checkAddedOrRemoveCommonSegmentsInInterval(
      @Nullable List<DataSegment> baseIntervalSegments,
      List<DataSegment> derivatedIntervalSegments,
      boolean onlyCheck
  )
  {
    if (baseIntervalSegments == null || baseIntervalSegments.size() == 0) {
      return false;
    }
    // found already materialized segments in same interval
    Set<Integer> alreadyMaterializedPartNums = new HashSet<>();
    for (DataSegment d : derivatedIntervalSegments) {
      if (d.getMaterializedSpec() == null) {
        continue;
      }
      BaseShardSpecsSpec baseShardSpecsSpec = d.getMaterializedSpec().getBaseShardSpecsSpec();
      if (baseShardSpecsSpec == null) {
        throw new IAE(
            "WTF? MaterializedDataSegment's MaterializedSpec should not null! materialized info:[%s]",
            d.getMaterializedSpec()
        );
      }

      for (int id = baseShardSpecsSpec.getStartPartitionNumber(); id
                                                                  < baseShardSpecsSpec.getEndPartitionNumber(); id++) {
        alreadyMaterializedPartNums.add(id);
      }
    }
    if (log.isDebugEnabled()) {
      log.debug(
          "Remove interval[%s] common partitionNum: materializedPartNums[%s], sortedBasePartNums[%s]",
          baseIntervalSegments.get(0).getInterval(),
          alreadyMaterializedPartNums,
          baseIntervalSegments.stream()
                              .map(d -> d.getId().getPartitionNum())
                              .sorted().collect(Collectors.toList())
      );
    }
    if (onlyCheck) {
      List<Integer> materializedPartNums = Arrays.asList(alreadyMaterializedPartNums.toArray(new Integer[0]));
      List<Integer> sortedBasePartNums = baseIntervalSegments.stream()
                                                             .map(d -> d.getId().getPartitionNum())
                                                             .collect(Collectors.toList());
      return !materializedPartNums.containsAll(sortedBasePartNums);
    } else {
      // appendingToExists: remove materialized segments
      // filter already materialized segments from baseIntervalSegments
      // exists added segments if baseIntervalSegments is not empty
      Iterator<DataSegment> iterator = baseIntervalSegments.iterator();
      while (iterator.hasNext()) {
        DataSegment dataSegment = iterator.next();
        int partNum = dataSegment.getId().getPartitionNum();
        if (alreadyMaterializedPartNums.contains(partNum)) {
          iterator.remove();
        }
      }
      //has remaining
      return baseIntervalSegments.size() > 0;
    }
  }

  @VisibleForTesting
  void submitTasks(
      SortedMap<Interval, Pair<Boolean, String>> unHandledSortedToBuildVersion,
      Map<Interval, List<DataSegment>> baseSegments
  )
  {
    // compute interval's score to sort toBudilIntervals
    ScoreMaterializedViewSearchPolicy sortedIntervalPolicy = new ScoreMaterializedViewSearchPolicy();
    ScoreMaterializedViewIterator intervalItr = sortedIntervalPolicy.reset(
        baseSegments,
        unHandledSortedToBuildVersion,
        policyConfig
    );
    // all candidate intervals group by materializedview segment granularity
    // if any group contains an overwrite mode,then this group must overwrite submit in one task.
    List<CandidateGroup> candidateGroups = groupIntervalBySegmentGranularity(intervalItr);
    final SortedMap<Interval, Pair<String, List<DataSegment>>> taskInputSegments = new TreeMap<>(Comparators.intervalsByStartThenEnd()
                                                                                                            .reversed());
    ArrayList<Interval> lockedIntervals = new ArrayList<>();
    for (CandidateGroup groupSortedToBuildVersion : candidateGroups) {
      try {
        if (spec.forceOverwrite() || groupSortedToBuildVersion.isOverwrite()) {
          if (runningTaskSets.size() >= maxTaskCount) {
            return;
          }
          // create overwrite task
          for (CandidateSegments baseIntervalChunk : groupSortedToBuildVersion.getCandidateBaseIntervals()) {
            Pair<String, List<DataSegment>> versionSegments = taskInputSegments.computeIfAbsent(
                baseIntervalChunk.getBaseInterval(),
                k -> new Pair<>(baseIntervalChunk.getVersion(), new ArrayList<>())
            );
            versionSegments.rhs.addAll(baseSegments.get(baseIntervalChunk.getBaseInterval()));
          }

          lockedIntervals.addAll(createMaterializedViewTask(new AtomicLong(), taskInputSegments, false,lockedIntervals));
        } else {
          final AtomicLong totalBatchSize = new AtomicLong();
          // create appending task
          for (CandidateSegments baseIntervalChunk : groupSortedToBuildVersion.getCandidateBaseIntervals()) {
            if (runningTaskSets.size() >= maxTaskCount) {
              return;
            }
            List<DataSegment> dataSegments = baseSegments.get(baseIntervalChunk.getBaseInterval());
            //确保baseInterval下segments按partitionNum有序提交物化
            Collections.sort(dataSegments);
            // compute task input segments
            for (DataSegment inputDataSegment : dataSegments) {
              if (runningTaskSets.size() >= maxTaskCount) {
                return;
              }

              //single interval segments end,if ds_mv segment granularity equal to ds_base segment granularity,then
              // must be combine to one task,
              // else if input segments combine to single complete ds_mv segment granularity,
              // also create task
              boolean isReached = spec.isReachMVSegmentGran(taskInputSegments,inputDataSegment);
              if(isReached){
                log.info("BaseDataSource[%s] segments[%s] reached mv granularity[%s],submit mv task!",
                         spec.getBaseDataSource(),
                         taskInputSegments.firstKey(), spec.granularitySpec.getSegmentGranularity());
                lockedIntervals.addAll(createMaterializedViewTask(totalBatchSize, taskInputSegments, true,lockedIntervals));
              }

              totalBatchSize.addAndGet(inputDataSegment.getSize());
              Pair<String, List<DataSegment>> versionSegments = taskInputSegments.computeIfAbsent(
                  baseIntervalChunk.getBaseInterval(),
                  k -> new Pair<>(baseIntervalChunk.getVersion(), new ArrayList<>())
              );

              if (totalBatchSize.get() > inputMaxSizeForAppendingTask) {
                if (taskInputSegments.size() <= 1 && versionSegments.rhs.isEmpty()) {
                  versionSegments.rhs.add(inputDataSegment);
                  // 提交taskInputSegments，并clear
                  lockedIntervals.addAll(createMaterializedViewTask(totalBatchSize, taskInputSegments, true,lockedIntervals));
                } else {
                  // 提交taskInputSegments，并clear
                  lockedIntervals.addAll(createMaterializedViewTask(totalBatchSize, taskInputSegments, true,lockedIntervals));

                  //new batch
                  versionSegments = taskInputSegments.computeIfAbsent(
                      baseIntervalChunk.getBaseInterval(),
                      k -> new Pair<>(baseIntervalChunk.getVersion(), new ArrayList<>())
                  );
                  versionSegments.rhs.add(inputDataSegment);
                  //重新计算
                  totalBatchSize.set(inputDataSegment.getSize());
                }
              } else {
                versionSegments.rhs.add(inputDataSegment);
              }
            }

          }
        }
      }
      catch (Exception e) {
        // throw new RuntimeException(e);
        log.error(e, "Exception task candidate segments group:%s", groupSortedToBuildVersion);
      }
    }
    // if singleBatchSegments not reached MIN_TASK_INPUT_SIZE,
    // but ending iterate materialized_view interval, still create task
    log.info("Submit materialized candidate input intervals[%s].",
             taskInputSegments.size());
    if (runningTaskSets.size() < maxTaskCount && taskInputSegments.size() > 0) {
      lockedIntervals.addAll(createMaterializedViewTask(new AtomicLong(), taskInputSegments, true,lockedIntervals));
    }
  }

  private List<Interval> createMaterializedViewTask(
      AtomicLong totalBatchSize,
      Map<Interval, Pair<String, List<DataSegment>>> candidateTaskInputSegments,
      boolean appendingToExists,
      List<Interval> lockedIntervals
  )
  {
    Map<Interval, Pair<String, List<DataSegment>>> taskInputSegments=
        filterLockedTaskInputSegments(candidateTaskInputSegments,lockedIntervals);
    log.info("Before segments:%s, find unlocked segments:%s",candidateTaskInputSegments.size(),taskInputSegments.size());
    if (taskInputSegments.isEmpty()) {
      return Collections.emptyList();
    }
    Task task = spec.createTask(
        taskInputSegments.values().stream()
                         .flatMap(list -> Objects.requireNonNull(list.rhs).stream())
                         .collect(Collectors.toList()),
        appendingToExists
    );
    log.info(
        "Submit appending[%s] task[%s] candidate inputIntervals[%s].",
        appendingToExists,
        task.getId(),
        taskInputSegments.size()
    );
    if (taskMaster.getTaskQueue().isPresent()) {
      taskMaster.getTaskQueue().get().add(task);
      runningTaskSets.add(task);

      for (Map.Entry<Interval, Pair<String, List<DataSegment>>> entry : taskInputSegments.entrySet()) {
        runningVersion.put(entry.getKey(), entry.getValue().lhs);
        runningTasks.put(entry.getKey(), task);
        cacheIntervalTaskStartTimes.put(entry.getKey(), new AtomicLong(System.currentTimeMillis()));
      }
      // clear input segments
      taskInputSegments.clear();
      totalBatchSize.set(0);
    } else {
      throw new IAE("TaskQueue is not present!");
    }
    return new ArrayList<>(taskInputSegments.keySet());
  }

  private Map<Interval, Pair<String, List<DataSegment>>> filterLockedTaskInputSegments(Map<Interval, Pair<String,
      List<DataSegment>>> taskInputSegments,List<Interval> beforeLockedIntervals) {
    Map<String,Integer> mvPriorityMap= new HashMap<>();
    mvPriorityMap.put(spec.getBaseDataSource(), Tasks.DEFAULT_BATCH_INDEX_TASK_PRIORITY);

    Map<String, List<Interval>> lockedIntervals = this.taskMaster.getLockedIntervals(mvPriorityMap);
    Map<String, List<Interval>> beforeLockedIntervalMap = new HashMap<>();
    beforeLockedIntervalMap.put(spec.getBaseDataSource(),beforeLockedIntervals);
    lockedIntervals.putAll(beforeLockedIntervalMap);

    Map<Interval, Pair<String, List<DataSegment>>> filteredTaskInputSegments = new HashMap<>();
    for (Map.Entry<Interval, Pair<String, List<DataSegment>>> entry : taskInputSegments.entrySet()) {
      if (!lockedIntervals.containsKey(spec.getBaseDataSource())) {
        filteredTaskInputSegments.put(entry.getKey(), entry.getValue());
      }else {
        List<Interval> intervals = lockedIntervals.get(spec.getBaseDataSource());
        boolean isLock=false;
        for(Interval interval : intervals) {
          if (entry.getKey().overlaps(interval)) {
            isLock=true;
            break;
          }
        }
        if (!isLock) {
          filteredTaskInputSegments.put(entry.getKey(), entry.getValue());
        }
      }
    }
    return filteredTaskInputSegments;
  }

  private List<CandidateGroup> groupIntervalBySegmentGranularity(
      ScoreMaterializedViewIterator intervalItr
  )
  {
    Map<Interval, CandidateGroup> sortedIntvalGroups = new HashMap<>();
    while (intervalItr.hasNext()) {
      CandidateSegments candidateIntervalSegs = intervalItr.next();
      Interval mvInterval = getMVInterval(candidateIntervalSegs.getBaseInterval());
      CandidateGroup candidateGroup = sortedIntvalGroups.computeIfAbsent(
          mvInterval,
          k -> new CandidateGroup(
              mvInterval,
              0,
              false,
              new ArrayList<>()
          )
      );
      candidateGroup.setScore(candidateGroup.getScore() + candidateIntervalSegs.getScore());
      candidateGroup.setOverwrite(candidateGroup.isOverwrite() || candidateIntervalSegs.isOverwrite());
      candidateGroup.getCandidateBaseIntervals().add(candidateIntervalSegs);
    }
    // sort all mvInterval
    List<CandidateGroup> sortedGroups = new ArrayList<>(sortedIntvalGroups.values());
    sortedGroups.sort((g1, g2) -> Integer.compare(g2.getScore(), g1.getScore()));
    // sort intervals in per group
    sortedGroups.forEach(g -> g.getCandidateBaseIntervals()
                               .sort((i1, i2) -> Integer.compare(i2.getScore(), i1.getScore())));
    return sortedGroups;
  }

  private Pair<Map<Interval, String>, Map<Interval, List<DataSegment>>> getVersionAndBaseSegments(
      Collection<DataSegment> snapshot,
      Map<Interval, Pair<Boolean, String>> toBuildHistoryMvInterval,
      Map<Interval, Pair<Boolean, String>> toBuildBaseIntervalFromMvInterval,
      Granularity segmentGranularity
  )
  {
    if (snapshot.size() > 0) {
      Interval maxAllowedToBuildInterval = snapshot.parallelStream()
                                                   .map(DataSegment::getInterval)
                                                   .max(Comparators.intervalsByStartThenEnd())
                                                   .get();
      // compute ingestion time range
      updateMVTaskIngestionTimeRange(maxAllowedToBuildInterval, segmentGranularity);
    }

    Map<Interval, String> versions = new HashMap<>();
    Map<Interval, List<DataSegment>> segments = new HashMap<>();
    for (DataSegment segment : snapshot) {
      Interval interval = segment.getInterval();
      // skip recent interval to avoid materializedview too frequency
      // skip old interval that not included ingestion time period.
      if (!isIncludedIngestionPeriod(interval)) {
        //忽略没有完全属于物化区间内的mvInterval
        toBuildHistoryMvInterval.remove(getMVInterval(interval));
        continue;
      }
      //兼容老版本：没有物化标识的segment需要通过覆盖模式重新提交
      for (Interval mvInterval : toBuildHistoryMvInterval.keySet()) {
        if (mvInterval.contains(segment.getInterval())) {
          toBuildBaseIntervalFromMvInterval.put(segment.getInterval(), new Pair<>(true, segment.getVersion()));
          break;
        }
      }

      versions.put(
          interval,
          segment.getVersion()
      );
      segments.computeIfAbsent(interval, i -> new ArrayList<>()).add(segment);
    }

    return new Pair<>(versions, segments);
  }

  public static DateTime getNow()
  {
    return new DateTime(DateTimeUtils.currentTimeMillis() + 8 * 3600L * 1000L, DateTimeZone.UTC);
  }

  private void updateMVTaskIngestionTimeRange(
      Interval maxAllowedToBuildInterval,
      Granularity segmentGranularity
  )
  {
    LocalDateTime maxDate = new LocalDateTime(
        Math.min(maxAllowedToBuildInterval.getEndMillis(), getNow().getMillis()),
        DateTimeZone.UTC
    );
    maxIngestionEndTimeMs.set(maxDate.minus(skipPeriodFromLatest).toDateTime(DateTimeZone.UTC).getMillis());
    minIngestionStartTimeMs.set(maxDate.minus(skipPeriodFromLatest)
                                       .minus(ingestionTimeRange)
                                       .toDateTime(DateTimeZone.UTC)
                                       .getMillis());
    //第二段物化区间end时间
    secondIngestionEndTimeMs.set(maxDate.minus(skipPeriodFromLatest)
                                        .minus(policyConfig.getFirstPeriodFromLatest())
                                        .toDateTime(DateTimeZone.UTC)
                                        .getMillis());

    //减少大粒度物化视图全量物化模式下的提交频次：按物化视图粒度提交
    if (config.isEnableTruncateIngestionTime()) {
      minIngestionStartTimeMs.set(segmentGranularity.bucketStart(DateTimes.utc(minIngestionStartTimeMs.get()))
                                                    .getMillis());
      long truncateEnd = segmentGranularity.bucketEnd(DateTimes.utc(maxIngestionEndTimeMs.get()))
                                           .getMillis();
      if (segmentGranularity.bucketStart(DateTimes.utc(maxIngestionEndTimeMs.get()))
                            .getMillis() == maxIngestionEndTimeMs.get()) {
        truncateEnd = maxIngestionEndTimeMs.get();
      }
      long truncateToday = segmentGranularity.bucketStart(getNow())
                                             .getMillis();
      maxIngestionEndTimeMs.set(Math.min(truncateEnd, truncateToday));
      //第二段物化区间end时间
      if (truncateEnd < truncateToday) {
        secondIngestionEndTimeMs.set(segmentGranularity.bucketStart(DateTimes.utc(maxIngestionEndTimeMs.get()))
                                                       .minus(policyConfig.getFirstPeriodFromLatest())
                                                       .toDateTime(DateTimeZone.UTC)
                                                       .getMillis());
      } else {
        secondIngestionEndTimeMs.set(segmentGranularity.bucketStart(getNow())
                                                       .minus(policyConfig.getFirstPeriodFromLatest())
                                                       .toDateTime(DateTimeZone.UTC)
                                                       .getMillis());
      }
      log.info(
          "物化区间[%s, %s], 全量物化区间[%s, %s], 增量物化区间[%s, %s]",
          new DateTime(minIngestionStartTimeMs.get(), DateTimeZone.UTC),
          new DateTime(maxIngestionEndTimeMs.get(), DateTimeZone.UTC),
          new DateTime(minIngestionStartTimeMs.get(), DateTimeZone.UTC),
          new DateTime(secondIngestionEndTimeMs.get(), DateTimeZone.UTC),
          new DateTime(secondIngestionEndTimeMs.get(), DateTimeZone.UTC),
          new DateTime(maxIngestionEndTimeMs.get(), DateTimeZone.UTC)
      );
    } else {
      log.info(
          "Compute ingestion time range[%s,%s], second ingestion end time[%s]",
          new DateTime(minIngestionStartTimeMs.get(), DateTimeZone.UTC),
          new DateTime(maxIngestionEndTimeMs.get(), DateTimeZone.UTC),
          new DateTime(secondIngestionEndTimeMs.get(), DateTimeZone.UTC)
      );
    }

  }

  /**
   * recovery materialized DataSegment{interval,version,materializedSegment}
   * from MaterializedView DataSegment's MaterializedSegment
   */
  @VisibleForTesting
  Pair<Map<Interval, String>, Map<Interval, List<DataSegment>>> getMaterializedVersionAndBaseSegments(
      Collection<DataSegment> materializedSnapshot,
      Map<Interval, Pair<Boolean, String>> toBuildMvInterval
  )
  {
    Map<Interval, String> versions = new HashMap<>();
    Map<Interval, List<DataSegment>> segments = new HashMap<>();
    for (DataSegment segment : materializedSnapshot) {
      Interval interval = segment.getInterval();
      MaterializedSpec materializedSpec = segment.getMaterializedSpec();
      if (materializedSpec == null) {
        //升级时兼容老版本离线数据源，历史DataSegment由于没有物化标识，不能实现增量物化区分，故重新全量物化。
        toBuildMvInterval.put(segment.getInterval(), new Pair<>(true, segment.getVersion()));
      } else if (materializedSpec.getBaseShardSpecsSpec() != null) {
        // materializedView dataSource segment granularity is equal to baseDatasource segment granularity
        versions.put(
            interval,
            materializedSpec.getBaseShardSpecsSpec().getVersion()
        );
        segments.computeIfAbsent(interval, i -> new ArrayList<>()).add(new MaterializedDataSegment(
            segment.getDataSource(),
            segment.getInterval(),
            materializedSpec.getBaseShardSpecsSpec().getVersion(),
            materializedSpec.getBaseShardSpecsSpec(),
            segment.getSize()
        ));
      } else {
        // materializedView dataSource segment granularity is greater than baseDatasource segment granularity
        Map<Short, BaseShardSpecsSpec> multiBaseShardSpecsSpec = materializedSpec.getSourceBaseShardSpecsSpecs();
        for (Map.Entry<Short, BaseShardSpecsSpec> entry : multiBaseShardSpecsSpec.entrySet()) {
          Pair<Interval, String> recoveryIntervalVerion = MaterializedViewUtils.getSrcIntervalByIntervalId(
              entry.getKey(),
              entry.getValue(),
              materializedSpec.getMapBuckets(),
              interval
          );
          versions.put(
              recoveryIntervalVerion.lhs,
              recoveryIntervalVerion.rhs
          );
          segments.computeIfAbsent(recoveryIntervalVerion.lhs, i -> new ArrayList<>())
                  .add(new MaterializedDataSegment(
                      segment.getDataSource(),
                      recoveryIntervalVerion.lhs,
                      recoveryIntervalVerion.rhs,
                      entry.getValue(),
                      segment.getSize() / multiBaseShardSpecsSpec.size() + 1
                  ));
        }
      }

    }
    return new Pair<>(versions, segments);
  }

  /**
   * check whether the target interval be included in ingestion time range.
   *
   * @param target
   * @return true if the target interval be included in ingestion time range.
   */
  private boolean isIncludedIngestionPeriod(Interval target)
  {
    return maxIngestionEndTimeMs.get() >= target.getEndMillis()
           && minIngestionStartTimeMs.get() <= target.getStartMillis();
  }

  private boolean isIncludedHadoopIngestionPeriod(Interval target)
  {
    return maxIngestionEndTimeMsForOverwriteHadoop.getMillis() >= target.getEndMillis()
           && minIngestionStartTimeMsForOverwriteHadoop.getMillis() <= target.getStartMillis();
  }

  /**
   * 判断interval是否落入第二段物化区间
   *
   * @param target
   * @return
   */
  private boolean isIncludedSecondPeriod(Interval target)
  {
    return secondIngestionEndTimeMs.get()
           >= target.getEndMillis()
           && minIngestionStartTimeMs.get() <= target.getStartMillis();
  }

  private void clearTasks()
  {
    for (Task task : runningTasks.values()) {
      if (taskMaster.getTaskQueue().isPresent()) {
        taskMaster.getTaskQueue().get().shutdown(task.getId(), "killing all tasks");
      }
    }
    runningTasks.clear();
    runningVersion.clear();
    cacheIntervalTaskStartTimes.clear();
  }

  private void clearSegments()
  {
    log.info("Clear all metadata of dataSource %s", dataSource);
    metadataStorageCoordinator.deletePendingSegments(dataSource);
    sqlSegmentsMetadataManager.markAsUnusedAllSegmentsInDataSource(dataSource);
    metadataStorageCoordinator.deleteDataSourceMetadata(dataSource);
  }

  private void commitDataSourceMetadata(DataSourceMetadata dataSourceMetadata)
  {
    if (!metadataStorageCoordinator.insertDataSourceMetadata(dataSource, dataSourceMetadata)) {
      try {
        metadataStorageCoordinator.resetDataSourceMetadata(
            dataSource,
            dataSourceMetadata
        );
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
