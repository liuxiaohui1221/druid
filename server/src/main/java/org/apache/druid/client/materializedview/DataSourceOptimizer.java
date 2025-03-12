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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.client.selector.ServerSelector;
import org.apache.druid.common.guava.SettableSupplier;
import org.apache.druid.indexing.overlord.DerivativeDataSource;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.materializedview.MaterializedViewOptimizer;
import org.apache.druid.query.spec.MultipleSpecificSegmentSpec;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineLookup;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class DataSourceOptimizer implements MaterializedViewOptimizer
{
  private static final Logger log = new Logger(DataSourceOptimizer.class);
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private final TimelineServerView serverView;
  private final DerivativeDataSourceManager client;
  private final ConcurrentHashMap<String, AtomicLong> derivativesHitCount = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, AtomicLong> totalCount = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, AtomicLong> hitCount = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, AtomicLong> costTime = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, ConcurrentHashMap<Set<String>, AtomicLong>> missFields = new ConcurrentHashMap<>();

  @Inject
  public DataSourceOptimizer(TimelineServerView serverView, DerivativeDataSourceManager client)
  {
    this.serverView = serverView;
    this.client = client;
  }

  /**
   * Do main work about materialized view selection: transform user query to one or more sub-queries.
   * <p>
   * In the sub-query, the dataSource is the derivative of dataSource in user query, and sum of all sub-queries'
   * intervals equals the interval in user query
   * <p>
   * Derived dataSource with smallest average data size per segment granularity have highest priority to replace the
   * datasource in user query
   *
   * @param query only TopNQuery/TimeseriesQuery/GroupByQuery can be optimized
   * @return a list of queries with specified derived dataSources and intervals
   */
  @Override
  public List<Query> optimize(Query query)
  {
    log.info("MaterializedViewOptimizer optimize query start: %s", query);
    long start = System.currentTimeMillis();
    // only TableDataSource can be optimiezed
    if (!(query.getDataSource() instanceof TableDataSource)) {
      return Collections.singletonList(query);
    }
    String datasourceName = ((TableDataSource) query.getDataSource()).getName();
    // get all derivatives for datasource in query. The derivatives set is sorted by average size of
    // per segment granularity.
    ImmutableMap<String, DerivativeDataSource> subDerivatives = client.getSubDerivativeDataSources(datasourceName);
    if (subDerivatives.isEmpty()) {
      return Collections.singletonList(query);
    }
    String originBaseDataSource = client.getRootBaseDataSource(datasourceName);
    lock.readLock().lock();
    try {
      totalCount.computeIfAbsent(datasourceName, dsName -> new AtomicLong(0)).incrementAndGet();
      hitCount.putIfAbsent(datasourceName, new AtomicLong(0));
      costTime.computeIfAbsent(datasourceName, dsName -> new AtomicLong(0));

      //todo 物化视图自定义字段与查询字段匹配
      for (DerivativeDataSource derivativeDataSource : subDerivatives.values()) {
        derivativesHitCount.putIfAbsent(derivativeDataSource.getDataSource(), new AtomicLong(0));
      }
      DerivativeDataSource queryDerivativeDataSource = subDerivatives.get(datasourceName);
      query = unifyQueryGranularityIfNecessary(query, queryDerivativeDataSource);
      List<Query> queries = new ArrayList<>();
      List<Interval> queryIntervals = (List<Interval>) query.getIntervals();
      Map<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>> remainingQuerySegments = findSegments
          (queryIntervals, originBaseDataSource, false);

      Map<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>> lastMaterializedSegmentsByInterval = null;
      DerivativeDataSource lastDataSource = null;
      //累积每层物化的interval,用于最底层物化视图获取完整被物化的原始segment集合，并用于在最原始数据源中进行过滤
      Set<Interval> derivativeIntervals = new HashSet<>();

      //baseDatasource由于全量覆盖任务导致的version变化，之前记录的物化标识失效
      //记录每层物化视图version不一致的interval对应需要替换后的segments
      Map<String, Map<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>>> candidateDerivativeSegments = new HashMap<>();

      //子物化视图依次从粒度从大到小遍历，不存在多层，则只有当前这个物化视图。
      DerivativeDataSource curDerivativeDS = queryDerivativeDataSource;
      while (curDerivativeDS != null) {
        if (lastDataSource != null && !lastDataSource.getBaseDataSource()
                                                     .equals(curDerivativeDS.getDataSource())) {
          throw new IAE(
              "WTF? latest level baseDataSource[%s] do not equal to current dataSource[%s]",
              lastDataSource.getBaseDataSource(),
              curDerivativeDS.getDataSource()
          );
        }
        //物化分区interval -> Pair<标识物化分区是否是物化视图任务产生，物化分区segments>
        //interval级别过滤，直接通过对应的timeline过滤
        Map<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>> derivativeSegments = findSegments(
            remainingQuerySegments.keySet(),
            curDerivativeDS.getDataSource(),
            false
        );

        //记录当前层与上层不一致version的segments,同时删除interval对应所属上层的整个粒度interval
        if (lastMaterializedSegmentsByInterval != null) {
          lastMaterializedSegmentsByInterval = filterLastAndPushDownDiffIntervals(
              lastMaterializedSegmentsByInterval,
              lastDataSource.getDataSource(),
              derivativeSegments,
              candidateDerivativeSegments
          );
        }

        Set<Interval> missingIntervals = null;
        //记录中间层物化视图缺失的interval,此interval范围不会继续下推
        if (lastMaterializedSegmentsByInterval != null
            && lastMaterializedSegmentsByInterval.size() != derivativeSegments.size()) {
          // 上层记录的被物化的lastMaterializedSegmentsByInterval与当前实际存在的segment列表做对比，
          // 不考虑版本变化的情况下，当记录的segment对应基数据源数据并不存在时，一种情况是基数据源过期规则小于上层数据源导致不一致，一种情况是人为删除基数据源对应segment.
          // 当检测到记录存在，但实际不存在的interval，当做全部被物化，无需继续下推。
          missingIntervals = findMissingIntervals(
              lastMaterializedSegmentsByInterval.keySet(),
              derivativeSegments.keySet()
          );
          //过滤缺失interval(假定过期导致缺失，过期前均被完全物化)：物化记录存在，但实际不存在的interval
          remainingQuerySegments = filterOverlapMissingIntervals(remainingQuerySegments, missingIntervals);
        }

        // derivativeSegments可能包含当前层新产生的segment，或者历史interval，历史interval只查询当前层
        // 也即历史hadoop产生的interval直接过滤，不参与下推（即过滤最原始数据源对应interval）
        remainingQuerySegments = MaterializedViewUtils.minusHistoryInterval(
            remainingQuerySegments,
            derivativeSegments
        );

        //segment级别过滤得到当前层新增的segment，interval-->pair<是否为物化产生（false则为历史hadoop产生）,segments>
        Map<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>> newDerivativeSegments = filterLastMaterializedSegments(
            derivativeSegments,
            lastMaterializedSegmentsByInterval
        );
        //每层过滤缺失interval(假定过期导致缺失，过期前均被完全物化，缺失interval均不下推)：物化记录存在，但基数据源实际不存在的interval
        if (missingIntervals != null) {
          newDerivativeSegments = filterOverlapMissingIntervals(newDerivativeSegments, missingIntervals);
        }

        derivativeIntervals.addAll(newDerivativeSegments.keySet());

        //查询当前物化视图被物化的segment（通过物化标识还原），作为下一层物化视图segment查询过滤条件。
        lastMaterializedSegmentsByInterval = findSegments(
            derivativeIntervals,
            curDerivativeDS.getBaseDataSource(),
            true
        );

        lastDataSource = curDerivativeDS;

        //remainingQuerySegments中过滤掉当前物化视图已经物化的segments
        //针对多层物化，只在最下一层物化视图进行最原始数据源的segment过滤（要求：大粒度的物化视图数据生命周期大于小粒度的物化视图或实时数据源的生命周期）
        if (originBaseDataSource.equals(curDerivativeDS.getBaseDataSource())) {
          //相同粒度下做差集：过滤所有上层被物化过的segment
          remainingQuerySegments = MaterializedViewUtils.minusMV(
              remainingQuerySegments,
              lastMaterializedSegmentsByInterval
          );

        }
        if (!newDerivativeSegments.isEmpty()) {
          candidateDerivativeSegments.put(curDerivativeDS.getDataSource(), newDerivativeSegments);
          derivativesHitCount.get(curDerivativeDS.getDataSource()).incrementAndGet();
        }
        if (remainingQuerySegments.isEmpty()) {
          //最底层数据源没有剩余可下推的segment，无需继续下推，故跳出
          break;
        }
        // next layer
        curDerivativeDS = subDerivatives.get(curDerivativeDS.getBaseDataSource());
      }

      //物化视图查询
      if (!candidateDerivativeSegments.isEmpty()) {
        for (Map.Entry<String, Map<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>>> entry : candidateDerivativeSegments.entrySet()) {
          queries.add(
              query.withDataSource(new TableDataSource(entry.getKey()))
                   .withQuerySegmentSpec(new MultipleSpecificSegmentSpec(
                       getQuerySegmentDescriptors(entry.getValue()), queryIntervals)));
        }
      }
      //剩余的没有物化过的segmentId列表查询最原始数据源
      if (!remainingQuerySegments.isEmpty()) {
        queries.add(query.withDataSource(new TableDataSource(originBaseDataSource))
                         .withQuerySegmentSpec(new MultipleSpecificSegmentSpec(
                             getQuerySegmentDescriptors
                                 (remainingQuerySegments), queryIntervals)));
      }
      hitCount.get(datasourceName).incrementAndGet();
      costTime.get(datasourceName).addAndGet(System.currentTimeMillis() - start);
      log.info("Push down queries[%s] from query[%s]", queries, query);
      return queries;
    }
    finally {
      lock.readLock().unlock();
    }
  }

  /**
   * 由于对应记录baseDataSource的version发生变化，查询需要进一步过滤上层物化视图interval，并对interval进行下推。
   *
   * @param lastMaterializedSegmentsByInterval
   * @param derivativeSegments
   * @param candidateDerivativeSegments
   */
  private Map<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>> filterLastAndPushDownDiffIntervals(
      Map<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>> lastMaterializedSegmentsByInterval,
      String lastDataSource,
      Map<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>> derivativeSegments,
      Map<String, Map<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>>> candidateDerivativeSegments
  )
  {
    Map<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>> diffVersionWithRecorded = findDiffVersionWithRecorded(
        lastMaterializedSegmentsByInterval,
        derivativeSegments
    );
    //删除上层中对应interval粒度，包括删除对应记录的物化列表
    Map<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>> lastCandidateDerivativeSegments = candidateDerivativeSegments.get(
        lastDataSource);
    if (diffVersionWithRecorded.size() == 0 || lastCandidateDerivativeSegments == null) {
      return lastMaterializedSegmentsByInterval;
    }
    filterLastDerivativeIntervals(lastCandidateDerivativeSegments, diffVersionWithRecorded.keySet());
    //上层物化记录中删除diffVersionWithRecorded(当做上层没有物化过，本层仍需要查询)
    return MaterializedViewUtils.minusMV(lastMaterializedSegmentsByInterval, diffVersionWithRecorded);
  }

  private void filterLastDerivativeIntervals(
      Map<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>> lastCandidateDerivativeSegments,
      Set<Interval> candidateFilteredBaseIntervals
  )
  {
    Iterator<Map.Entry<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>>> iterator = lastCandidateDerivativeSegments.entrySet()
                                                                                                                                .iterator();
    while (iterator.hasNext()) {
      Map.Entry<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>> entry = iterator.next();
      if (existsContainsInterval(entry.getKey(), candidateFilteredBaseIntervals)) {
        iterator.remove();
      }
    }
  }

  private boolean existsContainsInterval(Interval interval, Set<Interval> candidateFilteredBaseIntervals)
  {
    for (Interval subInterval : candidateFilteredBaseIntervals) {
      if (interval.contains(subInterval)) {
        return true;
      }
    }
    return false;
  }

  /**
   * 查到记录的segment的版本与实际版本不一致的interval
   *
   * @param lastMaterializedSegmentsByInterval
   * @param derivativeSegments
   * @return
   */
  private Map<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>> findDiffVersionWithRecorded(
      Map<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>> lastMaterializedSegmentsByInterval,
      Map<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>> derivativeSegments
  )
  {
    Map<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>> newVersions = new HashMap<>();
    MapDifference<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>> difference = Maps.difference(
        lastMaterializedSegmentsByInterval,
        derivativeSegments
    );
    Map<Interval, MapDifference.ValueDifference<Pair<SettableSupplier<Boolean>, List<DataSegment>>>> intervalValueDifferenceMap = difference.entriesDiffering();
    for (Map.Entry<Interval, MapDifference.ValueDifference<Pair<SettableSupplier<Boolean>, List<DataSegment>>>> entry : intervalValueDifferenceMap.entrySet()) {
      List<DataSegment> recordSegments = entry.getValue().leftValue().rhs;
      List<DataSegment> actualSegments = entry.getValue().rightValue().rhs;
      // 记录物化的segment中，version属性并没有取实际原始version，取的是对应物化视图的以避免BrokerServerView中的增删冲突，
      // 原始segment的version记录在了baseShardSpecsSpec中
      if (recordSegments.size() > 0 && actualSegments.size() > 0 && !recordSegments.get(0)
                                                                                   .getMaterializedSpec()
                                                                                   .getBaseShardSpecsSpec()
                                                                                   .getVersion()
                                                                                   .equals(actualSegments.get(0)
                                                                                                         .getVersion())) {
        newVersions.put(entry.getKey(), entry.getValue().rightValue());
      }
    }
    return newVersions;
  }

  private Query unifyQueryGranularityIfNecessary(
      Query query,
      DerivativeDataSource queryDerivativeDataSource
  )
  {
    Granularity granularity = query.getGranularity();
    if (granularity != null && queryDerivativeDataSource.getGranularitySpec().getQueryGranularity() != null) {
      Comparator<Granularity> comparator = Comparators.granularityGreaterFirst();
      int compare = comparator.compare(
          queryDerivativeDataSource.getGranularitySpec().getQueryGranularity(),
          granularity
      );
      query = query.withOverriddenGranularity(compare < 0
                                              ? queryDerivativeDataSource.getGranularitySpec().getQueryGranularity()
                                              : granularity);
    }
    return query;
  }

  private Map<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>> filterOverlapMissingIntervals(
      Map<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>> remainingQuerySegments,
      Set<Interval> lastMissingDerivatives
  )
  {
    if (lastMissingDerivatives.size() == 0) {
      return remainingQuerySegments;
    }
    Map<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>> filteredRemainingQuerySegments = new HashMap<>();
    for (Map.Entry<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>> entry : remainingQuerySegments.entrySet()) {
      boolean isComplete = true;
      for (Interval missingInterval : lastMissingDerivatives) {
        if (entry.getKey().overlaps(missingInterval)) {
          isComplete = false;
        }
      }
      if (isComplete) {
        filteredRemainingQuerySegments.put(entry.getKey(), entry.getValue());
      }
    }
    return filteredRemainingQuerySegments;
  }

  private Set<Interval> findMissingIntervals(Set<Interval> completeIntervals, Set<Interval> actualIntervals)
  {
    return Sets.difference(completeIntervals, actualIntervals);
  }

  private List<SegmentDescriptor> getQuerySegmentDescriptors(Map<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>> derivativeSegments)
  {
    return derivativeSegments.values()
                             .stream()
                             .flatMap(newAndDataSegments -> newAndDataSegments.rhs.stream()
                                                                                  .map(d -> new SegmentDescriptor(
                                                                                      d.getInterval(),
                                                                                      d.getVersion(),
                                                                                      d.getShardSpec()
                                                                                       .getPartitionNum()
                                                                                  )))
                             .collect(Collectors.toList());
  }

  private Map<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>> findSegments(
      Collection<Interval> remainingQueryIntervals,
      String dataSource,
      boolean chooseMaterialized
  )
  {
    Optional<? extends TimelineLookup<String, ServerSelector>> timelineLookup = serverView
        .getTimeline(JoinDataSource.forDataSource(new TableDataSource(dataSource)), chooseMaterialized);
    if (!timelineLookup.isPresent()) {
      return new HashMap<>();
    }
    //通过小粒度interval列表remainingQueryIntervals匹配所属大粒度物化视图segment列表，故segment存在重复匹配，需要去重。
    Set<DataSegment> segments = remainingQueryIntervals.stream()
                                                       .flatMap(interval -> timelineLookup
                                                           .orElseThrow(() -> new ISE(
                                                               "No timeline for dataSource[%s]", dataSource
                                                           ))
                                                           .lookup(interval)
                                                           .stream()
                                                           .flatMap(
                                                               holder -> Lists.newArrayList(holder.getObject()
                                                                                                  .iterator())
                                                                              .stream()
                                                                              .map(p -> p.getObject().getSegment()))
                                                       )
                                                       .collect(Collectors.toSet());
    return SegmentUtils.groupDerivativeSegmentsByInterval(segments);
  }

  /**
   * 通过remainingQueryIntervals来过滤匹配查找对应datasourceName中候选的segment列表。其次通过lastRemainingSegments过滤并返回最终需要查询的segmentId列表。
   * 用途：比如基于小时粒度存在天粒度物化视图，而基于天粒度存在周粒度物化视图。
   * 调用此方法查询周粒度物化视图时，首先会根据remainingQueryIntervals查询周粒度物化视图数据，lastRemainingSegments初始为null，最后返回周粒度物化视图数据。
   * 查询完周粒度数据源后，再次开始调用此方法查询天粒度物化视图数据，并同时过滤上次周粒度物化视图已经查过的(lastRemainingSegments)，最后返回被过滤后的天粒度物化视图数据。
   *
   * @param candidateMVSegmentsByInterval 物化视图数据源过滤query interval后的列表。
   * @param lastRemainingSegments         上个查找到的物化视图的数据所对应的被物化的baseDatasource数据，
   *                                      查询的当前datasource即是上一个baseDatasource时，需要同时过滤此列表。
   * @return 返回最终需要查询dataSource的segmentId列表
   */
  private Map<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>> filterLastMaterializedSegments(
      Map<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>> candidateMVSegmentsByInterval,
      @Nullable Map<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>> lastRemainingSegments
  )
  {

    if (lastRemainingSegments == null) {
      return candidateMVSegmentsByInterval;
    }
    //再次过滤：segment级别，通过上层还原的lastRemainingSegments进行过滤
    //过滤逻辑：candidateMVSegmentsByInterval - lastRemainingIntervals
    return MaterializedViewUtils.minusMV(candidateMVSegmentsByInterval, lastRemainingSegments);
  }

  public List<DataSourceOptimizerStats> getAndResetStats()
  {
    ImmutableMap<String, AtomicLong> derivativesHitCountSnapshot;
    ImmutableMap<String, AtomicLong> totalCountSnapshot;
    ImmutableMap<String, AtomicLong> hitCountSnapshot;
    ImmutableMap<String, AtomicLong> costTimeSnapshot;
    ImmutableMap<String, ConcurrentHashMap<Set<String>, AtomicLong>> missFieldsSnapshot;
    lock.writeLock().lock();
    try {
      derivativesHitCountSnapshot = ImmutableMap.copyOf(derivativesHitCount);
      totalCountSnapshot = ImmutableMap.copyOf(totalCount);
      hitCountSnapshot = ImmutableMap.copyOf(hitCount);
      costTimeSnapshot = ImmutableMap.copyOf(costTime);
      missFieldsSnapshot = ImmutableMap.copyOf(missFields);
      derivativesHitCount.clear();
      totalCount.clear();
      hitCount.clear();
      costTime.clear();
      missFields.clear();
    }
    finally {
      lock.writeLock().unlock();
    }
    List<DataSourceOptimizerStats> stats = new ArrayList<>();
    ImmutableMap<String, HashMap<String, DerivativeDataSource>> baseToDerivatives =
        DerivativeDataSourceManager.getAllDerivatives();
    for (Map.Entry<String, HashMap<String, DerivativeDataSource>> entry : baseToDerivatives.entrySet()) {
      Map<String, Long> derivativesStat = new HashMap<>();
      Map<String, DerivativeDataSource> values = entry.getValue();
      for (DerivativeDataSource derivative : values.values()) {
        derivativesStat.put(
            derivative.getDataSource(),
            derivativesHitCountSnapshot.getOrDefault(derivative.getDataSource(), new AtomicLong(0)).get()
        );
      }
      stats.add(
          new DataSourceOptimizerStats(
              entry.getKey(),
              hitCountSnapshot.getOrDefault(entry.getKey(), new AtomicLong(0)).get(),
              totalCountSnapshot.getOrDefault(entry.getKey(), new AtomicLong(0)).get(),
              costTimeSnapshot.getOrDefault(entry.getKey(), new AtomicLong(0)).get(),
              missFieldsSnapshot.getOrDefault(entry.getKey(), new ConcurrentHashMap<>()),
              derivativesStat
          )
      );
    }
    return stats;
  }
}
