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
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.apache.druid.common.guava.SettableSupplier;
import org.apache.druid.guice.DruidGuiceExtensions;
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.Query;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.expression.TimestampFloorExprMacro;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.timeline.BaseShardSpecsSpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.MaterializedDataSegment;
import org.apache.druid.timeline.MaterializedSpec;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class MaterializedViewUtils
{
  public static final ExprMacroTable INSTANCE;

  static {
    final Injector injector = Guice.createInjector(
        new DruidGuiceExtensions(),
        binder -> binder.bind(Key.get(ObjectMapper.class, Json.class)).toInstance(new DefaultObjectMapper()),
        new ExpressionModule()
    );

    INSTANCE = injector.getInstance(ExprMacroTable.class);
  }
  /**
   * extract all dimensions in query.
   * only support TopNQuery/TimeseriesQuery/GroupByQuery
   *
   * @param query
   * @return dimensions set in query
   */
  public static Pair<Granularity,Set<String>> getRequiredFields(Query query)
  {
    Set<String> dimsInFilter = null == query.getFilter() ? new HashSet<>() : query.getFilter().getRequiredColumns();
    Set<String> dimensions = new HashSet<>(dimsInFilter);
    Granularity granularity=null;
    if (query instanceof TopNQuery) {
      TopNQuery q = (TopNQuery) query;
      dimensions.addAll(extractFieldsFromAggregations(q.getAggregatorSpecs()));
      dimensions.add(q.getDimensionSpec().getDimension());
    } else if (query instanceof TimeseriesQuery) {
      TimeseriesQuery q = (TimeseriesQuery) query;
      dimensions.addAll(extractFieldsFromAggregations(q.getAggregatorSpecs()));
    } else if (query instanceof GroupByQuery) {
      GroupByQuery q = (GroupByQuery) query;
      dimensions.addAll(extractFieldsFromAggregations(q.getAggregatorSpecs()));
      VirtualColumns virtualColumns = q.getVirtualColumns();

      for (DimensionSpec spec : q.getDimensions()) {
        String dim = spec.getDimension();
        if(virtualColumns != null && virtualColumns.getVirtualColumn(dim)!=null){
          VirtualColumn virtualColumn = virtualColumns.getVirtualColumn(dim);
          List<String> cols = virtualColumn.requiredColumns();
          if(virtualColumn instanceof ExpressionVirtualColumn){
            String expression = ((ExpressionVirtualColumn) virtualColumn).getExpression();
            Expr parsedExpr = Parser.parse(expression, INSTANCE);
            //extract real granularity
            if(parsedExpr instanceof TimestampFloorExprMacro.TimestampFloorExpr){
              granularity = ((TimestampFloorExprMacro.TimestampFloorExpr) parsedExpr).getGranularity();
            }
//            Expr.BindingAnalysis analysis = parsedExpr.analyzeInputs();
//            Set<String> dims = analysis.getRequiredBindings();
          }
          dimensions.addAll(cols);
        }else{
          dimensions.add(dim);
        }
      }
    } else {
      throw new UnsupportedOperationException("Method getRequiredFields only supports TopNQuery/TimeseriesQuery/GroupByQuery");
    }
    return Pair.of(granularity,dimensions.stream().filter(d->!d.equals("__time")).collect(Collectors.toSet()));
  }

  private static Set<String> extractFieldsFromAggregations(List<AggregatorFactory> aggs)
  {
    Set<String> ret = new HashSet<>();
    for (AggregatorFactory agg : aggs) {
      if (agg instanceof FilteredAggregatorFactory) {
        FilteredAggregatorFactory fagg = (FilteredAggregatorFactory) agg;
        ret.addAll(fagg.getFilter().getRequiredColumns());
      }
      ret.addAll(agg.requiredFields());
    }
    return ret;
  }

  /**
   * calculate the intervals which are covered by interval2, but not covered by interval1.
   * result：针对相同粒度interval下segments做差集操作。
   * intervals = interval2 - interval1 ∩ interval2，
   *
   * @param interval2 list of intervals
   * @param interval1 list of intervals
   * @return list of intervals are covered by interval2, but not covered by interval1.
   */
  public static Map<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>> minusMV(
      Map<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>> interval2,
      Map<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>> interval1
  )
  {
    if (interval1.isEmpty() || interval2.isEmpty()) {
      return interval2;
    }
    //此处没有检查version，由timeline保证还原segment的覆盖逻辑
    MapDifference<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>> difference = Maps.difference(
        interval2,
        interval1
    );
    //当前物化视图数据源和基于此物化视图的物化视图比较：选出当前物化视图独有的interval
    Map<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>> particularIntervals = difference.entriesOnlyOnLeft();
    Map<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>> remainingIntervals = new HashMap<>(
        particularIntervals);
    //相同interval粒度下segment的差集
    Map<Interval, MapDifference.ValueDifference<Pair<SettableSupplier<Boolean>, List<DataSegment>>>> intervalValueDifferenceMap = difference
        .entriesDiffering();
    //找出当前物化视图数据源独有segment
    for (Map.Entry<Interval, MapDifference.ValueDifference<Pair<SettableSupplier<Boolean>, List<DataSegment>>>> entry : intervalValueDifferenceMap
        .entrySet()) {
      MapDifference.ValueDifference<Pair<SettableSupplier<Boolean>, List<DataSegment>>> diffSegs = entry.getValue();
      List<DataSegment> currDataSegments = diffSegs.leftValue().rhs;
      Pair<Integer, Integer> materializedRange = getMaterializedRange(diffSegs.rightValue().rhs);
      for (DataSegment ds : currDataSegments) {
        //判断当前数据源的segment是否被上层物化视图物化过
        if (!isMaterialized(ds.getId().getPartitionNum(), materializedRange.lhs, materializedRange.rhs)) {
          Pair<SettableSupplier<Boolean>, List<DataSegment>> settableSupplierListPair = remainingIntervals.computeIfAbsent(
              entry.getKey(),
              k -> new Pair<>(new SettableSupplier<>(false), new
                  ArrayList<>())
          );
          settableSupplierListPair.lhs.set(settableSupplierListPair.lhs.get() || diffSegs.leftValue().lhs.get());
          settableSupplierListPair.rhs.add(ds);
        }
      }
    }
    return remainingIntervals;
  }

  private static Pair<Integer, Integer> getMaterializedRange(List<DataSegment> materializedDataSegments)
  {
    int startPartitionNumber = Integer.MAX_VALUE;
    int endPartitionNumber = Integer.MIN_VALUE;
    for (DataSegment md : materializedDataSegments) {
      MaterializedSpec materializedSpec = md.getMaterializedSpec();
      if (materializedSpec.getBaseShardSpecsSpec() == null) {
        throw new IAE("WTF? materialized spec can not be null!");
      }
      startPartitionNumber = Math.min(
          startPartitionNumber,
          materializedSpec.getBaseShardSpecsSpec().getStartPartitionNumber()
      );
      endPartitionNumber = Math.max(
          endPartitionNumber,
          materializedSpec.getBaseShardSpecsSpec().getEndPartitionNumber()
      );
    }
    return new Pair<>(startPartitionNumber, endPartitionNumber);
  }

  private static boolean isMaterialized(int chunkNumber, int startPartitionNumber, int endPartitionNumber)
  {
    return chunkNumber < endPartitionNumber && chunkNumber >= startPartitionNumber;
  }

  /**
   * 根据intervalId值还原出实际的interval.
   *
   * @param intervalId         记录baseDatasource的分区在物化视图分区的位置编号，以代替存储原始interval,节省存储空间。
   * @param baseShardSpecsSpec 当物化视图分区粒度和baseDatasource分区粒度一致时，记录对应被物化的baseDatasource分区内的segment列表。
   * @param mapBuckets         记录物化视图分区粒度/baseDatasource分区粒度的倍数，比如:day/hour=24
   * @param mvInterval         物化视图的分区粒度interval
   * @return 将被物化数据源的分区intervalId还原成实际interval
   */
  public static Pair<Interval, String> getSrcIntervalByIntervalId(
      short intervalId, // start from 0
      BaseShardSpecsSpec baseShardSpecsSpec,
      short mapBuckets,
      Interval mvInterval
  )
  {
    long bucketRange = mvInterval.toDurationMillis() / mapBuckets;
    return new Pair<>(
        Intervals.utc(
            mvInterval.getStartMillis() + intervalId * bucketRange,
            mvInterval.getStartMillis() + (intervalId + 1) * bucketRange
        ),
        baseShardSpecsSpec.getVersion()
    );
  }

  /**
   * 将mvSegment中记录的被物化的segment进行还原，但原始数据源的一个interval只还原对应一个MaterializedDataSegment，
   * 原始interval下若存在多个segment信息，则通过MaterializedSpec记录被物化的segment的partitionNumber的范围
   * 注意：还原的segmentId需要确保唯一性，即在对同baseDataSource一个interval下增量物化segment时，还原出的segmentId要确保能准确代表每次增量物化的那些segment
   * 在对interval的多次全量物化时，也是同理;另外还原的多个segment，需要确保不能彼此覆盖。
   * 故还原出的segmentId组成中：partitionNumber取被物化segments的endPartitionNumbers,version为baseDataSource中对应interval的version，
   * 还原的segment的shardSpec采用增量分区以避免彼此覆盖。
   *
   * @param mvSegment 物化视图segment
   * @return 还原出被物化的原始数据源segment列表
   */
  public static List<MaterializedDataSegment> recoveryBaseDataSegments(
      DerivativeDataSourceManager derivativeDatasourceMeta,
      DataSegment mvSegment
  )
  {
    List<MaterializedDataSegment> materializedDataSegments = new ArrayList<>();
    String baseDataSource = derivativeDatasourceMeta.getDirectBaseDataSource(mvSegment.getDataSource());
    if (baseDataSource == null) {
      return materializedDataSegments;
    }
    MaterializedSpec mSegment = mvSegment.getMaterializedSpec();
    switch (mSegment.getType()) {
      case MaterializedSpec.TYPE_SAME_SEGMENT_GRAN:
        materializedDataSegments.add(new MaterializedDataSegment(
            baseDataSource,
            mvSegment.getInterval(),
            mvSegment.getVersion(),
            mSegment.getBaseShardSpecsSpec(),
            mvSegment.getSize()
        ));
        break;
      case MaterializedSpec.TYPE_DIFF_SEGMENT_GRAN:
        Map<Short, BaseShardSpecsSpec> multiMaterializedSpec = mSegment.getSourceBaseShardSpecsSpecs();
        for (Map.Entry<Short, BaseShardSpecsSpec> entry : multiMaterializedSpec.entrySet()) {
          Pair<Interval, String> recoveryIntervalVerion = MaterializedViewUtils.getSrcIntervalByIntervalId(
              entry.getKey(),
              entry.getValue(),
              mSegment.getMapBuckets(),
              mvSegment.getInterval()
          );
          materializedDataSegments.add(new MaterializedDataSegment(
              baseDataSource,
              recoveryIntervalVerion.lhs,
              mvSegment.getVersion(),
              entry.getValue(),
              mvSegment.getSize() / multiMaterializedSpec.size() + 1
          ));
        }
        break;
      default:
    }
    return materializedDataSegments;
  }

  /**
   * remainingQueryIntervals为最原始数据源，从此集合中过滤掉对应derivativeSegments中存在交集的历史interval
   *
   * @param remainingQueryIntervals
   * @param derivativeSegments
   * @return
   */
  public static Map<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>> minusHistoryInterval(
      Map<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>> remainingQueryIntervals,
      Map<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>> derivativeSegments
  )
  {
    List<Interval> historyIntevers = new ArrayList<>();
    //hadoop历史数据兼容：物化视图interval下不存在物化标识的segment则为历史数据（原因：针对历史数据物化使用全量覆盖）
    for (Map.Entry<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>> entry : derivativeSegments.entrySet()) {
      if (!entry.getValue().lhs.get()) {
        //非物化任务产生，则为历史hadoop产生的interval,不在继续下推，而是baseDataSource的intervals下推集合进行补全
        historyIntevers.add(entry.getKey());
      }
    }
    if (historyIntevers.size() == 0) {
      return remainingQueryIntervals;
    }

    //过滤掉物化视图历史interval所对应的最原始数据源的interval
    Map<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>> filteredRemainingQueryIntervals = new HashMap<>();
    for (Map.Entry<Interval, Pair<SettableSupplier<Boolean>, List<DataSegment>>> entry : remainingQueryIntervals.entrySet()) {
      boolean isHistory = false;
      for (Interval historyInterval : historyIntevers) {
        if (historyInterval.overlaps(entry.getKey())) {
          isHistory = true;
          break;
        }
      }
      if (!isHistory) {
        filteredRemainingQueryIntervals.put(entry.getKey(), entry.getValue());
      }
    }
    return filteredRemainingQueryIntervals;
  }

  /**
   * calculate the intervals which are covered by interval2, but not covered by interval1.
   * result intervals = interval2 - interval1 ∩ interval2
   * e.g.
   * a list of interval2: ["2018-04-01T00:00:00.000Z/2018-04-02T00:00:00.000Z",
   *                       "2018-04-03T00:00:00.000Z/2018-04-10T00:00:00.000Z"]
   * a list of interval1: ["2018-04-04T00:00:00.000Z/2018-04-06T00:00:00.000Z"]
   * the result list of intervals: ["2018-04-01T00:00:00.000Z/2018-04-02T00:00:00.000Z",
   *                                "2018-04-03T00:00:00.000Z/2018-04-04T00:00:00.000Z",
   *                                "2018-04-06T00:00:00.000Z/2018-04-10T00:00:00.000Z"]
   * If interval2 is empty, then return an empty list of interval.
   * @param interval2 list of intervals
   * @param interval1 list of intervals
   * @return list of intervals are covered by interval2, but not covered by interval1.
   */
  public static List<Interval> minus(List<Interval> interval2, List<Interval> interval1)
  {
    if (interval1.isEmpty() || interval2.isEmpty()) {
      return interval2;
    }
    Iterator<Interval> it1 = JodaUtils.condenseIntervals(interval1).iterator();
    Iterator<Interval> it2 = JodaUtils.condenseIntervals(interval2).iterator();
    List<Interval> remaining = new ArrayList<>();
    Interval currInterval1 = it1.next();
    Interval currInterval2 = it2.next();
    long start1 = currInterval1.getStartMillis();
    long end1 = currInterval1.getEndMillis();
    long start2 = currInterval2.getStartMillis();
    long end2 = currInterval2.getEndMillis();
    while (true) {
      if (start2 < start1 && end2 <= start1) {
        remaining.add(Intervals.utc(start2, end2));
        if (it2.hasNext()) {
          currInterval2 = it2.next();
          start2 = currInterval2.getStartMillis();
          end2 = currInterval2.getEndMillis();
        } else {
          break;
        }
      }
      if (start2 < start1 && end2 > start1 && end2 < end1) {
        remaining.add(Intervals.utc(start2, start1));
        start1 = end2;
        if (it2.hasNext()) {
          currInterval2 = it2.next();
          start2 = currInterval2.getStartMillis();
          end2 = currInterval2.getEndMillis();
        } else {
          break;
        }
      }
      if (start2 < start1 && end2 == end1) {
        remaining.add(Intervals.utc(start2, start1));
        if (it2.hasNext() && it1.hasNext()) {
          currInterval2 = it2.next();
          start2 = currInterval2.getStartMillis();
          end2 = currInterval2.getEndMillis();
          currInterval1 = it1.next();
          start1 = currInterval1.getStartMillis();
          end1 = currInterval1.getEndMillis();
        } else {
          break;
        }
      }
      if (start2 < start1 && end2 > end1) {
        remaining.add(Intervals.utc(start2, start1));
        start2 = end1;
        if (it1.hasNext()) {
          currInterval1 = it1.next();
          start1 = currInterval1.getStartMillis();
          end1 = currInterval1.getEndMillis();
        } else {
          remaining.add(Intervals.utc(end1, end2));
          break;
        }
      }
      if (start2 == start1 && end2 >= start1 && end2 < end1) {
        start1 = end2;
        if (it2.hasNext()) {
          currInterval2 = it2.next();
          start2 = currInterval2.getStartMillis();
          end2 = currInterval2.getEndMillis();
        } else {
          break;
        }
      }
      if (start2 == start1 && end2 > end1) {
        start2 = end1;
        if (it1.hasNext()) {
          currInterval1 = it1.next();
          start1 = currInterval1.getStartMillis();
          end1 = currInterval1.getEndMillis();
        } else {
          remaining.add(Intervals.utc(end1, end2));
          break;
        }
      }
      if (start2 > start1 && start2 < end1 && end2 < end1) {
        start1 = end2;
        if (it2.hasNext()) {
          currInterval2 = it2.next();
          start2 = currInterval2.getStartMillis();
          end2 = currInterval2.getEndMillis();
        } else {
          break;
        }
      }
      if (start2 > start1 && start2 < end1 && end2 > end1) {
        start2 = end1;
        if (it1.hasNext()) {
          currInterval1 = it1.next();
          start1 = currInterval1.getStartMillis();
          end1 = currInterval1.getEndMillis();
        } else {
          remaining.add(Intervals.utc(end1, end2));
          break;
        }
      }
      if (start2 >= start1 && start2 <= end1 && end2 == end1) {
        if (it2.hasNext() && it1.hasNext()) {
          currInterval2 = it2.next();
          start2 = currInterval2.getStartMillis();
          end2 = currInterval2.getEndMillis();
          currInterval1 = it1.next();
          start1 = currInterval1.getStartMillis();
          end1 = currInterval1.getEndMillis();
        } else {
          break;
        }
      }
      if (start2 >= end1 && end2 > end1) {
        if (it1.hasNext()) {
          currInterval1 = it1.next();
          start1 = currInterval1.getStartMillis();
          end1 = currInterval1.getEndMillis();
        } else {
          remaining.add(Intervals.utc(start2, end2));
          break;
        }
      }
    }

    while (it2.hasNext()) {
      remaining.add(Intervals.of(it2.next().toString()));
    }
    return remaining;
  }
}
