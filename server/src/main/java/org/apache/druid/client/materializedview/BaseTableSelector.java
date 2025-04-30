package org.apache.druid.client.materializedview;

import org.apache.druid.indexing.overlord.DerivativeDataSource;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class BaseTableSelector {
  public static String selectBestBase(
      Set<DerivativeDataSource> candidates,
      Set<String> requiredDims,
      Granularity requiredGranularity,
      Set<Interval> currentIntervals,
      String rootDataSource
                                      ) {
    // 筛选条件：维度包含、粒度更细、时间重叠
    List<DerivativeDataSource> filtered = candidates.stream()
                                                    .filter(view ->
                                                                requiredGranularity.isFinerThan(view.getGranularitySpec().getQueryGranularity()) &&
                                                                isCompatibleInterval(view.getIntervals(),
                                                                                     currentIntervals)
                                                                && isCompatibleDims(view.getDimensions(), requiredDims))
                                                    .collect(Collectors.toList());

    // 排序策略：维度数量升序 -> 粒度降序
    filtered.sort(Comparator
                      .comparingInt((DerivativeDataSource v) -> v.getDimensions().size())
            .thenComparing((DerivativeDataSource v1,DerivativeDataSource v2)  -> Granularity.IS_FINER_THAN.compare(v2.getGranularitySpec().getQueryGranularity(), v1.getGranularitySpec().getQueryGranularity())));
    return filtered.isEmpty() ?
           rootDataSource : // 回退到原始表
           filtered.get(0).getBaseDataSource();
  }

  private static boolean isCompatibleDims(Set<String> dimensions, Set<String> requiredDims) {
    return dimensions.containsAll(requiredDims);
  }

  private static boolean isCompatibleInterval(Set<Interval> intervals, Set<Interval> currentIntervals) {
    for (Interval currentInterval : currentIntervals) {
      boolean flag=false;
      for (Interval interval : intervals) {
        if (interval.contains(currentInterval)) {
          flag=true;
        }
      }
      if(!flag){
        return false;
      }
    }
    return true;
  }
}
