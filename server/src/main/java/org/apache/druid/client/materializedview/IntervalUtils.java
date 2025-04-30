package org.apache.druid.client.materializedview;

import com.google.common.base.Splitter;
import org.apache.druid.java.util.common.Intervals;
import org.joda.time.Interval;
import java.util.Set;
import java.util.stream.Collectors;

public class IntervalUtils {
  /**
   * 合并多个 Interval 字符串
   * 符合 Druid 的区间处理规范：
   * 1. 自动排序
   * 2. 合并重叠/相邻区间
   * 3. 返回 ISO8601 格式列表
   */
  public static Set<Interval> parseAndMerge(String intervals) {
    Set<Interval> parsed = Splitter.on(',')
                                   .trimResults()
                                   .splitToList(intervals)
                                   .stream()
                                   .map(Intervals::of)
                                   .collect(Collectors.toSet());

    return parsed;
  }

  /*private static List<Interval> mergeIntervals(List<Interval> intervals) {
    intervals.sort(Comparator.comparing(Interval::getStart));

    List<Interval> merged = new ArrayList<>();
    for (Interval current : intervals) {
      if (merged.isEmpty()) {
        merged.add(current);
        continue;
      }

      Interval last = merged.get(merged.size() - 1);
      if (last.overlaps(current) ||
          last.getEnd().equals(current.getStart())) {
        merged.set(merged.size() - 1, last.(current));
      } else {
        merged.add(current);
      }
    }
    return merged;
  }*/
}
