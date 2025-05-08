package org.apache.druid.client.materializedview;

import com.google.common.base.Splitter;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IntervalUtils {
  /**
   * 合并多个 Interval 字符串
   * 符合 Druid 的区间处理规范：
   * 1. 自动排序
   * 2. 合并重叠/相邻区间
   * 3. 返回 ISO8601 格式列表
   */
  public static Map<Interval,Integer> parseAndMerge(String intervals, String mergedLifetime) {
    List<Interval> parsed = Splitter.on(',')
                                   .trimResults()
                                   .splitToList(intervals)
                                   .stream()
                                   .map(Intervals::of)
                                   .collect(Collectors.toList());
    String[] lifeTimes = mergedLifetime.split(",");
    if(lifeTimes.length>1 && parsed.size() != lifeTimes.length){
      throw new IllegalArgumentException("intervals and lifetime size not match");
    }

    Map<Interval,Integer> merged = new HashMap<>();
    for(int i = 0; i < parsed.size(); i++){
      if(lifeTimes.length==1){
        merged.put(parsed.get(i),Integer.parseInt(lifeTimes[0]));
      }else{
        merged.put(parsed.get(i),Integer.parseInt(lifeTimes[i]));
      }
    }
    return merged;
  }

  public static List<Interval> mergeIntervals(List<Interval> intervals) {
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
        merged.set(merged.size() - 1, last.withEndMillis(current.getEndMillis()));
      } else {
        merged.add(current);
      }
    }
    return merged;
  }

  public static boolean isValidLifeTime(boolean skipCheck, String insertTime, int lifetime) {
    if(skipCheck){
      return true;
    }
    DateTime now = DateTimes.nowUtc();
    if(now.getDayOfMonth()==DateTimes.of(insertTime).getDayOfMonth()){
      return now.getHourOfDay() < lifetime;
    }
    return false;
  }

  public static Map<Interval,Integer> getValidIntervals(boolean skipCheck, Map<Interval,Integer> intervals) {
    Map<Interval,Integer> validIntervals = new HashMap<>();
    for(Map.Entry<Interval,Integer> entry : intervals.entrySet()){
      if(isValidLifeTime(skipCheck, DateTimes.nowUtc().toString(),entry.getValue())){
        validIntervals.put(entry.getKey(),entry.getValue());
      }
    }
    return validIntervals;
  }
}
