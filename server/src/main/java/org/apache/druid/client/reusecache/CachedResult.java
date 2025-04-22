package org.apache.druid.client.reusecache;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.joda.time.Interval;

public class CachedResult implements Serializable
{
  // 原始查询的维度列表（如 ["dim1", "dim2"]）
  private final List<String> originalDimensions;

  // 聚合器名称列表（如 ["sum_metric", "count_events"]）
  private final List<String> aggregatorNames;

  // 按父维度分组的中间数据（Key为维度组合，Value为聚合器状态）
  private final Map<DimensionKey, AggregatedValue> groupedData;

  // 时间范围（用于过滤兼容性检查）
  private final Interval interval;

  public CachedResult(
      List<String> originalDimensions,
      List<String> aggregatorNames,
      Map<DimensionKey, AggregatedValue> groupedData,
      Interval interval
  ) {
    this.originalDimensions = originalDimensions;
    this.aggregatorNames = aggregatorNames;
    this.groupedData = groupedData;
    this.interval = interval;
  }

  // Getters...

  public List<String> getOriginalDimensions()
  {
    return originalDimensions;
  }

  public List<String> getAggregatorNames()
  {
    return aggregatorNames;
  }

  public Map<DimensionKey, AggregatedValue> getGroupedData()
  {
    return groupedData;
  }

  public Interval getInterval()
  {
    return interval;
  }
}
