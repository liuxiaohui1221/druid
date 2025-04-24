package org.apache.druid.query.cache;

import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.filter.DimFilter;
import org.joda.time.Interval;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

public class SubQueryCacheKey extends CacheKey
{

  private String dataSource;
  private List<Interval> intervals;
  private DimFilter filter;      // 过滤条件（需序列化为哈希）
  private List<String> dimensions; // 排序后的维度列表
  private List<String> aggregators; // 聚合器字段名
  private Granularity granularity;

  public SubQueryCacheKey(
      String namespace,
      String dataSource,
      List<Interval> intervals,
      DimFilter filter,
      List<String> dimensions,
      List<String> aggregators,
      Granularity granularity
  )
  {
    super(namespace);
    this.dataSource = dataSource;
    this.intervals = intervals;
    this.filter = filter;
    this.dimensions = dimensions;
    this.aggregators = aggregators;
    this.granularity = granularity;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SubQueryCacheKey that = (SubQueryCacheKey) o;
    return Objects.equals(namespace, that.namespace)
           && Objects.equals(dataSource, that.dataSource)
           && Objects.equals(intervals, that.intervals)
           && Objects.equals(filter, that.filter)
           && Objects.equals(dimensions, that.dimensions)
           && Objects.equals(aggregators, that.aggregators)
           && Objects.equals(granularity, that.granularity);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(namespace,dataSource, intervals, filter, dimensions, aggregators, granularity);
  }

  @Override
  public int length() {
    int filterLength = filter == null ? 0 : filter.getCacheKey().length;
    int dimensionsLength = dimensions == null ? 0 : dimensions.size() * 24;
    int aggregatorsLength = aggregators == null ? 0 : aggregators.size() * 24;
    return filterLength + dimensionsLength + aggregatorsLength + 200;
  }

  public String getDataSource()
  {
    return dataSource;
  }

  public List<Interval> getIntervals()
  {
    return intervals;
  }

  public DimFilter getFilter()
  {
    return filter;
  }

  public List<String> getDimensions()
  {
    return dimensions;
  }

  public List<String> getAggregators()
  {
    return aggregators;
  }

  public Granularity getGranularity() { return granularity;}

  @Override
  public String toString()
  {
    return "CacheKey{" +
           "namespace='" + namespace + '\'' +
           ", dataSource='" + dataSource + '\'' +
           ", intervals=" + intervals +
           ", filter=" + filter +
           ", dimensions=" + dimensions +
           ", aggregators=" + aggregators +
           ", granularity=" + granularity +
           '}';
  }
}
