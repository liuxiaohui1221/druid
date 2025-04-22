package org.apache.druid.client.reusecache;

import org.apache.druid.query.aggregation.Aggregator;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

public class AggregatedValue  implements Serializable
{
  // 存储每个聚合器的最终状态（非工厂类）
  private final List<Object> aggregatorStates;

  public AggregatedValue(List<Aggregator> aggregators) {
    this.aggregatorStates = aggregators.stream()
                                       .map(agg -> agg.get()) // 获取聚合器最终状态
                                       .collect(Collectors.toList());
  }

  public Object getAggregatorState(int index) {
    return aggregatorStates.get(index);
  }
  }
