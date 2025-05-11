package org.apache.druid.client.materializedview;

import org.apache.druid.indexing.overlord.DerivativeDataSource;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.function.Function;

public class DerivativeDataSourceInitializer implements Function<DerivativeDataSourceCreationParams, DerivativeDataSourceMetadata>
{
  @Override
  public DerivativeDataSourceMetadata apply(DerivativeDataSourceCreationParams input)
  {
    String rootBaseDataSource = DerivativeDataSourceManager.getRootBaseDataSource(input.getDataSource());
    Granularity granularity = input.getQueryGranularity();
    Map<Interval,Integer> intervalLifeTimes = IntervalUtils.parseAndMerge(input.getIntervalStr(), input.getLifeTime() +
                                                                                               "");
    //找到兼容性匹配的最高聚合粒度和最小维度的派生表
    SortedSet<DerivativeDataSource> candidateSortedDerivatives = DerivativeDataSourceManager.getCandidateSortedDerivatives(rootBaseDataSource,
                                                                                                                           input.getDimensions(),
                                                                                                                           granularity,
                                                                                                                           new ArrayList<>(intervalLifeTimes.keySet()));
    //过滤失效时段
//    Map<Interval,Integer> filterdIntervalLifeTimes = new HashMap<>();
//    for(Map.Entry<Interval,Integer> entry : intervalLifeTimes.entrySet()){
//      if(!IntervalUtils.isValidLifeTime(DateTimes.nowUtc(),entry.getValue())){
//        continue;
//      }
//      filterdIntervalLifeTimes.put(entry.getKey(),entry.getValue());
//    }
    String baseDataSource = BaseTableSelector.selectBestBase(candidateSortedDerivatives,input.getDimensions(),
                                                             granularity,intervalLifeTimes.keySet(),
                                                             rootBaseDataSource);
    ClientTaskGranularitySpec granularitySpec =
        new ClientTaskGranularitySpec(chooseSegmentGranularity(granularity),
                                      granularity,true);
    Set<String> dimensions = input.getDimensions();
    Set<String> metrics = input.getMetrics();//默认物化所有指标

    return new DerivativeDataSourceMetadata(baseDataSource,granularitySpec,dimensions,metrics,
                                            intervalLifeTimes);
  }

  private Granularity chooseSegmentGranularity(Granularity queryGranularity) {
    return queryGranularity.isFinerThan(Granularities.HOUR)?Granularities.HOUR:Granularities.DAY;
  }
}
