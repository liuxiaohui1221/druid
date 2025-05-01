package org.apache.druid.client.materializedview;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.base.Splitter;
import com.google.common.hash.Hashing;
import jdk.nashorn.internal.ir.ObjectNode;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.DerivativeDataSource;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DerivativeDataSourceInitializer implements Function<DerivativeDataSourceCreationParams, DerivativeDataSourceMetadata>
{
  @Override
  public DerivativeDataSourceMetadata apply(DerivativeDataSourceCreationParams input)
  {
    String rootBaseDataSource = DerivativeDataSourceManager.getRootBaseDataSource(input.getDataSource());
    Granularity granularity = input.getQueryGranularity();
    //找到兼容性匹配的最高聚合粒度和最小维度的派生表
    SortedSet<DerivativeDataSource> candidateSortedDerivatives = DerivativeDataSourceManager.getCandidateSortedDerivatives(rootBaseDataSource,
                                                                                                                           input.getDimensions(),
                                                                                                                           granularity
    );
    Map<Interval,Integer> intervalLifeTimes = IntervalUtils.parseAndMerge(input.getIntervalStr(), input.getLifeTime() +
                                                                                               "");
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
