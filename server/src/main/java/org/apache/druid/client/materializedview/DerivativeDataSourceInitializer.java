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
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.joda.time.Interval;
import org.skife.jdbi.v2.Handle;

import java.io.IOException;
import java.util.List;
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
    Set<Interval> intervals = parseAndMerge(input.getIntervalStr());
    String baseDataSource = BaseTableSelector.selectBestBase(candidateSortedDerivatives,input.getDimensions(),
                                                             granularity,intervals,
                                                             rootBaseDataSource);
    ClientTaskGranularitySpec granularitySpec =
        new ClientTaskGranularitySpec(chooseSegmentGranularity(granularity),
                                      granularity,true);
    Set<String> dimensions = input.getDimensions();
    Set<String> metrics = input.getMetrics();//默认物化所有指标

    return new DerivativeDataSourceMetadata(baseDataSource,granularitySpec,dimensions,metrics,intervals);
  }

  private Granularity chooseSegmentGranularity(Granularity queryGranularity) {
    return queryGranularity.isFinerThan(Granularities.HOUR)?Granularities.HOUR:Granularities.DAY;
  }
  public static Set<Interval> parseAndMerge(String intervals) {
    Set<Interval> parsed = Splitter.on(',')
                                    .trimResults()
                                    .splitToList(intervals)
                                    .stream()
                                    .map(Intervals::of)
                                    .collect(Collectors.toSet());

    return parsed;
  }
}
