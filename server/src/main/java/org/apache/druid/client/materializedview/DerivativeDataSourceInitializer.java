package org.apache.druid.client.materializedview;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.hash.Hashing;
import jdk.nashorn.internal.ir.ObjectNode;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.joda.time.Interval;
import org.skife.jdbi.v2.Handle;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

public class DerivativeDataSourceInitializer implements Function<DerivativeDataSourceCreationParams, DataSourceMetadata>
{

  @Override
  public DataSourceMetadata apply(DerivativeDataSourceCreationParams input) {
    try {
      //找到兼容性匹配的最高聚合粒度和最小维度的派生表
      String baseDataSource = input.getBaseDataSource();
      String dataSource = input.getDataSource();
      String intervalStr = input.getIntervalStr();
      ClientTaskGranularitySpec granularitySpec = input.getGranularitySpec();
      Set<String> dimensions = input.getDimensions();
      Set<String> metrics = input.getMetrics();

      // 使用 Druid 内置工具类处理时间区间
      Set<Interval> intervals = IntervalUtils.parseAndMerge(intervalStr);

      return new DerivativeDataSourceMetadata(baseDataSource,granularitySpec,dimensions,metrics,intervals);
    } catch (IOException e) {
      throw new RuntimeException("Failed to initialize data source", e);
    }
  }
}
