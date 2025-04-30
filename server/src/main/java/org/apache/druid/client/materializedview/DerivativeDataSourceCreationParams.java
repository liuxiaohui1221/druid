package org.apache.druid.client.materializedview;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.GranularityType;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.format.ISOPeriodFormat;

import java.io.IOException;
import java.util.Set;

/**
 * 接收到的预查询模板信息
 */
public class DerivativeDataSourceCreationParams
{
  private final String dataSource;
  private final String intervalStr;
  private final Set<String> dimensions;
  private final Set<String> metrics;
  @JsonDeserialize(using = FlexibleGranularityDeserializer.class)
  private final Granularity queryGranularity;

  @JsonCreator
  public DerivativeDataSourceCreationParams(@JsonProperty("dataSource") String dataSource,
                                            @JsonProperty("intervalStr") String intervalStr,
                                            @JsonProperty("dimensions") Set<String> dimensions,
                                            @JsonProperty("metrics") Set<String> metrics,
                                            @JsonProperty("queryGranularity") Granularity queryGranularity) {
    this.dataSource = dataSource;
    this.intervalStr = intervalStr;
    this.dimensions = dimensions;
    this.metrics = metrics;
    this.queryGranularity = queryGranularity;
  }

  // Getters
  public String getDataSource() { return dataSource; }
  public String getIntervalStr() { return intervalStr; }

  public Set<String> getDimensions() {
    return dimensions;
  }
  public Set<String> getMetrics() {
    return metrics;
  }
  public Granularity getQueryGranularity() {
    return queryGranularity;
  }

  static class FlexibleGranularityDeserializer extends StdDeserializer<Granularity> {
    public FlexibleGranularityDeserializer() {
      super(Granularity.class);
    }

    @Override
    public Granularity deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      JsonNode node = p.getCodec().readTree(p);

      if (node.isTextual()) { // 处理字符串类型
        return GranularityType.valueOf(node.asText()).getDefaultGranularity();
      } else if (node.isObject()) { // 处理对象类型
        String period = node.get("period").asText();
        DateTimeZone tz = DateTimes.inferTzFromString(node.get("timeZone").asText());
        return new PeriodGranularity(Period.parse(period), null, tz);
      }

      throw new IllegalArgumentException("Invalid granularity format");
    }
  }
}
