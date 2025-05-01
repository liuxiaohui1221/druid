package org.apache.druid.client.materializedview;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.GranularityType;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;

import java.io.IOException;
import java.util.Objects;
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
  private final int lifeTime;//intervals的存活时间，hour=[0-23]

  @JsonCreator
  public DerivativeDataSourceCreationParams(@JsonProperty("dataSource") String dataSource,
                                            @JsonProperty("intervalStr") String intervalStr,
                                            @JsonProperty("dimensions") Set<String> dimensions,
                                            @JsonProperty("metrics") Set<String> metrics,
                                            @JsonProperty("queryGranularity") Granularity queryGranularity,
                                            @JsonProperty("lifeTime") int lifeTime
                                            ) {
    this.dataSource = dataSource;
    this.intervalStr = intervalStr;
    this.dimensions = dimensions;
    this.metrics = metrics;
    this.queryGranularity = queryGranularity;
    this.lifeTime = lifeTime;
  }

  // Getters
  @JsonProperty("dataSource")
  public String getDataSource() { return dataSource; }
  @JsonProperty("intervalStr")
  public String getIntervalStr() { return intervalStr; }
  @JsonProperty("dimensions")
  public Set<String> getDimensions() {
    return dimensions;
  }
  @JsonProperty("metrics")
  public Set<String> getMetrics() {
    return metrics;
  }
  @JsonProperty("queryGranularity")
  public Granularity getQueryGranularity() {
    return queryGranularity;
  }
  @JsonProperty("lifeTime")
  public int getLifeTime() {
    return lifeTime;
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
    DerivativeDataSourceCreationParams that = (DerivativeDataSourceCreationParams) o;
    return lifeTime == that.lifeTime
           && Objects.equals(dataSource, that.dataSource)
           && Objects.equals(
        intervalStr,
        that.intervalStr
    )
           && Objects.equals(dimensions, that.dimensions)
           && Objects.equals(metrics, that.metrics)
           && Objects.equals(queryGranularity, that.queryGranularity);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dataSource, intervalStr, dimensions, metrics, queryGranularity, lifeTime);
  }

  @Override
  public String toString()
  {
    return "DerivativeDataSourceCreationParams{" +
           "dataSource='" + dataSource + '\'' +
           ", intervalStr='" + intervalStr + '\'' +
           ", dimensions=" + dimensions +
           ", metrics=" + metrics +
           ", queryGranularity=" + queryGranularity +
           ", lifeTime=" + lifeTime +
           '}';
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
