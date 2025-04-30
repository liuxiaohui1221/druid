package org.apache.druid.client.materializedview;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.query.aggregation.AggregatorFactory;

import javax.annotation.Nullable;

public class InputDataSourceSpec
{
  private final String dataSource;
  private final DimensionsSpec dimensionsSpec;
  private final AggregatorFactory[] metricsSpec;
  private final ClientTaskGranularitySpec granularitySpec;

  public InputDataSourceSpec(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("dimensionsSpec") @Nullable DimensionsSpec dimensionsSpec,
      @JsonProperty("metricsSpec") @Nullable AggregatorFactory[] metricsSpec,
      @JsonProperty("granularitySpec") ClientTaskGranularitySpec granularitySpec
  )
  {
    this.dataSource = dataSource;
    this.dimensionsSpec = dimensionsSpec;
    this.metricsSpec = metricsSpec;
    this.granularitySpec = granularitySpec;
  }

  @JsonProperty
  public String getDataSource(){
    return dataSource;
  }
  @JsonProperty
  public DimensionsSpec getDimensionsSpec(){
    return dimensionsSpec;
  }
  @JsonProperty
  public AggregatorFactory[] getMetricsSpec(){
    return metricsSpec;
  }
  @JsonProperty
  public ClientTaskGranularitySpec getGranularitySpec(){
    return granularitySpec;
  }
}
