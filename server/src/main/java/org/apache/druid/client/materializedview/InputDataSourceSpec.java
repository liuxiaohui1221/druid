package org.apache.druid.client.materializedview;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.query.aggregation.AggregatorFactory;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Objects;

public class InputDataSourceSpec
{
  private final String dataSource;
  private final DimensionsSpec dimensionsSpec;
  private final AggregatorFactory[] metricsSpec;
  private final ClientTaskGranularitySpec granularitySpec;

  @JsonCreator
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

  @JsonProperty("dataSource")
  public String getDataSource(){
    return dataSource;
  }
  @JsonProperty("dimensionsSpec")
  public DimensionsSpec getDimensionsSpec(){
    return dimensionsSpec;
  }
  @JsonProperty("metricsSpec")
  public AggregatorFactory[] getMetricsSpec(){
    return metricsSpec;
  }
  @JsonProperty("granularitySpec")
  public ClientTaskGranularitySpec getGranularitySpec(){
    return granularitySpec;
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
    InputDataSourceSpec that = (InputDataSourceSpec) o;
    return Objects.equals(dataSource, that.dataSource) && Objects.equals(
        dimensionsSpec,
        that.dimensionsSpec
    ) && Objects.deepEquals(metricsSpec, that.metricsSpec) && Objects.equals(
        granularitySpec,
        that.granularitySpec
    );
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dataSource, dimensionsSpec, Arrays.hashCode(metricsSpec), granularitySpec);
  }

  @Override
  public String toString()
  {
    return "InputDataSourceSpec{" +
           "dataSource='" + dataSource + '\'' +
           ", dimensionsSpec=" + dimensionsSpec +
           ", metricsSpec=" + Arrays.toString(metricsSpec) +
           ", granularitySpec=" + granularitySpec +
           '}';
  }
}
