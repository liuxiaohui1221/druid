/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.overlord;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.druid.client.materializedview.ClientTaskGranularitySpec;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Comparators;

import java.util.Objects;

public class DerivativeDataSource/* implements Comparable<DerivativeDataSource>*/
{
  private final String dataSource;
  private final String baseDataSource;
  private final ClientTaskGranularitySpec granularitySpec;

  @JsonCreator
  public DerivativeDataSource(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("baseDataSource") String baseDataSource,
      @JsonProperty("granularitySpec") ClientTaskGranularitySpec granularitySpec
  )
  {
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.baseDataSource = Preconditions.checkNotNull(baseDataSource, "baseDataSource");
    this.granularitySpec = Preconditions.checkNotNull(granularitySpec, "granularitySpec");
  }

  @VisibleForTesting
  public DerivativeDataSource(
      String dataSource,
      String baseDataSource,
      Granularity segmentGranularity
  )
  {
    this(dataSource, baseDataSource, new ClientTaskGranularitySpec(segmentGranularity, segmentGranularity, true));
  }

  @JsonCreator
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonCreator
  public String getBaseDataSource()
  {
    return baseDataSource;
  }

  @JsonCreator
  public ClientTaskGranularitySpec getGranularitySpec()
  {
    return granularitySpec;
  }

  /*@Override
  public int compareTo(DerivativeDataSource o)
  {
    int result = Comparators.granularityGreaterFirst()
                            .compare(
                                granularitySpec.getQueryGranularity(),
                                o.getGranularitySpec().getQueryGranularity()
                            );
    if (result != 0) {
      return result;
    }
    int result1 = Comparators.granularityGreaterFirst()
                             .compare(
                                 granularitySpec.getSegmentGranularity(),
                                 o.getGranularitySpec().getSegmentGranularity()
                             );
    if (result1 != 0) {
      return result1;
    }
    int result2 = dataSource.compareTo(o.getDataSource());
    if (result2 != 0) {
      return result2;
    }
    return baseDataSource.compareTo(o.getBaseDataSource());
  }
*/
  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DerivativeDataSource that = (DerivativeDataSource) o;
    return Objects.equals(dataSource, that.dataSource)
           && Objects.equals(baseDataSource, that.baseDataSource)
           && Objects.equals(granularitySpec, that.granularitySpec);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dataSource, baseDataSource, granularitySpec);
  }

  @Override
  public String toString()
  {
    return "DerivativeDataSource{" +
           "  dataSource='" + dataSource + '\'' +
           ", baseDataSource='" + baseDataSource + '\'' +
           ", granularitySpec=" + granularitySpec +
           '}';
  }
}
