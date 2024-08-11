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

package org.apache.druid.client.materializedview;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.druid.indexing.overlord.DataSourceMetadata;

import java.util.Objects;


public class DerivativeDataSourceMetadata implements DataSourceMetadata
{
  private final String baseDataSource;
  private final ClientTaskGranularitySpec granularitySpec;

  @JsonCreator
  public DerivativeDataSourceMetadata(
      @JsonProperty("baseDataSource") String baseDataSource,
      @JsonProperty("granularitySpec") ClientTaskGranularitySpec granularitySpec
  )
  {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(baseDataSource),
        "baseDataSource cannot be null or empty. Please provide a baseDataSource."
    );
    this.granularitySpec = Preconditions.checkNotNull(
        granularitySpec,
        "granularitySpec cannot be null. This is not a valid DerivativeDataSourceMetadata."
    );
    this.baseDataSource = baseDataSource;
  }

  @JsonProperty("granularitySpec")
  public ClientTaskGranularitySpec getGranularitySpec()
  {
    return granularitySpec;
  }

  @JsonProperty("baseDataSource")
  public String getBaseDataSource()
  {
    return baseDataSource;
  }

  @Override
  public boolean isValidStart()
  {
    return false;
  }

  @Override
  public DataSourceMetadata asStartMetadata()
  {
    return this;
  }

  @Override
  public boolean matches(DataSourceMetadata other)
  {
    return equals(other);
  }

  @Override
  public DataSourceMetadata plus(DataSourceMetadata other)
  {
    throw new UnsupportedOperationException("Derivative dataSource metadata is not allowed to plus");
  }

  @Override
  public DataSourceMetadata minus(DataSourceMetadata other)
  {
    throw new UnsupportedOperationException("Derivative dataSource metadata is not allowed to minus");
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
    DerivativeDataSourceMetadata that = (DerivativeDataSourceMetadata) o;
    return Objects.equals(baseDataSource, that.baseDataSource) && Objects.equals(granularitySpec, that.granularitySpec);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(baseDataSource, granularitySpec);
  }

  @Override
  public String toString()
  {
    return "DerivativeDataSourceMetadata{" +
           "baseDataSource='" + baseDataSource + '\'' +
           ", granularitySpec=" + granularitySpec +
           '}';
  }
}
