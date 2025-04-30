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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;


public class PreQuerryDataSourceMetadata implements DataSourceMetadata
{
  private final InputDataSourceSpec inputDataSourceSpec;

  @JsonCreator
  public PreQuerryDataSourceMetadata(
      @JsonProperty("inputSpec") InputDataSourceSpec inputDataSourceSpec
  )
  {
    Preconditions.checkArgument(inputDataSourceSpec!=null,
        "baseDataSource cannot be null or empty. Please provide a baseDataSource."
    );
    this.inputDataSourceSpec=inputDataSourceSpec;
  }

  public InputDataSourceSpec getInputDataSourceSpec()
  {
    return inputDataSourceSpec;
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

}
