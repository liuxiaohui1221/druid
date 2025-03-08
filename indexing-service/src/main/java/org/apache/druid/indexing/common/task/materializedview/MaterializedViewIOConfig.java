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

package org.apache.druid.indexing.common.task.materializedview;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.indexing.common.task.CompactionInputSpec;
import org.apache.druid.segment.indexing.IOConfig;

import java.util.Objects;

/**
 * {@link IOConfig} for {@link MaterializedViewTask}. Should be synchronized with {@link
 * org.apache.druid.client.indexing.ClientCompactionIOConfig}.
 *
 * @see CompactionInputSpec
 */
@JsonTypeName("materialized_view")
public class MaterializedViewIOConfig implements IOConfig
{
  private final MaterializedViewInputSpec inputSpec;
  private final Boolean appendToExisting;
  private final Boolean isDropExisting;

  @JsonCreator
  public MaterializedViewIOConfig(
      @JsonProperty("inputSpec") MaterializedViewInputSpec inputSpec,
      @JsonProperty("appendToExisting") Boolean appendToExisting
  )
  {
    this.inputSpec = inputSpec;
    this.appendToExisting = appendToExisting;
    this.isDropExisting = false;
  }


  @JsonProperty
  public MaterializedViewInputSpec getInputSpec()
  {
    return inputSpec;
  }

  @Override
  @JsonProperty
  public boolean isDropExisting()
  {
    return isDropExisting;
  }

  @JsonProperty
  public boolean appendToExisting()
  {
    return appendToExisting;
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
    MaterializedViewIOConfig that = (MaterializedViewIOConfig) o;
    return isDropExisting == that.isDropExisting && Objects.equals(inputSpec, that.inputSpec);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(inputSpec, isDropExisting);
  }

  @Override
  public String toString()
  {
    return "MaterializedViewIOConfig{" +
           "inputSpec=" + inputSpec +
           ", appendToExisting=" + appendToExisting +
           ", isDropExisting=" + isDropExisting +
           '}';
  }

}
