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

package org.apache.druid.timeline;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * 用于记录被物化的baseDatasource分区下segmentid信息
 */
public class BaseShardSpecsSpec
{
  private final int startPartitionNumber;
  private final int endPartitionNumber; // exclusive
  private final String version;

  @JsonCreator
  public BaseShardSpecsSpec(
      @JsonProperty("startPartitionNumber") int startPartitionNumber,
      @JsonProperty("endPartitionNumber") int endPartitionNumber,
      @JsonProperty("version") String version
  )
  {
    this.startPartitionNumber = startPartitionNumber;
    this.endPartitionNumber = endPartitionNumber;
    this.version = version;
  }

  @JsonProperty
  public int getStartPartitionNumber()
  {
    return startPartitionNumber;
  }

  @JsonProperty
  public int getEndPartitionNumber()
  {
    return endPartitionNumber;
  }

  @JsonProperty
  public String getVersion()
  {
    return version;
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
    BaseShardSpecsSpec that = (BaseShardSpecsSpec) o;
    return startPartitionNumber == that.startPartitionNumber
           && endPartitionNumber == that.endPartitionNumber
           && Objects.equals(version, that.version);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(startPartitionNumber, endPartitionNumber, version);
  }

  @Override
  public String toString()
  {
    return "MaterializedSpec{" +
           "startPartitionNumber=" + startPartitionNumber +
           ", endPartitionNumber=" + endPartitionNumber +
           ", version='" + version + '\'' +
           '}';
  }
}
