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
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

/**
 * 物化视图数据源DataSegment中会记录被物化视图任务所物化的原始数据源（即baseDatasouce）的segment列表信息。
 * 当物化视图分区粒度和baseDatasource分区粒度相同时，被物化的原始segment信息记录到MaterializedSpec参数中。
 * 当物化视图分区粒度大于baseDatasource分区粒度时，分区之间存在一对多的关系。
 * 比如：物化视图为day粒度，baseDatasource为hour粒度，则会将物化视图segment对应的baseDatasource的segment信息记录到属性Map<Short, MaterializedSpec>中，
 * 其中key表示baseDatasource的具体interval(hour长度)处于day中的第几个分区，这里的取值范围是[0-23]。
 * MaterializedSpec和Map<Short, MaterializedSpec>两个属性不会同时为null,和同时不为null的情况。
 */
public class MaterializedSpec
{
  public static final byte TYPE_SAME_SEGMENT_GRAN = 1;
  public static final byte TYPE_DIFF_SEGMENT_GRAN = 2;
  public static final byte TYPE_UNSUPPORT_GRAN = -1;
  // type标识物化视图数据源与baseDatasource数据源分区粒度是一对一，还是一对多的关系。
  // 一对一时物化信息记录到MaterializedSpec，一对多则记录到Map<Short, MaterializedSpec>
  private final byte type;
  private final BaseShardSpecsSpec baseShardSpecsSpec;
  // 此map记录被物化的base segment信息列表。
  // key:base分区粒度映射到derivative分区粒度的相对编号，比如hour粒度映射到day粒度，则key为[0-23]
  private final Map<Short, BaseShardSpecsSpec> sourceBaseShardSpecsSpecs;
  private final short mapBuckets;

  @JsonCreator
  public MaterializedSpec(
      @JsonProperty("type") byte type,
      @JsonProperty("baseShardSpecsSpec") @Nullable BaseShardSpecsSpec baseShardSpecsSpec,
      @JsonProperty("sourceBaseShardSpecsSpecs") @Nullable Map<Short, BaseShardSpecsSpec> sourceBaseShardSpecsSpecs,
      @JsonProperty("mapBuckets") short mapBuckets
  )
  {
    Preconditions.checkArgument((type == TYPE_SAME_SEGMENT_GRAN && baseShardSpecsSpec != null)
                                || type == TYPE_DIFF_SEGMENT_GRAN && sourceBaseShardSpecsSpecs != null, "WTF? illegal "
                                                                                                        + "parameters"
                                                                                                        + baseShardSpecsSpec
                                                                                                        + sourceBaseShardSpecsSpecs);
    this.type = type;
    this.baseShardSpecsSpec = baseShardSpecsSpec;
    this.sourceBaseShardSpecsSpecs = sourceBaseShardSpecsSpecs;
    this.mapBuckets = mapBuckets;
  }

  @JsonProperty
  public int getType()
  {
    return type;
  }

  @Nullable
  @JsonProperty
  public BaseShardSpecsSpec getBaseShardSpecsSpec()
  {
    return baseShardSpecsSpec;
  }

  @Nullable
  @JsonProperty
  public Map<Short, BaseShardSpecsSpec> getSourceBaseShardSpecsSpecs()
  {
    return sourceBaseShardSpecsSpecs;
  }

  @JsonProperty
  public short getMapBuckets()
  {
    return mapBuckets;
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
    MaterializedSpec that = (MaterializedSpec) o;
    return type == that.type && mapBuckets == that.mapBuckets && Objects.equals(
        baseShardSpecsSpec,
        that.baseShardSpecsSpec
    ) && Objects.equals(sourceBaseShardSpecsSpecs, that.sourceBaseShardSpecsSpecs);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(type, baseShardSpecsSpec, sourceBaseShardSpecsSpecs, mapBuckets);
  }

  @Override
  public String toString()
  {
    return "MaterializedSpec{" +
           "type=" + type +
           ", baseShardSpecsSpec=" + baseShardSpecsSpec +
           ", sourceBaseShardSpecsSpecs=" + sourceBaseShardSpecsSpecs +
           ", mapBuckets=" + mapBuckets +
           '}';
  }
}
