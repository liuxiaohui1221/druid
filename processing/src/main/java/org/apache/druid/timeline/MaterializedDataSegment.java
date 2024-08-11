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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Preconditions;
import org.apache.druid.jackson.CommaListJoinDeserializer;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Recover materialized DataSegment{interval,version,materializedSegment}
 * from MaterializedView DataSegment's MaterializedSegment
 */
public class MaterializedDataSegment extends DataSegment
{
  public MaterializedDataSegment(
      String dataSource,
      Interval interval,
      String version,
      BaseShardSpecsSpec baseShardSpecsSpec,
      long size
  )
  {
    super(
        dataSource,
        interval,
        version,
        null,
        null,
        null,
        new NumberedShardSpec(baseShardSpecsSpec.getEndPartitionNumber(), 0),
        null,
        null,
        size
    );
    this.materializedSpec = new MaterializedSpec(
        MaterializedSpec.TYPE_SAME_SEGMENT_GRAN,
        baseShardSpecsSpec,
        null,
        (short) 1
    );
  }

  public MaterializedDataSegment(
      String dataSource,
      Interval interval,
      String version,
      MaterializedSpec materializedSpec,
      long size
  )
  {
    super(
        dataSource,
        interval,
        version,
        null,
        null,
        null,
        null,
        null,
        null,
        size
    );
    this.materializedSpec = materializedSpec;
  }


  public MaterializedDataSegment(
      String dataSource,
      Interval interval,
      String version,
      @Nullable Map<String, Object> loadSpec,
      @Nullable List<String> dimensions,
      @Nullable List<String> metrics,
      @Nullable ShardSpec shardSpec,
      @Nullable CompactionState lastCompactionState,
      Integer binaryVersion,
      long size,
      @JsonProperty("materializedSpec") @Nullable MaterializedSpec materializedSpec
  )
  {
    super(
        dataSource,
        interval,
        version,
        loadSpec,
        dimensions,
        metrics,
        shardSpec,
        lastCompactionState,
        materializedSpec,
        binaryVersion,
        size
    );
  }

  @JsonCreator
  public MaterializedDataSegment(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("version") String version,
      @JsonProperty("loadSpec") @Nullable Map<String, Object> loadSpec,
      @JsonProperty("dimensions")
      @JsonDeserialize(using = CommaListJoinDeserializer.class)
      @Nullable
      List<String> dimensions,
      @JsonProperty("metrics")
      @JsonDeserialize(using = CommaListJoinDeserializer.class)
      @Nullable
      List<String> metrics,
      @JsonProperty("shardSpec") @Nullable ShardSpec shardSpec,
      @JsonProperty("lastCompactionState") @Nullable CompactionState lastCompactionState,
      @JsonProperty("binaryVersion") Integer binaryVersion,
      @JsonProperty("size") long size,
      @JacksonInject PruneSpecsHolder pruneSpecsHolder,
      @JsonProperty("materializedSpec") @Nullable MaterializedSpec materializedSpec
  )
  {
    super(
        dataSource,
        interval,
        version,
        loadSpec,
        dimensions,
        metrics,
        shardSpec,
        lastCompactionState,
        materializedSpec,
        binaryVersion,
        size,
        pruneSpecsHolder
    );
  }

  @JsonProperty
  @Override
  @Nullable
  public MaterializedSpec getMaterializedSpec()
  {
    return materializedSpec;
  }

  @Nullable
  private BaseShardSpecsSpec getBaseShardShecsSpec()
  {
    if (materializedSpec != null) {
      if (materializedSpec.getType() == MaterializedSpec.TYPE_SAME_SEGMENT_GRAN) {
        return materializedSpec.getBaseShardSpecsSpec();
      } else if (materializedSpec.getType() == MaterializedSpec.TYPE_DIFF_SEGMENT_GRAN) {
        Map<Short, BaseShardSpecsSpec> multiBaseShardSpecsSpec = materializedSpec.getSourceBaseShardSpecsSpecs();
        if (multiBaseShardSpecsSpec != null) {
          Optional<BaseShardSpecsSpec> first = multiBaseShardSpecsSpec.values()
                                                                      .stream()
                                                                      .findFirst();
          if (first.isPresent()) {
            return first.get();
          }
        }
      }
    }
    return null;
  }

  @Override
  public MaterializedDataSegment withStoreMaterializedSegment(MaterializedSpec materializedSpec)
  {
    return builder(this).storeMaterializedSegment(materializedSpec).build();
  }


  public static Builder builder(MaterializedDataSegment segment)
  {
    return new Builder(segment);
  }

  public static class Builder
  {
    private String dataSource;
    private Interval interval;
    private String version;
    private Map<String, Object> loadSpec;
    private List<String> dimensions;
    private List<String> metrics;
    private ShardSpec shardSpec;
    private CompactionState lastCompactionState;
    private MaterializedSpec materializedSpec;
    private Integer binaryVersion;
    private long size;

    public Builder(MaterializedDataSegment segment)
    {
      this.dataSource = segment.getDataSource();
      this.interval = segment.getInterval();
      this.version = segment.getVersion();
      this.loadSpec = segment.getLoadSpec();
      this.dimensions = segment.getDimensions();
      this.metrics = segment.getMetrics();
      this.shardSpec = segment.getShardSpec();
      this.lastCompactionState = segment.getLastCompactionState();
      this.materializedSpec = segment.getMaterializedSpec();
      this.binaryVersion = segment.getBinaryVersion();
      this.size = segment.getSize();
    }

    public Builder storeMaterializedSegment(MaterializedSpec materializedSpec)
    {
      this.materializedSpec = materializedSpec;
      return this;
    }

    public MaterializedDataSegment build()
    {
      // Check stuff that goes into the id, at least.
      Preconditions.checkNotNull(dataSource, "dataSource");
      Preconditions.checkNotNull(interval, "interval");
      Preconditions.checkNotNull(version, "version");
      Preconditions.checkNotNull(shardSpec, "shardSpec");

      return new MaterializedDataSegment(
          dataSource,
          interval,
          version,
          materializedSpec,
          size
      );
    }
  }
}
