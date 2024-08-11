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

package org.apache.druid.indexing.materializedview;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.apache.druid.client.materializedview.ClientTaskGranularitySpec;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManagerConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.metadata.MetadataSupervisorManager;
import org.apache.druid.metadata.SqlSegmentsMetadataManager;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.TuningConfig;
import org.apache.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class MaterializedViewSupervisorSpec implements SupervisorSpec
{
  private static final String SUPERVISOR_TYPE = "materialized_view";
  private final String baseDataSource;
  private final DimensionsSpec dimensionsSpec;
  private final AggregatorFactory[] aggregators;
  protected final ClientTaskGranularitySpec granularitySpec;
  private final TuningConfig tuningConfig;
  private final String dataSourceName;
  private final Map<String, Object> context;
  private final Set<String> metrics;
  private final Set<String> dimensions;
  private final SupervisorStateManagerConfig supervisorStateManagerConfig;
  private final boolean suspended;

  public MaterializedViewSupervisorSpec(
      @JsonProperty("baseDataSource") String baseDataSource,
      @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec,
      @JsonProperty("metricsSpec") AggregatorFactory[] aggregators,
      @JsonProperty("granularitySpec") ClientTaskGranularitySpec granularitySpec,
      @JsonProperty("tuningConfig") TuningConfig tuningConfig,
      @JsonProperty("dataSource") String dataSourceName,
      @JsonProperty("context") Map<String, Object> context,
      @JsonProperty("suspended") Boolean suspended,
      @JacksonInject SupervisorStateManagerConfig supervisorStateManagerConfig
  )
  {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(baseDataSource),
        "baseDataSource cannot be null or empty. Please provide a baseDataSource."
    );
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(dataSourceName),
        "dataSource cannot be null or empty. Please provide a dataSource."
    );
    this.baseDataSource = baseDataSource;

    // 兼容web-console页面提交
    if (dimensionsSpec != null
        && (dimensionsSpec.getDimensions() == null || dimensionsSpec.getDimensions().size() == 0)) {
      this.dimensionsSpec = null;
    } else {
      this.dimensionsSpec = dimensionsSpec;
    }
    if (aggregators == null || aggregators.length == 0) {
      this.aggregators = null;
    } else {
      this.aggregators = aggregators;
    }
    this.tuningConfig = tuningConfig;
    this.granularitySpec = granularitySpec;
    this.dataSourceName = dataSourceName;
    this.context = context == null ? new HashMap<>() : context;
    this.supervisorStateManagerConfig = supervisorStateManagerConfig;
    this.suspended = suspended != null ? suspended : false;

    this.metrics = new HashSet<>();
    if (aggregators != null) {
      for (AggregatorFactory aggregatorFactory : aggregators) {
        metrics.add(aggregatorFactory.getName());
      }
    }
    this.dimensions = new HashSet<>();
    if (dimensionsSpec != null) {
      for (DimensionSchema schema : dimensionsSpec.getDimensions()) {
        dimensions.add(schema.getName());
      }
    }
  }

  public abstract Task createTask(
      Interval interval,
      String version,
      List<DataSegment> segments,
      boolean appendToExisting
  );

  public abstract boolean forceOverwrite();

  public abstract boolean isOverwritePartition();

  public Task createTask(List<DataSegment> segments, boolean appendToExisting)
  {
    return createTask(null, null, segments, appendToExisting);
  }

  public Set<String> getDimensions()
  {
    return dimensions;
  }

  public Set<String> getMetrics()
  {
    return metrics;
  }

  @JsonProperty("baseDataSource")
  public String getBaseDataSource()
  {
    return baseDataSource;
  }

  @JsonProperty("dimensionsSpec")
  public DimensionsSpec getDimensionsSpec()
  {
    return dimensionsSpec;
  }

  @JsonProperty("metricsSpec")
  public AggregatorFactory[] getMetricsSpec()
  {
    return aggregators;
  }

  @JsonProperty("granularitySpec")
  public ClientTaskGranularitySpec getGranularitySpec()
  {
    return granularitySpec;
  }

  @JsonProperty("tuningConfig")
  public TuningConfig getTuningConfig()
  {
    return tuningConfig;
  }

  @JsonProperty("dataSource")
  public String getDataSourceName()
  {
    return dataSourceName;
  }

  @JsonProperty("context")
  public Map<String, Object> getContext()
  {
    return context;
  }

  @Override
  @JsonProperty("suspended")
  public boolean isSuspended()
  {
    return suspended;
  }

  @Override
  @JsonProperty("type")
  public String getType()
  {
    return SUPERVISOR_TYPE;
  }

  @Override
  @JsonProperty("source")
  public String getSource()
  {
    return getBaseDataSource();
  }

  @Override
  public List<String> getDataSources()
  {
    return ImmutableList.of(dataSourceName);
  }

  public SupervisorStateManagerConfig getSupervisorStateManagerConfig()
  {
    return supervisorStateManagerConfig;
  }

  @Override
  public String toString()
  {
    return "MaterializedViewSupervisorSpec{" +
           "baseDataSource=" + baseDataSource +
           ", dimensions=" + dimensions +
           ", metrics=" + metrics +
           '}';
  }
}
