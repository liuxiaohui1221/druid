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

package org.apache.druid.indexing.prequery;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.joda.time.Period;

import javax.annotation.Nullable;

public class PreQueryTaskConfig
{
  @JsonProperty
  private Period taskCheckDuration = new Period("PT1M");
  @JsonProperty
  private Period hadoopIntervalCheckDuration = new Period("P1D");
  @JsonProperty
  private boolean enableTruncateIngestionTime = true;
  @JsonProperty
  private int maxNumSegmentsToMerge = 30;
  @JsonProperty
  boolean skipCheckTemplateLifeTime = true;
  @JsonProperty
  private PartitionsSpec backupOverwritePartitionsSpec = getDefaultPartitionsSpec(true, null);
  @JsonProperty
  private PartitionsSpec backupAppendingPartitionsSpec = getDefaultPartitionsSpec(false, null);

  public PreQueryTaskConfig()
  {
  }

  @JsonCreator
  public PreQueryTaskConfig(
      @JsonProperty("taskCheckDuration") @Nullable Period taskCheckDuration,
      @JsonProperty("hadoopIntervalCheckDuration") @Nullable Period hadoopIntervalCheckDuration,
      @JsonProperty("enableTruncateIngestionTime") @Nullable Boolean enableTruncateIngestionTime,
      @JsonProperty("maxNumSegmentsToMerge") @Nullable Integer maxNumSegmentsToMerge,
      @JsonProperty("skipCheckTemplateLifeTime") @Nullable Boolean skipCheckTemplateLifeTime,
      @JsonProperty("backupOverwritePartitionsSpec") @Nullable  PartitionsSpec backupOverwritePartitionsSpec,
      @JsonProperty("backupAppendingPartitionsSpec") @Nullable  PartitionsSpec backupAppendingPartitionsSpec
  )
  {
    this.taskCheckDuration = taskCheckDuration == null ? new Period("PT1M") : taskCheckDuration;
    this.hadoopIntervalCheckDuration = hadoopIntervalCheckDuration == null ? new Period("P1D") : hadoopIntervalCheckDuration;
    this.enableTruncateIngestionTime = enableTruncateIngestionTime == null || enableTruncateIngestionTime;
    this.maxNumSegmentsToMerge = maxNumSegmentsToMerge == null ? 30 : maxNumSegmentsToMerge;
    this.skipCheckTemplateLifeTime = skipCheckTemplateLifeTime == null || skipCheckTemplateLifeTime;
    this.backupOverwritePartitionsSpec = backupOverwritePartitionsSpec == null ? getDefaultPartitionsSpec(true, null) : backupOverwritePartitionsSpec;
    this.backupAppendingPartitionsSpec = backupAppendingPartitionsSpec == null ? getDefaultPartitionsSpec(false, null) : backupAppendingPartitionsSpec;
  }
  @JsonProperty("taskCheckDuration")
  public Period getTaskCheckDuration()
  {
    return taskCheckDuration;
  }
  @JsonProperty("backupOverwritePartitionsSpec")
  public PartitionsSpec getBackupOverwritePartitionsSpec()
  {
    return backupOverwritePartitionsSpec;
  }
  @JsonProperty("hadoopIntervalCheckDuration")
  public Period getHadoopIntervalCheckDuration()
  {
    return hadoopIntervalCheckDuration;
  }

  public void setHadoopIntervalCheckDuration(Period hadoopIntervalCheckDuration)
  {
    this.hadoopIntervalCheckDuration = hadoopIntervalCheckDuration;
  }
  @JsonProperty("maxNumSegmentsToMerge")
  public int getMaxNumSegmentsToMerge()
  {
    return maxNumSegmentsToMerge;
  }

  @VisibleForTesting
  public void setTaskCheckDuration(Period taskCheckDuration)
  {
    this.taskCheckDuration = taskCheckDuration;
  }
  @JsonProperty("backupAppendingPartitionsSpec")
  public PartitionsSpec getBackupAppendingPartitionsSpec()
  {
    return backupAppendingPartitionsSpec;
  }
  @JsonProperty("enableTruncateIngestionTime")
  public boolean isEnableTruncateIngestionTime()
  {
    return enableTruncateIngestionTime;
  }

  public void setEnableTruncateIngestionTime(boolean enableTruncateIngestionTime)
  {
    this.enableTruncateIngestionTime = enableTruncateIngestionTime;
  }
  public static PartitionsSpec getDefaultPartitionsSpec(boolean isOverwrite, Integer maxRowsPerSegment)
  {
    if (isOverwrite) {
      return new HashedPartitionsSpec(maxRowsPerSegment, null, null);
    } else {
      return new DynamicPartitionsSpec(Integer.MAX_VALUE, null);
    }
  }
  @JsonProperty("skipCheckTemplateLifeTime")
  public boolean getSkipCheckTemplateLifeTime() {
    return skipCheckTemplateLifeTime;
  }
}
