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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.joda.time.Period;

public class MaterializedViewTaskConfig 
{
  @JsonProperty
  private Period taskCheckDuration = new Period("PT1M");
  
  public Period getTaskCheckDuration()
  {
    return taskCheckDuration;
  }

  @VisibleForTesting
  public void setTaskCheckDuration(Period taskCheckDuration)
  {
    this.taskCheckDuration = taskCheckDuration;
  }

  @JsonProperty
  private Period hadoopIntervalCheckDuration = new Period("P3D");

  public Period getHadoopIntervalCheckDuration()
  {
    return hadoopIntervalCheckDuration;
  }

  public void setHadoopIntervalCheckDuration(Period hadoopIntervalCheckDuration)
  {
    this.hadoopIntervalCheckDuration = hadoopIntervalCheckDuration;
  }

  @JsonProperty
  private boolean enableTruncateIngestionTime = true;

  public boolean isEnableTruncateIngestionTime()
  {
    return enableTruncateIngestionTime;
  }

  public void setEnableTruncateIngestionTime(boolean enableTruncateIngestionTime)
  {
    this.enableTruncateIngestionTime = enableTruncateIngestionTime;
  }

  @JsonProperty
  private int maxNumSegmentsToMerge = 30;

  public int getMaxNumSegmentsToMerge()
  {
    return maxNumSegmentsToMerge;
  }

  @JsonProperty
  private PartitionsSpec backupOverwritePartitionsSpec = getDefaultPartitionsSpec(true, null);

  public PartitionsSpec getBackupOverwritePartitionsSpec()
  {
    return backupOverwritePartitionsSpec;
  }

  @JsonProperty
  private PartitionsSpec backupAppendingPartitionsSpec = getDefaultPartitionsSpec(false, null);

  public PartitionsSpec getBackupAppendingPartitionsSpec()
  {
    return backupAppendingPartitionsSpec;
  }

  public static PartitionsSpec getDefaultPartitionsSpec(boolean isOverwrite, Integer maxRowsPerSegment)
  {
    if (isOverwrite) {
      return new HashedPartitionsSpec(maxRowsPerSegment, null, null);
    } else {
      return new DynamicPartitionsSpec(Integer.MAX_VALUE, null);
    }
  }
}
