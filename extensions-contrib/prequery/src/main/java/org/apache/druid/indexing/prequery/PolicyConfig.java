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
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.Locale;

public class PolicyConfig
{
  public static final String SCORE = "score";
  //默认物化区间，物化segments中最新7天的segment
  public static final Period DEFAULT_INGESTION_DURATION = new Period("P7D");
  public static final Period DEFAULT_FIRST_PERIOD_FROM_LATEST = new Period("P3D");
  public static final Period DEFAULT_SKIP_PERIOD_FROM_LATEST = new Period("PT0S");
  public static final long MAX_TASK_INPUT_SIZE = 400_000_000;
  private final String type;
  //第一段区间内：采用得分策略选择
  private final Period firstPeriodFromLatest;
  private final Period skipPeriodFromLatest;
  private final Period ingestDuration;
  private final long inputMaxSizeForAppendingTask;
  //在appending模式下，第二段物化区间是否开启overwrite,解决增量物化模式下，过多segment的问题
  private final boolean enableSecondPeriodOverwrite;
  private final Integer targetRowsPerSegmentForOverwrite;

  @JsonCreator
  public PolicyConfig(
      @JsonProperty("type") @Nullable String type,
      @JsonProperty("skipPeriodFromLatest") @Nullable Period skipPeriodFromLatest,
      @JsonProperty("firstPeriodFromLatest") @Nullable Period firstPeriodFromLatest,
      @JsonProperty("ingestDuration") @Nullable Period ingestDuration,
      @JsonProperty("inputMaxSizeForAppendingTask") @Nullable Long inputMaxSizeForAppendingTask,
      @JsonProperty("enableSecondPeriodOverwrite") @Nullable Boolean enableSecondPeriodOverwrite,
      @JsonProperty("targetRowsPerSegmentForOverwrite") @Nullable Integer targetRowsPerSegmentForOverwrite
  )
  {
    this.type = type == null ? SCORE : type.toLowerCase(Locale.ROOT);
    this.ingestDuration = ingestDuration == null ? DEFAULT_INGESTION_DURATION : ingestDuration;
    this.firstPeriodFromLatest = firstPeriodFromLatest == null
                                 ? DEFAULT_FIRST_PERIOD_FROM_LATEST
                                 : firstPeriodFromLatest;
    this.skipPeriodFromLatest = skipPeriodFromLatest == null
                                ? DEFAULT_SKIP_PERIOD_FROM_LATEST
                                : skipPeriodFromLatest;
    this.inputMaxSizeForAppendingTask = inputMaxSizeForAppendingTask == null ? MAX_TASK_INPUT_SIZE : inputMaxSizeForAppendingTask;
    this.enableSecondPeriodOverwrite = enableSecondPeriodOverwrite == null ? true : enableSecondPeriodOverwrite;
    this.targetRowsPerSegmentForOverwrite = targetRowsPerSegmentForOverwrite;
  }

  @JsonProperty("type")
  public String getType()
  {
    return type;
  }

  @JsonProperty("skipPeriodFromLatest")
  public Period getSkipPeriodFromLatest()
  {
    return skipPeriodFromLatest;
  }

  @JsonProperty("firstPeriodFromLatest")
  public Period getFirstPeriodFromLatest()
  {
    return firstPeriodFromLatest;
  }

  @JsonProperty("ingestDuration")
  public Period getIngestDuration()
  {
    return ingestDuration;
  }

  @JsonProperty("inputMaxSizeForAppendingTask")
  public long getInputMaxSizeForAppendingTask()
  {
    return inputMaxSizeForAppendingTask;
  }

  @JsonProperty("enableSecondPeriodOverwrite")
  public boolean isEnableSecondPeriodOverwrite()
  {
    return enableSecondPeriodOverwrite;
  }

  @JsonProperty("targetRowsPerSegmentForOverwrite")
  public Integer getTargetRowsPerSegmentForOverwrite()
  {
    return targetRowsPerSegmentForOverwrite;
  }
}
