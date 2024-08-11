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

import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.util.List;
import java.util.Objects;

public class CandidateSegments implements Comparable<Integer>
{
  private final Interval baseInterval;
  private final String version;
  private final int score;
  private final boolean isOverwrite;
  private final List<DataSegment> segments;

  public CandidateSegments(
      Interval baseInterval,
      String version,
      int score,
      boolean isOverwrite,
      List<DataSegment> segments
  )
  {
    this.baseInterval = baseInterval;
    this.version = version;
    this.score = score;
    this.isOverwrite = isOverwrite;
    this.segments = segments;
  }

  public Interval getBaseInterval()
  {
    return baseInterval;
  }

  public String getVersion()
  {
    return version;
  }

  public int getScore()
  {
    return score;
  }

  public boolean isOverwrite()
  {
    return isOverwrite;
  }

  public List<DataSegment> getSegments()
  {
    return segments;
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
    CandidateSegments that = (CandidateSegments) o;
    return score == that.score && isOverwrite == that.isOverwrite && Objects.equals(
        baseInterval,
        that.baseInterval
    ) && Objects.equals(version, that.version) && Objects.equals(segments, that.segments);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(baseInterval, version, score, isOverwrite, segments);
  }

  @Override
  public String toString()
  {
    return "CandidateSegments{" +
           "baseInterval=" + baseInterval +
           ", version='" + version + '\'' +
           ", score=" + score +
           ", isOverwrite=" + isOverwrite +
           ", segments=" + segments +
           '}';
  }

  @Override
  public int compareTo(Integer score)
  {
    return Integer.compare(score, this.score);
  }
}
