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

import org.joda.time.Interval;

import java.util.List;
import java.util.Objects;

public class CandidateGroup implements Comparable<Integer>
{
  private final Interval mvInterval;
  private final List<CandidateSegments> candidateBaseIntervals;
  private int score;
  private boolean isOverwrite;

  public CandidateGroup(
      Interval mvInterval,
      int score,
      boolean isOverwrite,
      List<CandidateSegments> candidateBaseIntervals
  )
  {
    this.mvInterval = mvInterval;
    this.score = score;
    this.isOverwrite = isOverwrite;
    this.candidateBaseIntervals = candidateBaseIntervals;
  }

  @Override
  public int compareTo(Integer sor)
  {
    return Integer.compare(sor, this.score);
  }

  public void setScore(int score)
  {
    this.score = score;
  }

  public void setOverwrite(boolean overwrite)
  {
    isOverwrite = overwrite;
  }

  public Interval getMvInterval()
  {
    return mvInterval;
  }

  public int getScore()
  {
    return score;
  }

  public boolean isOverwrite()
  {
    return isOverwrite;
  }

  public List<CandidateSegments> getCandidateBaseIntervals()
  {
    return candidateBaseIntervals;
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
    CandidateGroup that = (CandidateGroup) o;
    return score == that.score
           && isOverwrite == that.isOverwrite
           && Objects.equals(mvInterval, that.mvInterval)
           && Objects.equals(candidateBaseIntervals, that.candidateBaseIntervals);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(mvInterval, score, isOverwrite, candidateBaseIntervals);
  }

  @Override
  public String toString()
  {
    return "CandidateGroup{" +
           "mvInterval=" + mvInterval +
           ", candidateBaseIntervals=" + candidateBaseIntervals +
           ", score=" + score +
           ", isOverwrite=" + isOverwrite +
           '}';
  }
}
