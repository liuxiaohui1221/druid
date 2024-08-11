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
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.firehose.WindowedSegmentId;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class SpecificMaterializedSegmentsSpec implements MaterializedViewInputSpec
{
  public static final String TYPE = "segments";

  private final List<WindowedSegmentId> segments;

  public static SpecificMaterializedSegmentsSpec fromSegments(List<DataSegment> segments)
  {
    Preconditions.checkArgument(!segments.isEmpty(), "Empty segment list");
    return new SpecificMaterializedSegmentsSpec(
        segments.stream()
                .map(segment -> new WindowedSegmentId(
                    segment.getId().toString(),
                    Collections.singletonList(segment.getInterval()), segment.getSize()
                ))
                .collect(Collectors.toList())
    );
  }

  @JsonCreator
  public SpecificMaterializedSegmentsSpec(
      @JsonProperty("segments") List<WindowedSegmentId> segments
  )
  {
    this.segments = segments;
    // Sort segments to use in validateSegments.
    Collections.sort(
        this.segments,
        (WindowedSegmentId c1, WindowedSegmentId c2) -> StringUtils.compare(
            c1.getSegmentId(),
            c2.getSegmentId()
        )
    );
  }

  @Override
  @JsonProperty
  public List<WindowedSegmentId> getSegments()
  {
    return segments;
  }

  @Override
  public Set<Interval> findInterval(String baseDataSource)
  {
    return segments
        .stream()
        .map(segment -> SegmentId.tryParse(baseDataSource, segment.getSegmentId()).getInterval())
        .collect(Collectors.toSet());
  }

  @Override
  public boolean validateSegments(LockGranularity lockGranularityInUse, List<DataSegment> latestSegments)
  {
    final List<WindowedSegmentId> thoseSegments = latestSegments
        .stream()
        .map(segment -> new WindowedSegmentId(
            segment.getId().toString(),
            Collections.singletonList(segment.getInterval()), segment.getSize()
        ))
        .sorted()
        .collect(Collectors.toList());
    if (lockGranularityInUse == LockGranularity.TIME_CHUNK) {
      return this.segments.equals(thoseSegments);
    } else {
      return thoseSegments.containsAll(segments);
    }
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
    SpecificMaterializedSegmentsSpec that = (SpecificMaterializedSegmentsSpec) o;
    return Objects.equals(segments, that.segments);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(segments);
  }

  @Override
  public String toString()
  {
    return "SpecificMaterializedSegmentsSpec{" +
           "segments=" + segments +
           '}';
  }
}
