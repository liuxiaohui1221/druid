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

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.timeline.BaseShardSpecsSpec;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.MaterializedDataSegment;
import org.apache.druid.timeline.MaterializedSpec;
import org.apache.druid.timeline.Partitions;
import org.apache.druid.timeline.SegmentTimeline;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class MaterializedDataSegmentTest
{
  private final List<DataSegment> dataSegments = new ArrayList<>();
  private final List<DataSegment> materializedDataSegments = new ArrayList<>();
  private final ObjectMapper mapper = TestHelper.makeJsonMapper();

  @Before
  public void setUp()
  {
    InjectableValues.Std injectableValues = new InjectableValues.Std();
    injectableValues.addValue(DataSegment.PruneSpecsHolder.class, DataSegment.PruneSpecsHolder.DEFAULT);
    mapper.setInjectableValues(injectableValues);

    dataSegments.add(new DataSegment(
        "base",
        Intervals.of("2015-01-01T01Z/2015-01-01T02Z"),
        "test_version02",
        ImmutableMap.of(),
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("m1"),
        new NumberedShardSpec(0, 2),
        new CompactionState(
            new HashedPartitionsSpec(100000, null, ImmutableList.of("dim1")),
            new DimensionsSpec(
                DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim1", "bar", "foo"))
            ),
            ImmutableList.of(ImmutableMap.of("type", "count", "name", "count")),
            ImmutableMap.of("filter", ImmutableMap.of("type", "selector", "dimension", "dim1", "value", "foo")),
            ImmutableMap.of(),
            ImmutableMap.of()
        ),
        9,
        1024
    ));

    dataSegments.add(new DataSegment(
        "base",
        Intervals.of("2015-01-01T01Z/2015-01-01T02Z"),
        "test_version02",
        ImmutableMap.of(),
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("m1"),
        new NumberedShardSpec(1, 2),
        new CompactionState(
            new HashedPartitionsSpec(100000, null, ImmutableList.of("dim1")),
            new DimensionsSpec(
                DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim1", "bar", "foo"))
            ),
            ImmutableList.of(ImmutableMap.of("type", "count", "name", "count")),
            ImmutableMap.of("filter", ImmutableMap.of("type", "selector", "dimension", "dim1", "value", "foo")),
            ImmutableMap.of(),
            ImmutableMap.of()
        ),
        9,
        1024
    ));

    dataSegments.add(new DataSegment(
        "base",
        Intervals.of("2015-01-01T00Z/2015-01-01T01Z"),
        "test_version01",
        ImmutableMap.of(),
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("m1"),
        new NumberedShardSpec(0, 3),
        new CompactionState(
            new HashedPartitionsSpec(100000, null, ImmutableList.of("dim1")),
            new DimensionsSpec(
                DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim1", "bar", "foo"))
            ),
            ImmutableList.of(ImmutableMap.of("type", "count", "name", "count")),
            ImmutableMap.of("filter", ImmutableMap.of("type", "selector", "dimension", "dim1", "value", "foo")),
            ImmutableMap.of(),
            ImmutableMap.of()
        ),
        9,
        1024
    ));

    materializedDataSegments.add(new MaterializedDataSegment(
        "test",
        Intervals.of("2015-01-03T00Z/2015-01-04T00Z"),
        "2015-01-04",
        ImmutableMap.of(),
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("m1"),
        new HashBasedNumberedShardSpec(0, 3, 0, 3, null, null, null),
        null,
        9,
        1024,
        new MaterializedSpec(
            MaterializedSpec.TYPE_SAME_SEGMENT_GRAN,
            new BaseShardSpecsSpec(0, 4, "2015-01-03"),
            null,
            (short) 1
        )
    ));
    materializedDataSegments.add(new MaterializedDataSegment(
        "test",
        Intervals.of("2015-01-03T00Z/2015-01-04T00Z"),
        "2015-01-04",
        ImmutableMap.of(),
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("m1"),
        new HashBasedNumberedShardSpec(1, 3, 1, 3, null, null, null),
        null,
        9,
        1024,
        new MaterializedSpec(
            MaterializedSpec.TYPE_SAME_SEGMENT_GRAN,
            new BaseShardSpecsSpec(0, 4, "2015-01-03"),
            null,
            (short) 1
        )
    ));
    materializedDataSegments.add(new MaterializedDataSegment(
        "test",
        Intervals.of("2015-01-03T00Z/2015-01-04T00Z"),
        "2015-01-04",
        ImmutableMap.of(),
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("m1"),
        new HashBasedNumberedShardSpec(2, 3, 2, 3, null, null, null),
        null,
        9,
        1024,
        new MaterializedSpec(
            MaterializedSpec.TYPE_SAME_SEGMENT_GRAN,
            new BaseShardSpecsSpec(0, 4, "2015-01-03"),
            null,
            (short) 1
        )
    ));
    materializedDataSegments.add(new MaterializedDataSegment(
        "test",
        Intervals.of("2015-01-03T00Z/2015-01-04T00Z"),
        "2015-01-04",
        ImmutableMap.of(),
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("m1"),
        new HashBasedNumberedShardSpec(3, 0, 3, 0, null, null, null),
        null,
        9,
        1024,
        new MaterializedSpec(
            MaterializedSpec.TYPE_SAME_SEGMENT_GRAN,
            new BaseShardSpecsSpec(3, 5, "2015-01-03"),
            null,
            (short) 1
        )
    ));
    materializedDataSegments.add(new MaterializedDataSegment(
        "test",
        Intervals.of("2015-01-03T00Z/2015-01-04T00Z"),
        "2015-01-04",
        ImmutableMap.of(),
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("m1"),
        new HashBasedNumberedShardSpec(4, 0, 4, 0, null, null, null),
        null,
        9,
        1024,
        new MaterializedSpec(
            MaterializedSpec.TYPE_SAME_SEGMENT_GRAN,
            new BaseShardSpecsSpec(3, 5, "2015-01-03"),
            null,
            (short) 1
        )
    ));


  }

  @Test
  public void toCaver() throws IOException
  {
    for (DataSegment dataSegment : dataSegments) {
      byte[] bytes = mapper.writeValueAsBytes(dataSegment);
      DataSegment dataSegment1 = mapper.readValue(bytes, MaterializedDataSegment.class);
      Assert.assertEquals(dataSegment1.getMaterializedSpec(), dataSegment.getMaterializedSpec());
    }
  }

  @Test
  public void timeData()
  {
    VersionedIntervalTimeline<String, DataSegment> timeline = SegmentTimeline
        .forSegments(dataSegments);
    Set<DataSegment> nonOvershadowedObjectsInInterval = timeline.findNonOvershadowedObjectsInInterval(
        Intervals.ETERNITY,
        Partitions.ONLY_COMPLETE
    );

    MaterializedDataSegment materializedDataSegment = new MaterializedDataSegment(
        "test",
        Intervals.of("2015-01-03T00Z/2015-01-04T00Z"),
        "2015-01-04",
        ImmutableMap.of(),
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("m1"),
        new HashBasedNumberedShardSpec(5, 0, 5, 0, null, null, null),
        null,
        9,
        1024,
        new MaterializedSpec(
            MaterializedSpec.TYPE_SAME_SEGMENT_GRAN,
            new BaseShardSpecsSpec(5, 7, "2015-01-03"),
            null,
            (short) 1
        )
    );

    VersionedIntervalTimeline<String, DataSegment> materializedTimeline = SegmentTimeline
        .forSegments(materializedDataSegments);
    materializedTimeline.add(
        materializedDataSegment.getInterval(),
        materializedDataSegment.getVersion(),
        materializedDataSegment.getShardSpec().createChunk(materializedDataSegment)
    );
    Set<DataSegment> materializednonOvershadowedObjectsInInterval = materializedTimeline.findNonOvershadowedObjectsInInterval(
        Intervals.ETERNITY,
        Partitions.ONLY_COMPLETE
    );
  }
}
