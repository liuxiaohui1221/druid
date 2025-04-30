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

import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;
import org.joda.time.base.BaseInterval;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.SortedMap;
import java.util.stream.Collectors;

public class ScoreMaterializedViewIterator
    extends PreQueryIterator<CandidateSegments>
{
  private static final EmittingLogger log = new EmittingLogger(ScoreMaterializedViewIterator.class);

  private final Map<Interval, List<DataSegment>> baseSegments;
  private final SortedMap<Interval, Pair<Boolean, String>> sortedToBuildVersion;
  private final PolicyConfig config;
  private final Map<Interval, List<DataSegment>> materializedViewIngestSegment = new HashMap<>();
  private final PriorityQueue<CandidateSegments> queue = new PriorityQueue<>(
      Comparator.comparingInt(CandidateSegments::getScore).reversed()
  );

  public ScoreMaterializedViewIterator(
      Map<Interval, List<DataSegment>> baseSegments,
      SortedMap<Interval, Pair<Boolean, String>> sortedToBuildVersion,
      PolicyConfig config
  )
  {
    this.baseSegments = baseSegments;
    this.sortedToBuildVersion = sortedToBuildVersion;
    this.config = config;
    reset();
  }

  @Override
  public boolean hasNext()
  {
    return !queue.isEmpty();
  }

  @Override
  public CandidateSegments next()
  {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    CandidateSegments poll = queue.poll();
    if (poll == null) {
      throw new NoSuchElementException();
    }
    return poll;
  }


  @Override
  void reset()
  {
    inIngestPeroidSegment();
    Interval nowInterval = new Interval(
        config.getFirstPeriodFromLatest(),
        PreQuerySupervisor.getNow()
    );
    earlyOffirstPeriodFromLatestSegment(nowInterval);

    Map<Interval, Long> intervalToSize = getIntervalTotalSize();
    Map<Interval, Integer> intervalToNum = getIntervalNumSize();

    Map<Interval, Integer> intervalToScore = getIntervalToScore(intervalToNum, intervalToSize);

    afterOffirstPeriodFromLatestSegment(nowInterval, intervalToScore);
    log.info("reset score MaterializedViewIterator compact");
  }

  private void afterOffirstPeriodFromLatestSegment(Interval nowInterval, Map<Interval, Integer> intervalToScore)
  {
    final Map<Interval, List<DataSegment>> afterOffirstPeriodSegments = new HashMap<>();
    for (Map.Entry<Interval, List<DataSegment>> intervalListEntry : materializedViewIngestSegment.entrySet()) {
      if (nowInterval.overlaps(intervalListEntry.getKey())) {
        afterOffirstPeriodSegments.put(intervalListEntry.getKey(), intervalListEntry.getValue());
      }
    }
    for (Map.Entry<Interval, List<DataSegment>> afterPeriodSegments : afterOffirstPeriodSegments.entrySet()) {
      Pair<Boolean, String> booleanStringPair = sortedToBuildVersion.get(afterPeriodSegments.getKey());
      if (booleanStringPair == null || booleanStringPair.lhs == null) {
        log.error(
            "base datasource in interval [%s] not hive version or isOverwrite [%s]",
            afterPeriodSegments.getKey(),
            booleanStringPair
        );
        continue;
      }
      CandidateSegments candidateSegments = new CandidateSegments(
          afterPeriodSegments.getKey(),
          booleanStringPair.rhs,
          intervalToScore.get(afterPeriodSegments.getKey()),
          booleanStringPair.lhs,
          afterPeriodSegments.getValue()
      );
      queue.add(candidateSegments);
      materializedViewIngestSegment.remove(afterPeriodSegments.getKey());
    }
  }

  private Map<Interval, Integer> getIntervalToScore(
      Map<Interval, Integer> intervalToNum,
      Map<Interval, Long> intervalToSize
  )
  {
    HashSet<Interval> intervals = new HashSet<>();
    HashMap<Interval, Integer> intevalToScore = new HashMap<>();
    int totalNum = intervalToNum.values().stream().mapToInt(x -> x).sum();
    if (totalNum != 0) {
      for (Map.Entry<Interval, Integer> intervalNumEntry : intervalToNum.entrySet()) {
        int numScore = intervalNumEntry.getValue() * 100 / totalNum;
        intevalToScore.putIfAbsent(intervalNumEntry.getKey(), numScore);
        intervals.add(intervalNumEntry.getKey());
      }
    }

    long totalSize = intervalToSize.values().stream().mapToLong(x -> x).sum();
    if (totalSize != 0) {
      for (Map.Entry<Interval, Long> intervalSizeEntry : intervalToSize.entrySet()) {
        int sizeScore = (int) (intervalSizeEntry.getValue() * 100 / totalSize);
        intevalToScore.put(intervalSizeEntry.getKey(), sizeScore + intevalToScore.get(intervalSizeEntry.getKey()));
        intervals.add(intervalSizeEntry.getKey());
      }
    }
    List<Long> collect = intervals.stream().map(BaseInterval::getStartMillis).collect(Collectors.toList());
    if (!collect.isEmpty()) {
      Long minTime = Collections.min(collect);
      long totalTimeDv = Collections.max(collect) - minTime;

      if (totalTimeDv != 0) {
        for (Map.Entry<Interval, Integer> integerEntry : intevalToScore.entrySet()) {
          long startMillis = integerEntry.getKey().getStartMillis();
          int timeScore = (int) ((startMillis - minTime) * 100 / totalTimeDv);
          intevalToScore.put(integerEntry.getKey(), timeScore + intevalToScore.get(integerEntry.getKey()));
        }
      }
    }
    return intevalToScore;
  }

  private void earlyOffirstPeriodFromLatestSegment(Interval nowInterval)
  {
    final Map<Interval, List<DataSegment>> earlyOffirstPeriodSegments = new HashMap<>();
    for (Map.Entry<Interval, List<DataSegment>> intervalListEntry : materializedViewIngestSegment.entrySet()) {
      if (!nowInterval.overlaps(intervalListEntry.getKey())) {
        earlyOffirstPeriodSegments.put(intervalListEntry.getKey(), intervalListEntry.getValue());
      }
    }
    for (Map.Entry<Interval, List<DataSegment>> earlyPeriodSegments : earlyOffirstPeriodSegments.entrySet()) {
      Pair<Boolean, String> booleanStringPair = sortedToBuildVersion.get(earlyPeriodSegments.getKey());
      if (booleanStringPair == null || booleanStringPair.lhs == null) {
        log.error(
            "base datasource in interval [%s] not hive version or isOverwrite [%s]",
            earlyPeriodSegments.getKey(),
            booleanStringPair
        );
        continue;
      }
      CandidateSegments candidateSegments = new CandidateSegments(
          earlyPeriodSegments.getKey(),
          booleanStringPair.rhs,
          Integer.MAX_VALUE,
          booleanStringPair.lhs,
          earlyPeriodSegments.getValue()
      );
      queue.add(candidateSegments);
      materializedViewIngestSegment.remove(earlyPeriodSegments.getKey());
    }
  }

  private void inIngestPeroidSegment()
  {
    for (Map.Entry<Interval, List<DataSegment>> intervalListEntry : baseSegments.entrySet()) {
      if (sortedToBuildVersion.containsKey(intervalListEntry.getKey())) {
        materializedViewIngestSegment.put(intervalListEntry.getKey(), intervalListEntry.getValue());
      }
    }
  }

  private Map<Interval, Long> getIntervalTotalSize()
  {
    return materializedViewIngestSegment.entrySet()
                                        .stream()
                                        .filter(Objects::nonNull)
                                        .map(x -> new AbstractMap.SimpleEntry<>(
                                            x.getKey(),
                                            x.getValue().stream().mapToLong(DataSegment::getSize).sum()
                                        ))
                                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private Map<Interval, Integer> getIntervalNumSize()
  {
    return materializedViewIngestSegment.entrySet().stream().filter(Objects::nonNull).map(x -> new AbstractMap.SimpleEntry<>(
        x.getKey(),
        x.getValue().size()
    )).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
