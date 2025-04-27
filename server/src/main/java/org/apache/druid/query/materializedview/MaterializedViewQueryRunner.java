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

package org.apache.druid.query.materializedview;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Bytes;
import org.apache.druid.client.CacheUtil;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.MergeSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.Queries;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.ResultLevelCachingQueryRunner;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.LimitSpec;
import org.apache.druid.query.spec.MultipleSpecificSegmentSpec;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.server.QueryResource;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class MaterializedViewQueryRunner<T> implements QueryRunner<T>
{
  private final QueryRunner runner;
  private final MaterializedViewOptimizer optimizer;
  private final CacheConfig cacheConfig;
  private final CacheStrategy cacheStrategy;

  public MaterializedViewQueryRunner(QueryRunner queryRunner,
                                     MaterializedViewOptimizer optimizer, CacheConfig cacheConfig,
                                     CacheStrategy cacheStrategy
  )
  {
    this.runner = queryRunner;
    this.optimizer = optimizer;;
    this.cacheConfig = cacheConfig;
    this.cacheStrategy = cacheStrategy;

  }

  @Override
  public Sequence<T> run(QueryPlus<T> queryPlus, ResponseContext responseContext)
  {
    Query query = queryPlus.getQuery();
    boolean reuseSubQueryCache = CacheUtil.isEnableSubQueryResultCache(query, cacheStrategy, cacheConfig,
                                                                    CacheUtil.ServerType.BROKER);
    //检测是否已缓存结果
    @Nullable
    final Object hitQueryTag = query.getContext().get(QueryResource.HEADER_CACHE_QUERY_HIT);
    final Object hitIntervalsTagObj = query.getContext().get(QueryResource.HEADER_CACHE_INTERVALS_HIT);
    List<Interval> hitIntervalsTag = null;
    if( hitIntervalsTagObj != null && hitIntervalsTagObj instanceof List){
      hitIntervalsTag = (List<Interval>) hitIntervalsTagObj;
    }
    @Nullable
    final Object prevEtag=query.getContext().get(QueryResource.HEADER_IF_NONE_MATCH);

    List<Query> optimizedQueries = optimizer.optimize(query);
    long skip=0;
    long limit=Long.MAX_VALUE;
    if(reuseSubQueryCache || cacheConfig.isPopulateResultLevelCache()){
      optimizedQueries = getUncachedQueries(query, hitQueryTag, hitIntervalsTag, optimizedQueries, responseContext,
                                            prevEtag, reuseSubQueryCache);
      if(optimizedQueries.isEmpty()){{
        return Sequences.empty();
      }}
      if(optimizedQueries.size()>1){
        if(query instanceof GroupByQuery){
          GroupByQuery groupByQuery=(GroupByQuery)query;
          LimitSpec limitSpec = groupByQuery.getLimitSpec();
          if(limitSpec instanceof DefaultLimitSpec){
            DefaultLimitSpec defaultLimitSpec = (DefaultLimitSpec) limitSpec;
            skip=defaultLimitSpec.getOffset();
            limit=defaultLimitSpec.getLimit();
          }
        }
      }
    }
    return new MergeSequence<>(
        query.getResultOrdering(),
        Sequences.simple(
            Lists.transform(
                optimizedQueries,
                (Function<Query, Sequence<T>>) query1 -> runner.run(
                    queryPlus.withQuery(Queries.withBaseDataSource(query1, query1.getDataSource())
                                               // assign the subqueryId. this will be used to validate that every query servers
                                               // have responded per subquery in RetryQueryRunner
                                               .withDefaultSubQueryId()),
                    responseContext
                )
            )
        )
    ).skip(skip).limit(limit);
  }

  @VisibleForTesting
  public List<Query> getUncachedQueries(
      Query query, Object hitQueryTag, List<Interval> hitIntervalsTag,
      List<Query> optimizedQueries, ResponseContext responseContext, Object prevEtag,boolean reuseSubQueryCache) {
    List<SegmentDescriptor> segments = new ArrayList<>();
    for (Query q : optimizedQueries) {
      segments.addAll(getSegments(q));
    }
    if(segments.isEmpty()){
      return optimizedQueries;
    }
    if (prevEtag != null) {
      if ("all".equals(hitQueryTag)) {//查询语句属于完全匹配
        return Collections.emptyList();
      }
      if(prevEtag instanceof String){
        //todo Etag为了标识重叠区间segment集合是否变化，改造为根据时间分区段格式存储：Map<Interval, String>
        @Nullable
        final String currentEtag = computeResultLevelCachingEtag(query, segments, cacheStrategy,
                                                                 reuseSubQueryCache);
        if (null != currentEtag) {
          responseContext.putEntityTag(currentEtag);
        }
        //查询语句匹配(或包含)，且segment集合没有变化
        if(currentEtag != null && currentEtag.equals(prevEtag)){
          if (hitQueryTag==null) {
            return Collections.emptyList();
          }
        }
      } else if (prevEtag instanceof Map) {
        Map<Interval, String> prevCacheEtagMap = (Map<Interval, String>) prevEtag;
        if(hitIntervalsTag == null){
          hitIntervalsTag = query.getIntervals();
        }
        Map<Interval,List<SegmentDescriptor>> coverSegments = getOverlapSegments(segments,hitIntervalsTag);
        //todo Etag为了标识重叠区间segment集合是否变化，改造为根据时间分区段格式存储：Map<Interval, String>
        @Nullable
        final Map<Interval,String> currentEtagMap = computePartialResultLevelCachingEtag(query, coverSegments);
        if (null != currentEtagMap) {
          String currentEtag = CacheUtil.computePartialHitEtag(currentEtagMap);
          responseContext.putPartialEntityTag(currentEtag);
        }
        //查询语句匹配，且重叠区间的segment集合没有变化
        if(currentEtagMap != null && cacheSegmentsNoChange(currentEtagMap, prevCacheEtagMap, hitIntervalsTag,
                                                           determineSegmentGranularity(segments.get(0))

        )){
          if(hitQueryTag instanceof List){//缓存未命中的区间
            //查询结果部分命中缓存，返回部分结果，并构造剩余子查询
            List<Interval> residualIntervals = (List<Interval>)hitQueryTag;
            //构造剩余子查询
            return getResidualQueries(optimizedQueries, residualIntervals);
          }
        }
      }

    }
    return optimizedQueries;
  }

  public static Granularity determineSegmentGranularity(SegmentDescriptor descriptor) {
    Interval interval = descriptor.getInterval();
    DateTime start = interval.getStart();
    DateTime end = interval.getEnd();

    // 按从粗到细的顺序检查粒度
    Granularity[] granularities = {
        Granularities.YEAR,
        Granularities.MONTH,
        Granularities.WEEK,
        Granularities.DAY,
        Granularities.HOUR,
        Granularities.MINUTE,
        Granularities.SECOND
    };

    for (Granularity granularity : granularities) {
      DateTime truncatedStart = granularity.bucketStart(start);
      if (!truncatedStart.equals(start)) {
        continue;
      }

      DateTime nextBucketStart = granularity.increment(truncatedStart);
      if (nextBucketStart.equals(end)) {
        return granularity;
      }
    }

    throw new IllegalArgumentException("无法为Interval " + interval + " 找到匹配的分区粒度。");
  }

  /**
   *
   * @param currentEtag
   * @param prevEtag
   * @param hitIntervalsTag 缓存命中的区间
   * @return
   */
  private boolean cacheSegmentsNoChange(Map<Interval, String> currentEtag, Map<Interval, String> prevEtag,
                                        List<Interval> hitIntervalsTag, Granularity segGranularity) {
    //检查所有缓存命中的区间hitIntervalsTag中两个Map中相同key对应相应的tag是否相等。
    for (Interval interval : hitIntervalsTag) {
      //interval按分区granularity划分多个intervals进行比较
      Iterator<Interval> iterator = segGranularity.getIterable(interval).iterator();
      while (iterator.hasNext()) {
        Interval subInterval = iterator.next();
        if(currentEtag.containsKey(subInterval)&& prevEtag.containsKey(subInterval)){
          if (currentEtag.get(subInterval).equals(prevEtag.get(subInterval))) {
            return true;
          }
        }
      }
    }
    return false;
  }

  private Map<Interval, String> computePartialResultLevelCachingEtag(Query query,
                                                                     Map<Interval,List<SegmentDescriptor>> overlapSegments) {
    Map<Interval, String> currentIntervalEtagMap = new HashMap<>();
    String dataSource = query.getDataSource().getTableNames().stream().findFirst().get();
    boolean hasOnlyHistoricalSegments = true;
    for(Map.Entry<Interval,List<SegmentDescriptor>> entry : overlapSegments.entrySet()){
      List<SegmentDescriptor> segments = entry.getValue();
      Hasher hasher = Hashing.sha1().newHasher();
      StringBuilder intervalSegsTagId = new StringBuilder();
      for (SegmentDescriptor seg : segments) {
        //todo 过滤对实时可变segment的缓存
        if (!hasOnlyHistoricalSegments) {
          return null;
        }
        intervalSegsTagId.append(SegmentId.of(dataSource, seg.getInterval(), seg.getVersion(), seg.getPartitionNumber()));
      }
      hasher.putString(intervalSegsTagId,StandardCharsets.UTF_8);
      currentIntervalEtagMap.put(entry.getKey(),StringUtils.encodeBase64String(hasher.hash().asBytes()));
    }
    return currentIntervalEtagMap;
  }

  private Map<Interval,List<SegmentDescriptor>> getOverlapSegments(List<SegmentDescriptor> segments,
                                                      List<Interval> hitIntervalsTag) {
    Map<Interval,List<SegmentDescriptor>> overlapSegments = new HashMap<>();
    for (SegmentDescriptor seg : segments) {
      for (Interval uInterval : hitIntervalsTag) {
        if (uInterval.contains(seg.getInterval())) {
          overlapSegments.computeIfAbsent(seg.getInterval(),I->new ArrayList<>()).add(seg);
        }
      }
    }
    return overlapSegments;
  }

  private List<Query> getResidualQueries(List<Query> optimizedQueries, List<Interval> residualIntervals) {
    List<Query> residualQueries = new ArrayList<>();
    for (Query q : optimizedQueries) {
      List<SegmentDescriptor> residualSegs = getSegments(q).stream().filter(input -> {
        for (Interval uInterval : residualIntervals) {
          if (uInterval.overlaps(input.getInterval())) {
            return true;
          }
        }
        return false;
      }).collect(Collectors.toList());

      Query newQuery = q.withQuerySegmentSpec(new MultipleSpecificSegmentSpec(residualSegs,residualIntervals));
      if(!newQuery.getIntervals().isEmpty()){
        residualQueries.add(newQuery);
      }
    }
    return residualQueries;
  }


  private List<SegmentDescriptor> getSegments(Query query) {
    if (query instanceof GroupByQuery) {
      if(((GroupByQuery) query).getQuerySegmentSpec() instanceof MultipleSpecificSegmentSpec){
        return ((MultipleSpecificSegmentSpec)((GroupByQuery) query).getQuerySegmentSpec()).getDescriptors();
      }
    }else if(query instanceof TopNQuery){
      if(((TopNQuery) query).getQuerySegmentSpec() instanceof MultipleSpecificSegmentSpec){
        return ((MultipleSpecificSegmentSpec)((TopNQuery) query).getQuerySegmentSpec()).getDescriptors();
      }
    }else{
      throw new UnsupportedOperationException("Unsupported query type");
    }
    return Collections.emptyList();
  }

  /**
   * It computes the ETAG which is used by {@link ResultLevelCachingQueryRunner} for
   * result level caches. queryCacheKey can be null if segment level cache is not being used. However, ETAG
   * is still computed since result level cache may still be on.
   */
  @Nullable
  String computeResultLevelCachingEtag(
      Query query,
      final List<SegmentDescriptor> segments,
      CacheStrategy strategy,
      boolean reuseSubQueryCache
  )
  {
    String dataSource = query.getDataSource().getTableNames().stream().findFirst().get();
    Hasher hasher = Hashing.sha1().newHasher();
    boolean hasOnlyHistoricalSegments = true;
    for (SegmentDescriptor seg : segments) {
      hasher.putString(
          SegmentId.of(dataSource, seg.getInterval(), seg.getVersion(), seg.getPartitionNumber()).toString(),
          StandardCharsets.UTF_8);
      // it is important to add the "query interval" as part ETag calculation
      // to have result level cache work correctly for queries with different
      // intervals covering the same set of segments
      hasher.putString(seg.getInterval().toString(), StandardCharsets.UTF_8);
    }
    //todo 过滤对实时可变segment的缓存
    if (!hasOnlyHistoricalSegments) {
      return null;
    }

    // query cache key can be null if segment level caching is disabled
    final byte[] queryCacheKeyFinal = computeSegmentLevelQueryCacheKey(strategy,query);
    if (queryCacheKeyFinal == null) {
      return null;
    }
    if(!reuseSubQueryCache){
      hasher.putBytes(queryCacheKeyFinal);
    }
    String currEtag = StringUtils.encodeBase64String(hasher.hash().asBytes());
    return currEtag;
  }

  /**
   * Adds the cache key prefix for join data sources. Return null if its a join but caching is not supported
   */
  @Nullable
  private byte[] computeSegmentLevelQueryCacheKey(CacheStrategy strategy,Query query)
  {
    Preconditions.checkNotNull(strategy, "strategy cannot be null");
    byte[] dataSourceCacheKey = query.getDataSource().getCacheKey();
    if (null == dataSourceCacheKey) {
      return null;
    } else if (dataSourceCacheKey.length > 0) {
      return Bytes.concat(dataSourceCacheKey, strategy.computeCacheKey(query));
    } else {
      return strategy.computeCacheKey(query);
    }
      }
}
