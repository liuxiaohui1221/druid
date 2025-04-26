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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Bytes;
import org.apache.druid.client.CacheUtil;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
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
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
    @Nullable
    final String prevEtag = (String) query.getContext().get(QueryResource.HEADER_IF_NONE_MATCH);

    List<Query> optimizedQueries = optimizer.optimize(query);
    long skip=0;
    long limit=Long.MAX_VALUE;
    if(reuseSubQueryCache || cacheConfig.isPopulateResultLevelCache()){
      optimizedQueries = getUncachedQueries(query, hitQueryTag, optimizedQueries, responseContext, prevEtag, reuseSubQueryCache);
      if(optimizedQueries.isEmpty()){{
        return Sequences.empty();
      }}
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

  private List<Query> getUncachedQueries(
      Query query, Object hitQueryTag,
      List<Query> optimizedQueries, ResponseContext responseContext, String prevEtag,boolean reuseSubQueryCache) {
    List<SegmentDescriptor> segments = new ArrayList<>();
    for (Query q : optimizedQueries) {
      segments.addAll(getSegments(q));
    }
    //查询语句完全匹配
    if (prevEtag != null) {
      @Nullable
      final String currentEtag = computeResultLevelCachingEtag(query,segments, cacheStrategy,reuseSubQueryCache);
      if (null != currentEtag) {
        responseContext.putEntityTag(currentEtag);
      }
      //查询语句匹配(或包含,或重叠)，且segment集合没有变化
      if(currentEtag != null && currentEtag.equals(prevEtag)){
        if (hitQueryTag==null || "all".equals(hitQueryTag)) {
          return Collections.emptyList();
        } else if(hitQueryTag instanceof List){
          //查询结果部分命中缓存，返回部分结果，并构造剩余子查询
          List<Interval> residualIntervals = (List<Interval>)hitQueryTag;
          //构造剩余子查询
          return getResidualQueries(optimizedQueries, residualIntervals);
        }
      }
    }
    return optimizedQueries;
  }

  private List<Query> getResidualQueries(List<Query> optimizedQueries, List<Interval> residualIntervals) {
    List<Query> residualQueries = new ArrayList<>();
    for (Query q : optimizedQueries) {
      List<SegmentDescriptor> residualSegs = getSegments(q).stream().filter(input -> {
        for (Interval uInterval : residualIntervals) {
          if (uInterval.overlaps(input.getInterval())) {
            return uInterval.overlaps(input.getInterval());
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
      return ((MultipleSpecificSegmentSpec)((GroupByQuery) query).getQuerySegmentSpec()).getDescriptors();
    }else if(query instanceof TopNQuery){
      return ((MultipleSpecificSegmentSpec)((TopNQuery) query).getQuerySegmentSpec()).getDescriptors();
    }else{
      throw new UnsupportedOperationException("Unsupported query type");
    }
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
