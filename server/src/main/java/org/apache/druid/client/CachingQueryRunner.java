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

package org.apache.druid.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.primitives.Bytes;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulator;
import org.apache.druid.client.reusecache.CacheKey;
import org.apache.druid.client.reusecache.CaffeineReuseCache;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.topn.TopNQuery;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class CachingQueryRunner<T> implements QueryRunner<T>
{
  private static final Logger log = LoggerFactory.getLogger(CachingQueryRunner.class);
  private final String cacheId;
  private final SegmentDescriptor segmentDescriptor;
  private final Interval actualDataInterval;
  private final Optional<byte[]> cacheKeyPrefix;
  private final QueryRunner<T> base;
  private final QueryToolChest toolChest;
  private final Cache<CacheKey,byte[]> cache;
  private final ObjectMapper mapper;
  private final CachePopulator cachePopulator;
  private final CacheConfig cacheConfig;
//  private boolean enableSubQueryReuse;

  public CachingQueryRunner(
      String cacheId,
      Optional<byte[]> cacheKeyPrefix,
      SegmentDescriptor segmentDescriptor,
      Interval actualDataInterval,
      ObjectMapper mapper,
      Cache cache,
      QueryToolChest toolchest,
      QueryRunner<T> base,
      CachePopulator cachePopulator,
      CacheConfig cacheConfig
  )
  {
    this.cacheKeyPrefix = cacheKeyPrefix;
    this.base = base;
    this.cacheId = cacheId;
    this.segmentDescriptor = segmentDescriptor;
    this.actualDataInterval = actualDataInterval;
    this.toolChest = toolchest;
    this.cache = cache;
    this.mapper = mapper;
    this.cachePopulator = cachePopulator;
    this.cacheConfig = cacheConfig;
//    this.enableSubQueryReuse = cacheConfig.isEnableSubQueryReuse();
  }

  @Override
  public Sequence<T> run(QueryPlus<T> queryPlus, ResponseContext responseContext)
  {
    Query<T> query = queryPlus.getQuery();
    final CacheStrategy strategy = toolChest.getCacheStrategy(query);
    final boolean populateCache = canPopulateCache(query, strategy);
    final boolean useCache = canUseCache(query, strategy);
    boolean enableSubQueryReuse = canSubQueryReuseCache(query, strategy);
    CacheKey exactKey = null;
    Cache.NamedKey key = null;
    if(!(cache instanceof CaffeineReuseCache)){
      enableSubQueryReuse = false;
    }
    log.info("populateCache:{},useCache:{},enableSubQueryReuse:{}",populateCache,useCache,enableSubQueryReuse);
    if (enableSubQueryReuse){
      log.info("Query info: {},{}", query.getDataSource(),query.getIntervals());
      exactKey = CacheUtil.computeReuseCacheKey(cacheId,query);
      if (exactKey != null){
        // 1. 尝试完全匹配缓存
        final byte[] exactResult = cache.get(exactKey);
        if (exactResult != null) {
          log.info("Total cache hit for query: {}", query);
          return convertToSequence(strategy,exactResult,enableSubQueryReuse);
        }
        // 2. 查找父维度缓存
        long start_search = System.currentTimeMillis();
        List<String> subDimensions=extractSubDimensions(query);
        CacheKey parentKey = findParentKey(query,subDimensions);
        if (parentKey!= null) {
          log.info("Partial cache hit for query: {},cost:{}ms", query,System.currentTimeMillis()-start_search);
          final byte[] parentResult = cache.get(parentKey);
          if (parentResult != null) {
            Sequence<T> originalResult = convertToSequence(strategy, parentResult,enableSubQueryReuse);
            // 2. 转换为子维度聚合的Sequence
            log.info("Partial cache aggregate by sub dimensions: {}", subDimensions);
            return strategy.reAggregateCacheSequence(originalResult,subDimensions);
          }
        }
      }
    }else{
      if (useCache || populateCache) {
        key = CacheUtil.computeSegmentCacheKey(
            cacheId,
            alignToActualDataInterval(segmentDescriptor),
            Bytes.concat(cacheKeyPrefix.get(), strategy.computeCacheKey(query))
        );
      } else {
        key = null;
      }

      if (useCache) {
        final byte[] cachedResult = cache.get(key);
        if (cachedResult != null) {
          // cache hit
          return convertToSequence(strategy,cachedResult,enableSubQueryReuse);
        }
      }
    }

    if (populateCache) {
      final Function cacheFn = strategy.prepareForSegmentLevelCache(enableSubQueryReuse);
      if(enableSubQueryReuse){
        // 3. 尝试缓存父维度查询结果
        return cachePopulator.wrap(base.run(queryPlus, responseContext), value -> cacheFn.apply(value), cache, exactKey);
      }else {
        return cachePopulator.wrap(base.run(queryPlus, responseContext), value -> cacheFn.apply(value), cache, key);
      }
    } else {
      return base.run(queryPlus, responseContext);
    }
  }

  private CacheKey findParentKey(Query<T> subQuery, List<String> subDims) {
    Interval queryInterval = subQuery.getIntervals().get(0);
    Granularity queryGranularity = subQuery.getGranularity();
    DimFilter queryFilter = subQuery.getFilter();
    List<CacheKey> parentKeys = cache.getDimensionToKeys(cacheId);
    for(CacheKey parentKey : parentKeys){
      //比较粒度
      if (queryGranularity.isFinerThan(parentKey.getGranularity())){
        continue;
      }
      //比较时间范围
      if (!parentKey.getIntervals().get(0).contains(queryInterval)){
        continue;
      }
      //比较维度
      if (!parentKey.getDimensions().containsAll(subDims)){
        continue;
      }
      //比较过滤条件
      if (!isFilterCompatible(parentKey.getFilter(), queryFilter)){
        continue;
      }
      return parentKey;
    }
    return null;
  }

  private List<String> extractSubDimensions(Query<T> subQuery) {
    if(subQuery instanceof GroupByQuery){
      return ((GroupByQuery) subQuery).getDimensions().stream().map(DimensionSpec::getDimension).collect(Collectors.toList());
    }else if(subQuery instanceof TopNQuery){
      return Collections.singletonList(((TopNQuery) subQuery).getDimensionSpec().getDimension());
    }
    return null;
  }

  // 检查父过滤条件是否被当前查询过滤条件覆盖
  boolean isFilterCompatible(DimFilter parentFilter, DimFilter subFilter) {
    // 实现逻辑：判断subFilter是否比parentFilter更严格，例如：
    // parentFilter是"dim1='a'"，subFilter是"dim1='a' AND dim2='b'"
    // 需要确保subFilter逻辑蕴含parentFilter（此处需自定义逻辑或使用表达式推导）
    if(parentFilter == null) {
      return true;
    }
    return parentFilter.equals(subFilter); // 简化实现，实际需深度解析Filter结构
  }

  private Sequence<T> convertToSequence(CacheStrategy strategy, byte[] cachedResult,boolean enableSubQueryReuse) {
    final TypeReference cacheObjectClazz = strategy.getCacheObjectClazz();
    final Function cacheFn = strategy.pullFromSegmentLevelCache(enableSubQueryReuse);
    return Sequences.map(
        new BaseSequence<>(
            new BaseSequence.IteratorMaker<T, Iterator<T>>()
            {
              @Override
              public Iterator<T> make()
              {
                try {
                  if (cachedResult.length == 0) {
                    return Collections.emptyIterator();
                  }

                  return mapper.readValues(
                      mapper.getFactory().createParser(cachedResult),
                      cacheObjectClazz
                  );
                }
                catch (IOException e) {
                  throw new RuntimeException(e);
                }
              }

              @Override
              public void cleanup(Iterator<T> iterFromMake)
              {
              }
            }
        ),
        cacheFn
    ).filter(input-> input != null);
  }

  /**
   * @return whether the segment level cache should be used or not. False if strategy is null
   */
  @VisibleForTesting
  boolean canUseCache(Query<T> query, CacheStrategy strategy)
  {
    return CacheUtil.isUseSegmentCache(
        query,
        strategy,
        cacheConfig,
        CacheUtil.ServerType.DATA
    ) && cacheKeyPrefix.isPresent();
  }

  /**
   * @return whether the segment level cache should be populated or not. False if strategy is null
   */
  @VisibleForTesting
  boolean canPopulateCache(Query<T> query, CacheStrategy strategy)
  {
    return CacheUtil.isPopulateSegmentCache(
        query,
        strategy,
        cacheConfig,
        CacheUtil.ServerType.DATA
    ) && cacheKeyPrefix.isPresent();
  }
  boolean canSubQueryReuseCache(Query<T> query, CacheStrategy strategy)
  {
    return CacheUtil.isEnableSubQueryReuseCache(
        query,
        strategy,
        cacheConfig,
        CacheUtil.ServerType.DATA
    ) && cacheKeyPrefix.isPresent();
  }

  private SegmentDescriptor alignToActualDataInterval(SegmentDescriptor in)
  {
    Interval interval = in.getInterval();
    return new SegmentDescriptor(
        interval.overlaps(actualDataInterval) ? interval.overlap(actualDataInterval) : interval,
        in.getVersion(),
        in.getPartitionNumber()
    );
  }

}
