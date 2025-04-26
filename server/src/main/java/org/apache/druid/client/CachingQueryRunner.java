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
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.cache.CacheKey;
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
import org.apache.druid.query.cache.SubQueryCacheKey;
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
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
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
    CacheKey key = null;
    if(!(cache instanceof CaffeineReuseCache)){
      enableSubQueryReuse = false;
    }
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
        log.info("Complate match cache total hit for query: {},{}",query.getDataSource(),query.getIntervals());
        return convertToSequence(strategy,cachedResult,false);
      }
    }
    if (enableSubQueryReuse){
      long start1 = System.currentTimeMillis();
      key = strategy.computeSubQueryCacheKey(cacheId,query);
      if (key != null){
        // 1. 尝试完全匹配缓存
        final byte[] exactResult = cache.get(key);
        if (exactResult != null) {
          log.debug("SubQuery total cache hit for query: {},{},cost:{}ms", query.getDataSource(),query.getIntervals()
              ,System.currentTimeMillis()-start1);
          return convertToSequence(strategy,exactResult,enableSubQueryReuse);
        }
        // 2. 查找父维度缓存
        List<String> subDimensions=strategy.extractSubDimensions(query);
        Pair<CacheUtil.HitInfo,SubQueryCacheKey> subQueryCacheKeyPair = CacheUtil.findParentKey(cache, query, cacheId, subDimensions);
        SubQueryCacheKey parentKey = subQueryCacheKeyPair.rhs;
        if (parentKey!= null) {
          final byte[] parentResult = cache.get(parentKey);
          if (parentResult != null) {
            Sequence<T> originalResult = convertToSequence(strategy, parentResult, enableSubQueryReuse);
            // 2. 转换为子维度聚合的Sequence
            log.info("SubQuery cache hit, aggregate by sub dimensions: {},hit dimensions:{},cost:{}ms", subDimensions,
                     parentKey.getDimensions(), System.currentTimeMillis() - start1);
//            return strategy.reAggregateCacheSequence(originalResult,subDimensions);
            return originalResult;
          }
        }
      }
    }

    if (populateCache) {
      final Function cacheFn = strategy.prepareForSegmentLevelCache(enableSubQueryReuse);
      return cachePopulator.wrap(base.run(queryPlus, responseContext), value -> cacheFn.apply(value), cache, key);
    } else {
      return base.run(queryPlus, responseContext);
    }
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
