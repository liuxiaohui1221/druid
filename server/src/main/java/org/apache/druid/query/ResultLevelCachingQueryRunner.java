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

package org.apache.druid.query;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.druid.client.CacheUtil;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.materializedview.MaterializedViewUtils;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.MergeSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.SequenceWrapper;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.cache.CacheKey;
import org.apache.druid.query.cache.SubQueryCacheKey;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupingEngine;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.LimitSpec;
import org.apache.druid.query.materializedview.MaterializedViewQuery;
import org.apache.druid.server.QueryResource;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ResultLevelCachingQueryRunner<T> implements QueryRunner<T>
{
  private static final Logger log = new Logger(ResultLevelCachingQueryRunner.class);
  private static final String PARTIAL_CACHE_KEY = "partial";
  private final QueryRunner baseRunner;
  private ObjectMapper objectMapper;
  private final Cache cache;
  private final CacheConfig cacheConfig;
  private final boolean useResultCache;
  private final boolean reuseSubQueryCache;
  private final boolean populateResultCache;
  private Query<T> query;
  private final CacheStrategy<T, Object, Query<T>> strategy;


  public ResultLevelCachingQueryRunner(
      QueryRunner baseRunner,
      QueryToolChest queryToolChest,
      Query<T> mvquery,
      ObjectMapper objectMapper,
      Cache cache,
      CacheConfig cacheConfig
  )
  {
    this.baseRunner = baseRunner;
    this.objectMapper = objectMapper;
    this.cache = cache;
    this.cacheConfig = cacheConfig;
    if(mvquery instanceof MaterializedViewQuery){
      query = ((MaterializedViewQuery)mvquery).getQuery();
    }else{
      this.query = mvquery;
    }
    this.strategy = queryToolChest.getCacheStrategy(mvquery);
    this.populateResultCache = CacheUtil.isPopulateResultCache(
        query,
        strategy,
        cacheConfig,
        CacheUtil.ServerType.BROKER
    );
    this.useResultCache = CacheUtil.isUseResultCache(query, strategy, cacheConfig, CacheUtil.ServerType.BROKER);
    this.reuseSubQueryCache = CacheUtil.isEnableSubQueryResultCache(query, strategy, cacheConfig,
                                                             CacheUtil.ServerType.BROKER);
  }

  @Override
  public Sequence<T> run(QueryPlus queryPlus, ResponseContext responseContext)
  {
    if (useResultCache || populateResultCache || reuseSubQueryCache) {
      byte[] cachedResultSet;
      CacheKey cacheKey;
      Boolean hitParentKey=null;
      boolean isSubResult=true;
      CacheUtil.HitInfo hitInfo = new CacheUtil.HitInfo();
      cacheKey = strategy.computeSubQueryCacheKey(query.getDataSource().getTableNames().stream().findFirst().get(),
                                                  query);
      if(reuseSubQueryCache && cacheKey!=null){
        List<String> subDimensions=strategy.extractSubDimensions(query);
        cachedResultSet = cache.get(cacheKey);
        if(cachedResultSet == null){
          Pair<CacheUtil.HitInfo,SubQueryCacheKey> subQueryCacheKeyPair = CacheUtil.findParentKey(cache, query,
                                                                                                  query.getDataSource().getTableNames().stream().findFirst().get(),
                                                                                                  subDimensions);
          SubQueryCacheKey parentKey = subQueryCacheKeyPair.rhs;
          hitInfo = subQueryCacheKeyPair.lhs;
          if(parentKey!=null){
            cachedResultSet = cache.get(parentKey);
            if(!subQueryCacheKeyPair.lhs.isSubQueryHit && parentKey.getLimitSpec() instanceof DefaultLimitSpec){
              isSubResult = false;
            }
            hitParentKey=true;
          }
        }else{
          hitParentKey=false;
        }
      }else{
        String cacheKeyStr = StringUtils.fromUtf8(strategy.computeResultLevelCacheKey(query));
        cachedResultSet = fetchResultsFromResultLevelCache(cacheKeyStr);
        cacheKey = CacheUtil.computeResultLevelCacheKey(cacheKeyStr);
      }

      HashMap<String, Object> cacheKeyMap = new HashMap<>();
      int skipResultSetTagLen;
      String existingResultSetId;
      if(reuseSubQueryCache){
        existingResultSetId = extractEtagFromResults(cachedResultSet);
        HashMap<Interval, String> existingPartialHitResultTag =
            CacheUtil.convertPartialHitEtagToMap(existingResultSetId);
        existingPartialHitResultTag = existingPartialHitResultTag == null ? new HashMap<>() : existingPartialHitResultTag;
        cacheKeyMap.put(QueryResource.HEADER_IF_NONE_MATCH, existingPartialHitResultTag);
        skipResultSetTagLen = existingResultSetId==null?0:existingResultSetId.length();
      }else{
        existingResultSetId = extractEtagFromResults(cachedResultSet);
        existingResultSetId = existingResultSetId == null ? "" : existingResultSetId;
        cacheKeyMap.put(QueryResource.HEADER_IF_NONE_MATCH, existingResultSetId);
        skipResultSetTagLen = existingResultSetId.toString().length();
      }

      if(isSubResult && hitParentKey != null){
        if(!hitParentKey || hitInfo.residualIntervals.isEmpty()){
          cacheKeyMap.put(QueryResource.HEADER_CACHE_QUERY_HIT, "all");
        }else{
          //未命中区间与命中区间
          cacheKeyMap.put(QueryResource.HEADER_CACHE_QUERY_HIT, hitInfo.residualIntervals);
          cacheKeyMap.put(QueryResource.HEADER_CACHE_INTERVALS_HIT, hitInfo.hitIntervals);
        }
      }
      query = query.withOverriddenContext(cacheKeyMap);

      Sequence<T> resultFromClient = baseRunner.run(
          QueryPlus.wrap(query),
          responseContext
      );

      String newResultSetId;
      if(reuseSubQueryCache){
        newResultSetId = responseContext.getPartialHitEntityTag();
      }else{
        newResultSetId = responseContext.getEntityTag();
        if (useResultCache && (hitParentKey==null || !hitParentKey) && newResultSetId != null && newResultSetId.equals(existingResultSetId)) {
          log.info("Return cached result set as there is no change in identifiers for query %s ", query.getId());
          // Call accumulate on the sequence to ensure that all Wrapper/Closer/Baggage/etc. get called
          resultFromClient.accumulate(null, (accumulated, in) -> accumulated);
          return deserializeResults(cachedResultSet, strategy, skipResultSetTagLen, hitParentKey, null);
        }
      }

      if(reuseSubQueryCache && cachedResultSet!=null && isSubResult){
        //子查询Query语句包含或部分重叠，结果集完全命中或部分重叠
        // Call accumulate on the sequence to ensure that all Wrapper/Closer/Baggage/etc. get called
        // resultFromClient.accumulate(null, (accumulated, in) -> accumulated);
        List<Sequence<T>> optimizedQueries = new ArrayList<>();
        optimizedQueries.add(resultFromClient);
        optimizedQueries.add(deserializeResults(cachedResultSet, strategy, skipResultSetTagLen, hitParentKey, hitInfo.hitIntervals));
        GroupByQuery groupByQuery=(GroupByQuery)query;
        LimitSpec limitSpec = groupByQuery.getLimitSpec();
        long skip=0;
        long limit=Long.MAX_VALUE;
        if(limitSpec instanceof DefaultLimitSpec){
          DefaultLimitSpec defaultLimitSpec = (DefaultLimitSpec) limitSpec;
          skip=defaultLimitSpec.getOffset();
          limit=defaultLimitSpec.getLimit();
        }
        log.info("Hit SubQuer cachey for query %s, return merged result set",query.getId());
        return new MergeSequence<>(query.getResultOrdering(),Sequences.simple(optimizedQueries)).skip(skip).limit(limit);
      }else {
        @Nullable
        ResultLevelCachePopulator resultLevelCachePopulator = createResultLevelCachePopulator(
            cacheKey,
            newResultSetId
        );
        if (resultLevelCachePopulator == null) {
          return resultFromClient;
        }
        final Function<T, Object> cacheFn = strategy.prepareForCache(true, reuseSubQueryCache);
        return Sequences.wrap(
            Sequences.map(
                resultFromClient,
                new Function<T, T>()
                {
                  @Override
                  public T apply(T input)
                  {
                    if (resultLevelCachePopulator.isShouldPopulate()) {
                      resultLevelCachePopulator.cacheResultEntry(input, cacheFn);
                    }
                    return input;
                  }
                }
            ),
            new SequenceWrapper()
            {
              @Override
              public void after(boolean isDone, Throwable thrown)
              {
                Preconditions.checkNotNull(
                    resultLevelCachePopulator,
                    "ResultLevelCachePopulator cannot be null during cache population"
                );
                if (thrown != null) {
                  log.error(
                      thrown,
                      "Error while preparing for result level caching for query %s with error %s ",
                      query.getId(),
                      thrown.getMessage()
                  );
                } else if (resultLevelCachePopulator.isShouldPopulate()) {
                  // The resultset identifier and its length is cached along with the resultset
                  resultLevelCachePopulator.populateResults();
                  log.info("Cache population complete for query %s", query.getId());
                }
                resultLevelCachePopulator.stopPopulating();
              }
            }
        );
      }
    } else {
      return baseRunner.run(
          queryPlus,
          responseContext
      );
    }
  }

  @Nullable
  private byte[] fetchResultsFromResultLevelCache(
      final String queryCacheKey
  )
  {
    if (useResultCache && queryCacheKey != null) {
      return cache.get(CacheUtil.computeResultLevelCacheKey(queryCacheKey));
    }
    return null;
  }

  private String extractEtagFromResults(
      final byte[] cachedResult
  )
  {
    if (cachedResult == null) {
      return null;
    }
    log.debug("Fetching result level cache identifier for query: %s", query.getId());
    int etagLength = ByteBuffer.wrap(cachedResult, 0, Integer.BYTES).getInt();
    return StringUtils.fromUtf8(Arrays.copyOfRange(cachedResult, Integer.BYTES, etagLength + Integer.BYTES));
  }

  private Sequence<T> deserializeResults(final byte[] cachedResult, CacheStrategy strategy, int skipResultSetTagLen,
                                         Boolean hitPartial, List<Interval> hitIntervals
  )
  {
    if (cachedResult == null) {
      log.error("Cached result set is null");
    }
    final Function<Object, T> pullFromCacheFunction = strategy.pullFromCache(true, reuseSubQueryCache);
    final TypeReference<T> cacheObjectClazz = strategy.getCacheObjectClazz();
    //Skip the resultsetID and its length bytes
    Sequence<T> cachedSequence = Sequences.simple(() -> {
      try {
        int resultOffset = Integer.BYTES + skipResultSetTagLen;
        return objectMapper.readValues(
            objectMapper.getFactory().createParser(
                cachedResult,
                resultOffset,
                cachedResult.length - resultOffset
            ),
            cacheObjectClazz
        );
      }
      catch (IOException e) {
        throw new RE(e, "Failed to retrieve results from cache for query ID [%s]", query.getId());
      }
    });
    Sequence<T> mapSequence = Sequences.map(cachedSequence, pullFromCacheFunction).filter(Objects::nonNull);
    if(reuseSubQueryCache && hitPartial){
      long start = System.currentTimeMillis();
      Sequence<T> aggSequence = strategy.reAggregateCacheSequence(mapSequence, hitIntervals);
      log.info("Reaggregate cache sequence cost: %s ms", System.currentTimeMillis() - start);
      return aggSequence;
    }else{
      return mapSequence;
    }
  }

  private ResultLevelCachePopulator createResultLevelCachePopulator(
      CacheKey cacheKey,
      String resultSetId
  )
  {
    if (resultSetId != null && populateResultCache) {
      ResultLevelCachePopulator resultLevelCachePopulator = new ResultLevelCachePopulator(
          cache,
          objectMapper,
          cacheKey,
          cacheConfig,
          true
      );
      try {
        //   Save the resultSetId and its length
        resultLevelCachePopulator.cacheObjectStream.write(ByteBuffer.allocate(Integer.BYTES)
                                                                    .putInt(resultSetId.length())
                                                                    .array());
        resultLevelCachePopulator.cacheObjectStream.write(StringUtils.toUtf8(resultSetId));
      }
      catch (IOException ioe) {
        log.error(ioe, "Failed to write cached values for query %s", query.getId());
        return null;
      }
      return resultLevelCachePopulator;
    } else {
      return null;
    }
  }

  private class ResultLevelCachePopulator
  {
    private final Cache cache;
    private final ObjectMapper mapper;
    private final SerializerProvider serialiers;
    private final CacheKey key;
    private final CacheConfig cacheConfig;
    @Nullable
    private ByteArrayOutputStream cacheObjectStream;

    private ResultLevelCachePopulator(
        Cache cache,
        ObjectMapper mapper,
        CacheKey key,
        CacheConfig cacheConfig,
        boolean shouldPopulate
    )
    {
      this.cache = cache;
      this.mapper = mapper;
      this.serialiers = mapper.getSerializerProviderInstance();
      this.key = key;
      this.cacheConfig = cacheConfig;
      this.cacheObjectStream = shouldPopulate ? new ByteArrayOutputStream() : null;
    }

    boolean isShouldPopulate()
    {
      return cacheObjectStream != null;
    }

    void stopPopulating()
    {
      cacheObjectStream = null;
    }

    private void cacheResultEntry(
        T resultEntry,
        Function<T, Object> cacheFn
    )
    {
      Preconditions.checkNotNull(cacheObjectStream, "cacheObjectStream");
      int cacheLimit = cacheConfig.getResultLevelCacheLimit();
      try (JsonGenerator gen = mapper.getFactory().createGenerator(cacheObjectStream)) {
        JacksonUtils.writeObjectUsingSerializerProvider(gen, serialiers, cacheFn.apply(resultEntry));
        if (cacheLimit > 0 && cacheObjectStream.size() > cacheLimit) {
          log.info("Result level cache limit exceeded[%s] for query %s, stopping caching", cacheLimit,query.getId());
          stopPopulating();
        }
      }
      catch (IOException ex) {
        log.error(ex, "Failed to retrieve entry to be cached. Result Level caching will not be performed!");
        stopPopulating();
      }
    }

    public void populateResults()
    {
      if(key==null){
        return;
      }
      CacheUtil.populateResultCache(
          cache,
          key,
          Preconditions.checkNotNull(cacheObjectStream, "cacheObjectStream").toByteArray()
      );
    }
  }
}
