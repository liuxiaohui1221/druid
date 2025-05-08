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

import org.apache.commons.collections.comparators.ComparableComparator;
import org.apache.commons.lang3.compare.ComparableUtils;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.materializedview.MaterializedViewUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.cache.CacheKey;
import org.apache.druid.query.cache.SubQueryCacheKey;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.LimitSpec;
import org.joda.time.Interval;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class CacheUtil
{
  private static final byte CACHE_GROUPBY_QUERY = 0x15;
  private static final String ENTRY_SPLIT_OPERATOR = ", ";
  private static final String KV_SPLIT_OPERATOR = "@@";

  public static <T> Pair<HitInfo,SubQueryCacheKey> findParentKey(Cache cache, Query<T> query, String namespace,
                                                                 List<String> subDimensions) {
    List<Interval> queryIntervals = query.getIntervals();
    Granularity queryGranularity = query.getGranularity();
    DimFilter queryFilter = query.getFilter();
    Set<CacheKey> parentKeys = cache.getNamespaceToKeys(namespace);
    long residualRangeLen = Long.MAX_VALUE;
    HitInfo hitInfo=null;
    SubQueryCacheKey bestParentKey = null;
    for(CacheKey parentKey : parentKeys){
      if(parentKey instanceof SubQueryCacheKey){
        SubQueryCacheKey parentSubKey = (SubQueryCacheKey) parentKey;
        //比较粒度
        if (queryGranularity.isFinerThan(parentSubKey.getGranularity())){
          continue;
        }
        //比较维度
        if (!parentSubKey.getDimensions().containsAll(subDimensions)){
          continue;
        }
        //比较指标
        if (!parentSubKey.getAggregators().containsAll(query.getRequiredColumns())){
          continue;
        }
        //比较过滤条件
        if (!isFilterCompatible(parentSubKey.getFilter(), queryFilter)){
          continue;
        }

        //比较limit
        if(!isLimitCompatible(parentSubKey.getLimitSpec(), query)){
          continue;
        }

        //compute hit info
        //比较时间范围
        List<Interval> residual = MaterializedViewUtils.minus(queryIntervals, parentSubKey.getIntervals());
        if(!residual.equals(queryIntervals)){
          //存在重叠区间，选择剩余时间段最短的
          long newResidualRangeLen = computeIntervalLen(residual);
          if((hitInfo==null || !hitInfo.isSubQueryHit) && newResidualRangeLen < residualRangeLen){
            residualRangeLen = newResidualRangeLen;
            hitInfo = new HitInfo();
            hitInfo.residualIntervals = residual;
            hitInfo.hitIntervals = MaterializedViewUtils.minus(queryIntervals, residual);


            hitInfo.isSubQueryHit = true;
            if(parentSubKey.getDimensions().size()!=subDimensions.size()){
              hitInfo.isSubQueryHit = false;
            }
            bestParentKey = parentSubKey;
          }
        }
      }
    }
    if(hitInfo == null){
      return Pair.of(null,null);
    }else{
      return Pair.of(hitInfo,bestParentKey);
    }
  }

  private static <T> boolean isLimitCompatible(LimitSpec limitSpec, Query<T> query) {
    if( limitSpec == null){
      return true;
    }
    if(limitSpec instanceof DefaultLimitSpec){
      if(query instanceof GroupByQuery){
        GroupByQuery groupByQuery=(GroupByQuery)query;
        if(groupByQuery.getLimitSpec() instanceof DefaultLimitSpec){
          DefaultLimitSpec parentLimitSpec = (DefaultLimitSpec) limitSpec;
          DefaultLimitSpec currentLimitSpec = (DefaultLimitSpec) groupByQuery.getLimitSpec();
          if(parentLimitSpec.getLimit()+parentLimitSpec.getOffset() < currentLimitSpec.getLimit()+currentLimitSpec.getOffset()){
            return false;
          }
        }
      }
    }
    return true;
  }

  private static long computeIntervalLen(List<Interval> residual) {
    long len = 0;
    for(Interval interval : residual){
      len += interval.toDurationMillis();
    }
    return len;
  }

  public static String computePartialHitEtag(Map<Interval, String> existingResultSetId) {
    //构造有序Map，根据key排序
    Map<String, String> sortedMap = new TreeMap<>();
    for(Map.Entry<Interval, String> entry : existingResultSetId.entrySet()){
      sortedMap.put(entry.getKey().toString(), entry.getValue());
    }
    StringBuilder etagBuilder = new StringBuilder();
    for (Map.Entry<String, String> entry : sortedMap.entrySet()) {
      etagBuilder.append(entry.getKey().toString()).append(KV_SPLIT_OPERATOR).append(entry.getValue()).append(ENTRY_SPLIT_OPERATOR);
    }
    return etagBuilder.toString();
  }

  public static HashMap<Interval, String> convertPartialHitEtagToMap(String existingResultSetId) {
    if (existingResultSetId == null) {
      return null;
    }
    HashMap<Interval, String> resultMap = new HashMap<>();
    String[] intervalTagId = existingResultSetId.split(ENTRY_SPLIT_OPERATOR);
    for(String intervalTagIdStr : intervalTagId){
      String[] intervalTagIdArr = intervalTagIdStr.split(KV_SPLIT_OPERATOR);
      resultMap.put(Intervals.of(intervalTagIdArr[0]), intervalTagIdArr[1]);
    }
    return resultMap;
  }


  public static class HitInfo{
    public Boolean isSubQueryHit;
    public List<Interval> residualIntervals;
    public List<Interval> hitIntervals;
  }

  // 检查父过滤条件是否被当前查询过滤条件覆盖
  static boolean isFilterCompatible(DimFilter parentFilter, DimFilter subFilter) {
    // 实现逻辑：判断subFilter是否比parentFilter更严格，例如：
    // parentFilter是"dim1='a'"，subFilter是"dim1='a' AND dim2='b'"
    // 需要确保subFilter逻辑蕴含parentFilter（此处需自定义逻辑或使用表达式推导）
    if(parentFilter == null) {
      return true;
    }
    return parentFilter.equals(subFilter); // 简化实现，实际需深度解析Filter结构
  }

  public enum ServerType
  {
    BROKER {
      @Override
      boolean willMergeRunners()
      {
        return false;
      }
    },
    DATA {
      @Override
      boolean willMergeRunners()
      {
        return true;
      }
    };

    /**
     * Same meaning as the "willMergeRunners" parameter to {@link CacheStrategy#isCacheable}.
     */
    abstract boolean willMergeRunners();
  }

  public static Cache.NamedKey computeResultLevelCacheKey(String resultLevelCacheIdentifier)
  {
    return new Cache.NamedKey(resultLevelCacheIdentifier, StringUtils.toUtf8(resultLevelCacheIdentifier));
  }

  public static void populateResultCache(
      Cache cache,
      CacheKey key,
      byte[] resultBytes
  )
  {
    cache.put(key, resultBytes);
  }

  public static Cache.NamedKey computeSegmentCacheKey(
      String segmentId,
      SegmentDescriptor descriptor,
      byte[] queryCacheKey
  )
  {
    final Interval segmentQueryInterval = descriptor.getInterval();
    final byte[] versionBytes = StringUtils.toUtf8(descriptor.getVersion());

    return new Cache.NamedKey(
        segmentId,
        ByteBuffer
            .allocate(16 + versionBytes.length + 4 + queryCacheKey.length)
            .putLong(segmentQueryInterval.getStartMillis())
            .putLong(segmentQueryInterval.getEndMillis())
            .put(versionBytes)
            .putInt(descriptor.getPartitionNumber())
            .put(queryCacheKey)
            .array()
    );
  }


  /**
   * Returns whether the segment-level cache should be checked for a particular query.
   *
   * @param query         the query to check
   * @param cacheStrategy result of {@link QueryToolChest#getCacheStrategy} on this query
   * @param cacheConfig   current active cache config
   * @param serverType    BROKER or DATA
   */
  public static <T> boolean isUseSegmentCache(
      Query<T> query,
      @Nullable CacheStrategy<T, Object, Query<T>> cacheStrategy,
      CacheConfig cacheConfig,
      ServerType serverType
  )
  {
    return cacheConfig.isUseCache()
           && query.context().isUseCache()
           && isQueryCacheable(query, cacheStrategy, cacheConfig, serverType, true);
  }

  /**
   * Returns whether the result-level cache should be populated for a particular query.
   *
   * @param query         the query to check
   * @param cacheStrategy result of {@link QueryToolChest#getCacheStrategy} on this query
   * @param cacheConfig   current active cache config
   * @param serverType    BROKER or DATA
   */
  public static <T> boolean isPopulateSegmentCache(
      Query<T> query,
      @Nullable CacheStrategy<T, Object, Query<T>> cacheStrategy,
      CacheConfig cacheConfig,
      ServerType serverType
  )
  {
    return isQueryCacheable(query, cacheStrategy, cacheConfig, serverType, true)
           && query.context().isPopulateCache()
           && cacheConfig.isPopulateCache();
  }

  public static <T> boolean isEnableSubQueryReuseCache(
      Query<T> query,
      @Nullable CacheStrategy<T, Object, Query<T>> cacheStrategy,
      CacheConfig cacheConfig,
      ServerType serverType
  )
  {
    return isQueryCacheable(query, cacheStrategy, cacheConfig, serverType, true)
           && query.context().isEnableSubQueryReuse()
           && cacheConfig.isEnableSubQueryReuse();
  }

  public static <T> boolean isEnableSubQueryResultCache(
      Query<T> query,
      @Nullable CacheStrategy<T, Object, Query<T>> cacheStrategy,
      CacheConfig cacheConfig,
      ServerType serverType
  )
  {
    return isQueryCacheable(query, cacheStrategy, cacheConfig, serverType, false)
           && query.context().isEnableSubQueryReuse()
           && cacheConfig.isEnableSubQueryReuse();
  }

  /**
   * Returns whether the result-level cache should be checked for a particular query.
   *
   * @param query         the query to check
   * @param cacheStrategy result of {@link QueryToolChest#getCacheStrategy} on this query
   * @param cacheConfig   current active cache config
   * @param serverType    BROKER or DATA
   */
  public static <T> boolean isUseResultCache(
      Query<T> query,
      @Nullable CacheStrategy<T, Object, Query<T>> cacheStrategy,
      CacheConfig cacheConfig,
      ServerType serverType
  )
  {
    return isQueryCacheable(query, cacheStrategy, cacheConfig, serverType, false)
           && query.context().isUseResultLevelCache()
           && cacheConfig.isUseResultLevelCache();
  }

  /**
   * Returns whether the result-level cache should be populated for a particular query.
   *
   * @param query         the query to check
   * @param cacheStrategy result of {@link QueryToolChest#getCacheStrategy} on this query
   * @param cacheConfig   current active cache config
   * @param serverType    BROKER or DATA
   */
  public static <T> boolean isPopulateResultCache(
      Query<T> query,
      @Nullable CacheStrategy<T, Object, Query<T>> cacheStrategy,
      CacheConfig cacheConfig,
      ServerType serverType
  )
  {
    return isQueryCacheable(query, cacheStrategy, cacheConfig, serverType, false)
           && query.context().isPopulateResultLevelCache()
           && cacheConfig.isPopulateResultLevelCache();
  }

  /**
   * Returns whether a particular query is cacheable. Does not check whether we are actually configured to use or
   * populate the cache; that should be done separately.
   *
   * @param query         the query to check
   * @param cacheStrategy result of {@link QueryToolChest#getCacheStrategy} on this query
   * @param cacheConfig   current active cache config
   * @param serverType    BROKER or DATA
   * @param bySegment     segement level or result-level cache
   */
  static <T> boolean isQueryCacheable(
      final Query<T> query,
      @Nullable final CacheStrategy<T, Object, Query<T>> cacheStrategy,
      final CacheConfig cacheConfig,
      final ServerType serverType,
      final boolean bySegment
  )
  {
    return cacheStrategy != null
           && cacheStrategy.isCacheable(query, serverType.willMergeRunners(), bySegment, query.context().isEnableSubQueryReuse())
           && cacheConfig.isQueryCacheable(query)
           && query.getDataSource().isCacheable(serverType == ServerType.BROKER);
  }
}
