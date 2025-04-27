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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import org.apache.druid.guice.annotations.ExtensionPoint;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.cache.CacheKey;
import org.apache.druid.segment.StringDimensionDictionary;
import org.apache.druid.segment.column.ColumnType;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Handles caching-related tasks for a particular query type.
 *
 * Generally returned by the toolchest method {@link QueryToolChest#getCacheStrategy}.
 */
@ExtensionPoint
public interface CacheStrategy<T, CacheType, QueryType extends Query<T>>
{

  /**
   * This method is deprecated and retained for backward incompatibility.
   * Returns whether the given query is cacheable or not.
   * The {@code willMergeRunners} parameter can be used for distinguishing the caller is a broker or a data node.
   *
   * @param ignoredQuery            the query to be cached
   * @param ignoredWillMergeRunners indicates that {@link QueryRunnerFactory#mergeRunners(QueryProcessingPool, Iterable)} will be
   *                                called on the cached by-segment results
   * @param bySegment
   * @param enableSubQueryReuse
   * @return true if the query is cacheable, otherwise false.
   */
  default boolean isCacheable(QueryType ignoredQuery, boolean ignoredWillMergeRunners,
                              boolean bySegment,
                              boolean enableSubQueryReuse
  )
  {
    return false;
  }

  /**
   * Returns whether the given query is cacheable or not.
   * The {@code willMergeRunners} parameter can be used for distinguishing the caller is a broker or a data node.
   *
   * @param query            the query to be cached
   * @param willMergeRunners indicates that {@link QueryRunnerFactory#mergeRunners(QueryProcessingPool, Iterable)} will be
   *                         called on the cached by-segment results
   * @param bySegment        segment level or result level cache
   *
   * @return true if the query is cacheable, otherwise false.
   */
  default boolean isCacheable(QueryType query, boolean willMergeRunners, boolean bySegment)
  {
    return isCacheable(query, willMergeRunners, bySegment, false);
  }

  /**
   * Computes the per-segment cache key for the given query. Because this is a per-segment cache key, it should only
   * include parts of the query that affect the results for a specific segment (i.e., the results returned from
   * {@link QueryRunnerFactory#createRunner}).
   *
   * @param query the query to be cached
   *
   * @return the per-segment cache key
   */
  byte[] computeCacheKey(QueryType query);
  default CacheKey computeSubQueryCacheKey(String namespace, QueryType query){return null;};
  /**
   * Computes the result-level cache key for the given query. The result-level cache will tack on datasource and
   * interval details, so this key does not need to include datasource and interval. But it should include anything
   * else that might affect the results of the query.
   *
   * Some implementations will need to include query parameters that are not used in {@link #computeCacheKey} for the
   * same query.
   *
   * @param query the query to be cached
   *
   * @return the result-level cache key
   */
  byte[] computeResultLevelCacheKey(QueryType query);

  /**
   * Returns the class type of what is used in the cache
   *
   * @return Returns the class type of what is used in the cache
   */
  TypeReference<CacheType> getCacheObjectClazz();

  /**
   * Returns a function that converts from the QueryType's result type to something cacheable.
   * <p>
   * The resulting function must be thread-safe.
   *
   * @param isResultLevelCache            indicates whether the function is invoked for result-level caching or segment-level caching
   * @param enableSubDimensionFilterReuse
   * @return a thread-safe function that converts the QueryType's result type into something cacheable
   */
  Function<T, CacheType> prepareForCache(boolean isResultLevelCache, boolean enableSubDimensionFilterReuse);

  /**
   * A function that does the inverse of the operation that the function prepareForCache returns
   *
   * @param isResultLevelCache            indicates whether the function is invoked for result-level caching or segment-level caching
   * @param enableSubDimensionFilterReuse
   * @return A function that does the inverse of the operation that the function prepareForCache returns
   */
  Function<CacheType, T> pullFromCache(boolean isResultLevelCache, boolean enableSubDimensionFilterReuse);


  default Function<T, CacheType> prepareForSegmentLevelCache(boolean enableSubDimensionFilterReuse)
  {
    return prepareForCache(false, enableSubDimensionFilterReuse);
  }

  default Function<CacheType, T> pullFromSegmentLevelCache(boolean enableSubDimensionFilterReuse)
  {
    return pullFromCache(false, enableSubDimensionFilterReuse);
  }

  /**
   * Helper function used by TopN, GroupBy, Timeseries queries in {@link #pullFromCache(boolean, boolean)}.
   * When using the result level cache, the agg values seen here are
   * finalized values generated by AggregatorFactory.finalizeComputation().
   * These finalized values are deserialized from the cache as generic Objects, which will
   * later be reserialized and returned to the user without further modification.
   * Because the agg values are deserialized as generic Objects, the values are subject to the same
   * type consistency issues handled by DimensionHandlerUtils.convertObjectToType() in the pullFromCache implementations
   * for dimension values (e.g., a Float would become Double).
   */
  static void fetchAggregatorsFromCache(
      List<AggregatorFactory> aggregators,
      Iterator<Object> resultIter,
      boolean isResultLevelCache,
      AddToResultFunction addToResultFunction
  )
  {
    for (int i = 0; i < aggregators.size(); i++) {
      final AggregatorFactory aggregator = aggregators.get(i);
      if (!resultIter.hasNext()) {
        throw new ISE("Ran out of objects while reading aggregators from cache!");
      }

      ColumnType resultType = aggregator.getResultType();
      ColumnType intermediateType = aggregator.getIntermediateType();

      boolean needsDeserialize = !isResultLevelCache || resultType.equals(intermediateType);

      if (needsDeserialize) {
        addToResultFunction.apply(aggregator.getName(), i, aggregator.deserialize(resultIter.next()));
      } else {
        addToResultFunction.apply(aggregator.getName(), i, resultIter.next());
      }
    }
  }

  static void fetchAggregatorsFromCache(
      StringDimensionDictionary colDictionary,
      List<AggregatorFactory> aggregators,
      List<Object> results,
      boolean isResultLevelCache,
      AddToResultFunction addToResultFunction
  )
  {
    for (int i = 0; i < aggregators.size(); i++) {
      final AggregatorFactory aggregator = aggregators.get(i);
      ColumnType resultType = aggregator.getResultType();
      ColumnType intermediateType = aggregator.getIntermediateType();

      boolean needsDeserialize = !isResultLevelCache || resultType.equals(intermediateType);

      if (needsDeserialize) {
        addToResultFunction.apply(aggregator.getName(), i,
                                  aggregator.deserialize(results.get(colDictionary.getId(Arrays.toString(aggregator.getCacheKey())))));
      } else {
        addToResultFunction.apply(aggregator.getName(), i,
                                  results.get(colDictionary.getId(Arrays.toString(aggregator.getCacheKey()))));
      }
    }
  }

  default Sequence<T> reAggregateCacheSequence(Sequence<T> originalResult
  )
  {
    return this.reAggregateCacheSequence(originalResult, null);
  }
  default Sequence<T> reAggregateCacheSequence(Sequence<T> originalResult,@Nullable List<Interval> hitIntervals)
  {
    return originalResult;
  }

   default List<String> extractSubDimensions(Query<T> query){return null;};

  interface AddToResultFunction
  {
    void apply(String aggregatorName, int aggregatorIndex, Object object);
  }
}
