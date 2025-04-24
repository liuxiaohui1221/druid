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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.inject.Inject;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.MetricManipulationFn;
import org.apache.druid.query.cache.CacheKey;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.cache.SubQueryCacheKey;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryQueryToolChest;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.StringDimensionDictionary;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.IndexSizeExceededException;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

public class MaterializedViewQueryQueryToolChest extends QueryToolChest
{
  private static final Logger log = LoggerFactory.getLogger(MaterializedViewQueryQueryToolChest.class);
  private final QueryToolChestWarehouse warehouse;
  private MaterializedViewOptimizer optimizer;

  @Inject
  public MaterializedViewQueryQueryToolChest(
      QueryToolChestWarehouse warehouse
  )
  {
    this.warehouse = warehouse;
  }

  @Override
  public QueryRunner mergeResults(QueryRunner runner)
  {
    return new QueryRunner()
    {
      @Override
      public Sequence run(QueryPlus queryPlus, ResponseContext responseContext)
      {
        Query realQuery = getRealQuery(queryPlus.getQuery());
        return warehouse.getToolChest(realQuery).mergeResults(runner).run(queryPlus.withQuery(realQuery), responseContext);
      }
    };
  }

  @Override
  public BinaryOperator createMergeFn(Query query)
  {
    final Query realQuery = getRealQuery(query);
    return warehouse.getToolChest(realQuery).createMergeFn(realQuery);
  }

  @Override
  public Comparator createResultComparator(Query query)
  {
    final Query realQuery = getRealQuery(query);
    return warehouse.getToolChest(realQuery).createResultComparator(realQuery);
  }

  @Override
  public QueryMetrics makeMetrics(Query query)
  {
    Query realQuery = getRealQuery(query);
    return warehouse.getToolChest(realQuery).makeMetrics(realQuery);
  }

  @Override
  public Function makePreComputeManipulatorFn(Query query, MetricManipulationFn fn)
  {
    Query realQuery = getRealQuery(query);
    return warehouse.getToolChest(realQuery).makePreComputeManipulatorFn(realQuery, fn);
  }

  @Override
  public Function makePostComputeManipulatorFn(Query query, MetricManipulationFn fn)
  {
    Query realQuery = getRealQuery(query);
    return warehouse.getToolChest(realQuery).makePostComputeManipulatorFn(realQuery, fn);
  }

  @Override
  public ObjectMapper decorateObjectMapper(final ObjectMapper objectMapper, final Query query)
  {
    Query realQuery = getRealQuery(query);
    return warehouse.getToolChest(realQuery).decorateObjectMapper(objectMapper, realQuery);
  }

  @Override
  public TypeReference getResultTypeReference()
  {
    return null;
  }

  @Override
  public QueryRunner preMergeQueryDecoration(final QueryRunner runner)
  {
    return new QueryRunner()
    {
      @Override
      public Sequence run(QueryPlus queryPlus, ResponseContext responseContext)
      {
        Query realQuery = getRealQuery(queryPlus.getQuery());
        QueryToolChest realQueryToolChest = warehouse.getToolChest(realQuery);
        QueryRunner realQueryRunner = realQueryToolChest.preMergeQueryDecoration(
            new MaterializedViewQueryRunner(runner, optimizer)
        );
        return realQueryRunner.run(queryPlus.withQuery(realQuery), responseContext);
      }
    };
  }

  public Query getRealQuery(Query query)
  {
    if (query instanceof MaterializedViewQuery) {
      optimizer = ((MaterializedViewQuery) query).getOptimizer();
      return ((MaterializedViewQuery) query).getQuery();
    }
    return query;
  }

  @Nullable
  @Override
  public CacheStrategy getCacheStrategy(Query mvquery)
  {

    Query realQuery = getRealQuery(mvquery);
    if (!(realQuery instanceof GroupByQuery)) {
      return null;
    }

    GroupByQuery groupByQuery = (GroupByQuery) realQuery;
    StringDimensionDictionary colDictionary = (StringDimensionDictionary) dataSourceDimDictionaryMap.computeIfAbsent(
        groupByQuery.getDataSource()
             .getTableNames()
             .stream()
             .findFirst()
             .get(),
        k -> new StringDimensionDictionary(false)
    );
    return new CacheStrategy<ResultRow, Object, GroupByQuery>()
    {
      private static final byte CACHE_STRATEGY_VERSION = 0x1;
      private final List<AggregatorFactory> aggs = groupByQuery.getAggregatorSpecs();
      private final List<DimensionSpec> dims = groupByQuery.getDimensions();

      @Override
      public boolean isCacheable(GroupByQuery query, boolean willMergeRunners, boolean bySegment, boolean enableSubQueryReuse)
      {
        //disable segment-level cache on borker,
        //see PR https://github.com/apache/druid/issues/3820  --fixed
        //return willMergeRunners || !bySegment;
        return enableSubQueryReuse;
      }

      @Override
      public byte[] computeCacheKey(GroupByQuery query)
      {
        CacheKeyBuilder builder = new CacheKeyBuilder(GroupByQueryQueryToolChest.GROUPBY_QUERY)
            .appendByte(CACHE_STRATEGY_VERSION)
            .appendCacheable(query.getGranularity())
            .appendCacheable(query.getDimFilter())
            .appendCacheables(query.getAggregatorSpecs())
            .appendCacheables(query.getDimensions())
            .appendCacheable(query.getVirtualColumns());
        if (query.isApplyLimitPushDown()) {
          builder.appendCacheable(query.getLimitSpec());
        }
        return builder.build();
      }
      @Override
      public CacheKey computeSubQueryCacheKey(String namespace, GroupByQuery query)
      {
        List<String> aggregatorSpecs =
            query.getAggregatorSpecs().stream().map(AggregatorFactory::getName).collect(Collectors.toList());
        List<String> dimensions = query.getDimensions().stream().map(DimensionSpec::getDimension).collect(Collectors.toList());
        String dataSource = query.getDataSource().getTableNames().stream().findFirst().get();
        return new SubQueryCacheKey(namespace, dataSource, query.getIntervals(), query.getFilter(), dimensions, aggregatorSpecs,
                                    query.getGranularity());
      }

      @Override
      public byte[] computeResultLevelCacheKey(GroupByQuery query)
      {
        final CacheKeyBuilder builder = new CacheKeyBuilder(GroupByQueryQueryToolChest.GROUPBY_QUERY)
            .appendByte(CACHE_STRATEGY_VERSION)
            .appendCacheable(query.getGranularity())
            .appendCacheable(query.getDimFilter())
            .appendCacheables(query.getAggregatorSpecs())
            .appendCacheables(query.getDimensions())
            .appendCacheable(query.getVirtualColumns())
            .appendCacheable(query.getHavingSpec())
            .appendCacheable(query.getLimitSpec())
            .appendCacheables(query.getPostAggregatorSpecs());

        if (query.getSubtotalsSpec() != null && !query.getSubtotalsSpec().isEmpty()) {
          for (List<String> subTotalSpec : query.getSubtotalsSpec()) {
            builder.appendStrings(subTotalSpec);
          }
        }
        return builder.build();
      }

      @Override
      public TypeReference<Object> getCacheObjectClazz()
      {
        return GroupByQueryQueryToolChest.OBJECT_TYPE_REFERENCE;
      }

      @Override
      public Function<ResultRow, Object> prepareForCache(boolean isResultLevelCache,
                                                         boolean enableSubDimensionFilterReuse
      )
      {
        final boolean resultRowHasTimestamp = groupByQuery.getResultRowHasTimestamp();
        if(enableSubDimensionFilterReuse){
          return prepareForCacheReuseFunction(resultRowHasTimestamp,isResultLevelCache,groupByQuery);
        }
        return new Function<ResultRow, Object>()
        {
          @Override
          public Object apply(ResultRow resultRow)
          {
            final List<Object> retVal = new ArrayList<>(1 + dims.size() + aggs.size());
            int inPos = 0;
            if (resultRowHasTimestamp) {
              retVal.add(resultRow.getLong(inPos++));
            } else {
              retVal.add(groupByQuery.getUniversalTimestamp().getMillis());
            }

            for (int i = 0; i < dims.size(); i++) {
              retVal.add(resultRow.get(inPos++));
            }
            for (int i = 0; i < aggs.size(); i++) {
              retVal.add(resultRow.get(inPos++));
            }
            if (isResultLevelCache) {
              for (int i = 0; i < groupByQuery.getPostAggregatorSpecs().size(); i++) {
                retVal.add(resultRow.get(inPos++));
              }
            }
            return retVal;
          }
        };
      }

      @Override
      public List<String> extractSubDimensions(Query<ResultRow> query)
      {
        return ((GroupByQuery) query).getDimensions().stream().map(DimensionSpec::getDimension).collect(Collectors.toList());
      }

      private Function<ResultRow, Object> prepareForCacheReuseFunction(
          boolean resultRowHasTimestamp,
          boolean isResultLevelCache, GroupByQuery query) {
        return new Function<ResultRow, Object>()
        {
          @Override
          public Object apply(ResultRow resultRow)
          {
            int size = Math.max(colDictionary.size(),1 + dims.size() + aggs.size());
            final Object[] retVal = new Object[size];
            int inPos = 0;
            if (resultRowHasTimestamp) {
              int newPos=colDictionary.add("__time");
              retVal[newPos]=resultRow.getLong(inPos++);
            } else {
              retVal[0]=query.getUniversalTimestamp().getMillis();
            }

            for (DimensionSpec dim : dims) {
              int newPos=colDictionary.add(Arrays.toString(dim.getCacheKey()));
              retVal[newPos]=resultRow.get(inPos++);
            }
            for (AggregatorFactory agg : aggs) {
              int newPos=colDictionary.add(Arrays.toString(agg.getCacheKey()));
              retVal[newPos]=resultRow.get(inPos++);
            }
            if (isResultLevelCache) {
              for (int i = 0; i < query.getPostAggregatorSpecs().size(); i++) {
                int newPos=colDictionary.add(Arrays.toString(query.getPostAggregatorSpecs()
                                                                  .get(i)
                                                                  .getCacheKey()));
                retVal[newPos]=resultRow.get(inPos++);
              }
            }
            return retVal;
          }
        };
      }

      @Override
      public Function<Object, ResultRow> pullFromCache(boolean isResultLevelCache,
                                                       boolean enableSubDimensionFilterReuse
      )
      {
        final boolean resultRowHasTimestamp = groupByQuery.getResultRowHasTimestamp();
        final int dimensionStart = groupByQuery.getResultRowDimensionStart();
        final int aggregatorStart = groupByQuery.getResultRowAggregatorStart();
        final int postAggregatorStart = groupByQuery.getResultRowPostAggregatorStart();
        if(enableSubDimensionFilterReuse){
          return pullFromCacheReuseFunction(resultRowHasTimestamp,isResultLevelCache,groupByQuery);
        }
        return new Function<Object, ResultRow>()
        {
          private final Granularity granularity = mvquery.getGranularity();

          @Override
          public ResultRow apply(Object input)
          {
            Iterator<Object> results = ((List<Object>) input).iterator();

            DateTime timestamp = granularity.toDateTime(((Number) results.next()).longValue());

            final int size = isResultLevelCache
                             ? groupByQuery.getResultRowSizeWithPostAggregators()
                             : groupByQuery.getResultRowSizeWithoutPostAggregators();

            final ResultRow resultRow = ResultRow.create(size);

            if (resultRowHasTimestamp) {
              resultRow.set(0, timestamp.getMillis());
            }

            final Iterator<DimensionSpec> dimsIter = dims.iterator();
            int dimPos = 0;
            while (dimsIter.hasNext() && results.hasNext()) {
              final DimensionSpec dimensionSpec = dimsIter.next();

              // Must convert generic Jackson-deserialized type into the proper type.
              resultRow.set(
                  dimensionStart + dimPos,
                  DimensionHandlerUtils.convertObjectToType(results.next(), dimensionSpec.getOutputType())
              );

              dimPos++;
            }

            CacheStrategy.fetchAggregatorsFromCache(
                aggs,
                results,
                isResultLevelCache,
                (aggName, aggPosition, aggValueObject) -> {
                  resultRow.set(aggregatorStart + aggPosition, aggValueObject);
                }
            );

            if (isResultLevelCache) {
              for (int postPos = 0; postPos < groupByQuery.getPostAggregatorSpecs().size(); postPos++) {
                if (!results.hasNext()) {
                  throw DruidException.defensive("Ran out of objects while reading postaggs from cache!");
                }
                resultRow.set(postAggregatorStart + postPos, results.next());
              }
            }
            if (dimsIter.hasNext() || results.hasNext()) {
              throw new ISE(
                  "Found left over objects while reading from cache!! dimsIter[%s] results[%s]",
                  dimsIter.hasNext(),
                  results.hasNext()
              );
            }

            return resultRow;
          }
        };
      }

      private Function<Object, ResultRow> pullFromCacheReuseFunction(boolean resultRowHasTimestamp, boolean isResultLevelCache, GroupByQuery query) {
        return new Function<Object, ResultRow>()
        {
          private final Granularity granularity = query.getGranularity();
          final int dimensionStart = query.getResultRowDimensionStart();
          final int aggregatorStart = query.getResultRowAggregatorStart();
          final int postAggregatorStart = query.getResultRowPostAggregatorStart();
          final List<Interval> intervals = query.getIntervals();
          @Override
          public ResultRow apply(Object input)
          {
            List<Object> results = (List<Object>) input;

            DateTime timestamp =
                granularity.toDateTime(((Number) results.get(0)).longValue());
            //判断时间是否在intervals范围内
            boolean flag= false;
            for(Interval interval : intervals) {
              if(interval.getStartMillis()<=timestamp.getMillis() && interval.getEndMillis()>=timestamp.getMillis()){
                flag=true;
                break;
              }
            }
            if(!flag){
              return null;
            }
            final int size = isResultLevelCache
                             ? query.getResultRowSizeWithPostAggregators()
                             : query.getResultRowSizeWithoutPostAggregators();

            final ResultRow resultRow = ResultRow.create(size);

            if (resultRowHasTimestamp) {
              resultRow.set(0, timestamp.getMillis());
            }

            final Iterator<DimensionSpec> dimsIter = dims.iterator();
            int dimPos = 0;
            while (dimsIter.hasNext()) {
              final DimensionSpec dimensionSpec = dimsIter.next();
              // Must convert generic Jackson-deserialized type into the proper type.
              resultRow.set(
                  dimensionStart + dimPos,
                  DimensionHandlerUtils.convertObjectToType(results.get(colDictionary.getId(
                                                                Arrays.toString(dimensionSpec.getCacheKey()))),
                                                            dimensionSpec.getOutputType())
              );
              dimPos++;
            }

            CacheStrategy.fetchAggregatorsFromCache(colDictionary,
                                                    aggs,
                                                    results,
                                                    isResultLevelCache,
                                                    (aggName, aggPosition, aggValueObject) -> {
                                                      resultRow.set(aggregatorStart + aggPosition, aggValueObject);
                                                    }
            );

            if (isResultLevelCache) {
              for (int postPos = 0; postPos < query.getPostAggregatorSpecs().size(); postPos++) {
                resultRow.set(postAggregatorStart + postPos,
                              results.get(colDictionary.getId(Arrays.toString(query.getPostAggregatorSpecs().get(postPos).getCacheKey()))));
              }
            }

            return resultRow;
          }
        };
      }

      @Override
      public Sequence<ResultRow> reAggregateCacheSequence(
          Sequence<ResultRow> originalResult
      )
      {
        long start = System.currentTimeMillis();
        List<String> dimOutputNames = groupByQuery.getDimensions().stream().map(DimensionSpec::getOutputName).collect(Collectors.toList());
        Granularity granularity = groupByQuery.getGranularity();
        final IncrementalIndexSchema incrementalIndexSchema =
            new IncrementalIndexSchema.Builder().withQueryGranularity(granularity)
                                                .withMetrics(groupByQuery.getAggregatorSpecs().toArray(new AggregatorFactory[0]))
                                                .withDimensionsSpec(new DimensionsSpec.Builder()
                                                                        .setDefaultSchemaDimensions(dimOutputNames).build())
                                                .build();
        final IncrementalIndex incrementalIndex =
            new OnheapIncrementalIndex.Builder().setIndexSchema(incrementalIndexSchema).setMaxRowCount(1000000).build();
        originalResult.map(row -> {
          try {
            incrementalIndex.add(row.toMapBasedInputRow(groupByQuery),true);
          }
          catch (IndexSizeExceededException e) {
            log.error("Index size exceeded!!!",e);
            throw new RuntimeException(e);
          }
          return null;
        }).toList();
        log.info("reAggregateCacheSequence cost:{}ms",System.currentTimeMillis()-start);
        return Sequences.simple(incrementalIndex.iterableWithPostAggregations(groupByQuery.getPostAggregatorSpecs(),
                                                                              groupByQuery.isDescending()))
                        .map(row -> ResultRow.fromLegacyRow(row,groupByQuery));
      }
    };
  }
}
