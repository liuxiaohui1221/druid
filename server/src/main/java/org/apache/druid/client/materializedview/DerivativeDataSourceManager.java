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

package org.apache.druid.client.materializedview;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.DerivativeDataSource;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.query.Query;
import org.apache.druid.query.TableDataSource;
import org.joda.time.Duration;
import org.skife.jdbi.v2.StatementContext;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Read and store derivatives information from dataSource table frequently.
 * When optimize query, DerivativesManager offers the information about derivatives.
 */
@ManageLifecycle
public class DerivativeDataSourceManager
{
  private static final EmittingLogger log = new EmittingLogger(DerivativeDataSourceManager.class);
  @VisibleForTesting
  public static final AtomicReference<ConcurrentHashMap<String, HashMap<String, DerivativeDataSource>>> DERIVATIVES_REF =
      new AtomicReference<>(new ConcurrentHashMap<>());
  private final MaterializedViewConfig config;
  private final Supplier<MetadataStorageTablesConfig> dbTables;
  private final SQLMetadataConnector connector;
  private final ObjectMapper objectMapper;
  private final Object lock = new Object();

  private boolean started = false;
  private ListeningScheduledExecutorService exec = null;
  private ListenableFuture<?> future = null;

  @Inject
  public DerivativeDataSourceManager(
      MaterializedViewConfig config,
      Supplier<MetadataStorageTablesConfig> dbTables,
      ObjectMapper objectMapper,
      SQLMetadataConnector connector
  )
  {
    this.config = config;
    this.dbTables = dbTables;
    this.objectMapper = objectMapper;
    this.connector = connector;
  }

  @LifecycleStart
  public void start()
  {
    log.info("starting derivatives manager.");
    synchronized (lock) {
      if (started) {
        return;
      }
      exec = MoreExecutors.listeningDecorator(Execs.scheduledSingleThreaded("DerivativeDataSourceManager-Exec-%d"));
      final Duration delay = config.getPollDuration().toStandardDuration();
      future = exec.scheduleWithFixedDelay(
          () -> {
            try {
              updateDerivatives();
            }
            catch (Exception e) {
              log.makeAlert(e, "uncaught exception in derivatives manager updating thread").emit();
            }
          },
          0,
          delay.getMillis(),
          TimeUnit.MILLISECONDS
      );
      started = true;
    }
    log.info("Derivatives manager started.");
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (lock) {
      if (!started) {
        return;
      }
      started = false;
      future.cancel(true);
      future = null;
      DERIVATIVES_REF.set(new ConcurrentHashMap<>());
      exec.shutdownNow();
      exec = null;
    }
  }

  /**
   * 根据dataSource获取所有baseDatasource列表（若baseDatasource也是物化视图，则存在多个）
   *
   * @param datasource
   * @return
   */
  public ImmutableMap<String, DerivativeDataSource> getSubDerivativeDataSources(String datasource)
  {
    return ImmutableMap.copyOf(DERIVATIVES_REF.get().getOrDefault(datasource, new HashMap<>()));
  }

  /**
   * 获取物化视图数据源所对应的最原始的实时数据源名
   *
   * @param datasource 实时数据源
   * @return
   */
  public String getRootBaseDataSource(String datasource)
  {
    ImmutableMap<String, DerivativeDataSource> derivativeDataSources = ImmutableMap.copyOf(DERIVATIVES_REF.get()
                                                                                                          .getOrDefault(
                                                                                                              datasource,
                                                                                                              new HashMap<>()
                                                                                                          ));
    if (!derivativeDataSources.containsKey(datasource)) {
      return datasource;
    }
    String rootBaseDataSource = derivativeDataSources.get(datasource).getBaseDataSource();
    while (derivativeDataSources.containsKey(rootBaseDataSource)) {
      rootBaseDataSource = derivativeDataSources.get(rootBaseDataSource).getBaseDataSource();
    }
    return rootBaseDataSource;
  }

  /**
   * 获取物化视图数据源直接关联的原始baseDataSource
   *
   * @param datasource
   * @return
   */
  public String getDirectBaseDataSource(String datasource)
  {
    ImmutableMap<String, DerivativeDataSource> stringDerivativeDataSourceImmutableMap = ImmutableMap.copyOf(
        DERIVATIVES_REF.get().getOrDefault(datasource, new HashMap<>()));
    if (!stringDerivativeDataSourceImmutableMap.containsKey(datasource)) {
      log.error("WTF? current derivative[%s]'s sub derivative dataSources need sorted by granularity desc and need "
                + "contains itself.", datasource);
      throw new ISE("WTF? current derivative[%s]'s sub derivative dataSources need sorted by granularity desc and "
                    + "need ", datasource);
    }
    return stringDerivativeDataSourceImmutableMap.get(datasource).getBaseDataSource();
  }

  public static ImmutableMap<String, HashMap<String, DerivativeDataSource>> getAllDerivatives()
  {
    return ImmutableMap.copyOf(DERIVATIVES_REF.get());
  }

  public static boolean isMaterializedViewQuery(Query query)
  {
    if (!(query.getDataSource() instanceof TableDataSource)) {
      return false;
    }
    String datasourceName = ((TableDataSource) query.getDataSource()).getName();
    if (DERIVATIVES_REF.get().containsKey(datasourceName)) {
      log.info("current datasource[%s] is materialized view.", datasourceName);
      return true;
    }
    log.info("current datasource[%s] is not materialized view. derivatives size[%s]", datasourceName,
             DERIVATIVES_REF.get().size());
    return false;
  }

  private void updateDerivatives()
  {
    List<Pair<String, DerivativeDataSourceMetadata>> derivativesInDatabase = connector.retryWithHandle(
        handle ->
            handle
                .createQuery(
                    StringUtils.format(
                        "SELECT DISTINCT dataSource,commit_metadata_payload FROM %1$s",
                        dbTables.get().getDataSourceTable()
                    )
                )
                .map((int index, ResultSet r, StatementContext ctx) -> {
                  String datasourceName = r.getString("dataSource");
                  DataSourceMetadata payload = JacksonUtils.readValue(
                      objectMapper,
                      r.getBytes("commit_metadata_payload"),
                      DataSourceMetadata.class
                  );
                  if (!(payload instanceof DerivativeDataSourceMetadata)) {
                    return null;
                  }
                  DerivativeDataSourceMetadata metadata = (DerivativeDataSourceMetadata) payload;
                  return new Pair<>(datasourceName, metadata);
                })
                .list()
    );

    List<DerivativeDataSource> derivativeDataSources =
        derivativesInDatabase.parallelStream().filter(data -> data != null)
                             .map(derivatives -> {
                               String dataSource = derivatives.lhs;
                               DerivativeDataSourceMetadata metadata = derivatives.rhs;
                               String baseDataSource = metadata.getBaseDataSource();
                               log.debug(
                                   "find derivatives: {bases=%s, derivative=%s, granularity=%s}",
                                   baseDataSource, dataSource, metadata.getGranularitySpec()
                               );
                               Set<String> columns = new HashSet<>();
                               columns.addAll(metadata.getDimensions());
                               columns.addAll(metadata.getMetrics());
                               return new DerivativeDataSource(
                                   dataSource,
                                   baseDataSource,
                                   metadata.getGranularitySpec(),
                                   columns
                               );
                             })
                             .collect(Collectors.toList());

    ConcurrentHashMap<String, HashMap<String, DerivativeDataSource>> newDerivatives = groupAndSortedByGranularity(
        derivativeDataSources);
    ConcurrentHashMap<String, HashMap<String, DerivativeDataSource>> current;
    do {
      current = DERIVATIVES_REF.get();
    } while (!DERIVATIVES_REF.compareAndSet(current, newDerivatives));
  }

  @VisibleForTesting
  public ConcurrentHashMap<String, HashMap<String, DerivativeDataSource>> groupAndSortedByGranularity(
      List<DerivativeDataSource> derivativeDataSources
  )
  {
    TreeMap<String, DerivativeDataSource> newDerivatives = new TreeMap<>();
    for (DerivativeDataSource derivative : derivativeDataSources) {
      newDerivatives.put(derivative.getDataSource(), derivative);
    }
    // mvDatasource -> SortedSet<更小粒度的 base or mv datasource>
    ConcurrentHashMap<String, HashMap<String, DerivativeDataSource>> groupDerivativeDataSources = new ConcurrentHashMap<>();
    for (Map.Entry<String, DerivativeDataSource> entry : newDerivatives.entrySet()) {
      HashMap<String, DerivativeDataSource> subGroups = groupDerivativeDataSources
          .computeIfAbsent(entry.getValue().getBaseDataSource(), k -> new HashMap<>());
      HashMap<String, DerivativeDataSource> curGroups = groupDerivativeDataSources
          .computeIfAbsent(entry.getKey(), k -> new HashMap<>());
      curGroups.put(entry.getKey(), entry.getValue());

      DerivativeDataSource subDerivativeDataSource = newDerivatives.get(entry.getValue().getBaseDataSource());
      while (subDerivativeDataSource != null) {
        subGroups.put(subDerivativeDataSource.getDataSource(), subDerivativeDataSource);
        subDerivativeDataSource = newDerivatives.get(subDerivativeDataSource.getBaseDataSource());
      }
      if (subGroups.size() > 0) {
        curGroups.putAll(subGroups);
      }
    }
    return groupDerivativeDataSources;
  }

  public SortedSet<DerivativeDataSource> getCandidateSortedDerivatives(String originBaseDataSource, Set<String> requiredFields,
                                                                       Granularity queryGranularity
  ) {
    SortedSet<DerivativeDataSource> results = new TreeSet<>();

    Set<DerivativeDataSource> allDerivatives = new HashSet<>();
    getAllDerivatives().values().forEach(map->allDerivatives.addAll(map.values()));
    Set<DerivativeDataSource> derivativesWithRequiredFields = new HashSet<>();
    for (DerivativeDataSource derivativeDataSource : allDerivatives) {
      if (derivativeDataSource.getColumns().containsAll(requiredFields)
          && !queryGranularity.isFinerThan(derivativeDataSource.getGranularitySpec().getQueryGranularity())) {
        derivativesWithRequiredFields.add(derivativeDataSource);
      }
    }
    for(DerivativeDataSource derivativeDataSource:derivativesWithRequiredFields){
      if(originBaseDataSource.equals(this.getRootBaseDataSource(derivativeDataSource.getDataSource()))){
        results.add(derivativeDataSource);
      }
    }
    return results;
  }
}
