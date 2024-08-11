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

import org.apache.druid.indexing.overlord.DerivativeDataSource;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

public class DerivativeDataSourceManagerTest
{

  @Test
  public void testGroupAndSortedByGranularity()
  {
    String baseds_a = "base01";
    String derivative_a = "hour_ds01";
    String derivative01_a = "two_hour_ds01";
    String derivative02_a = "sixhour_ds01";
    String derivative03_a = "day_ds01";

    String baseds_b = "base02";
    String derivative_b = "hour_ds02";
    String derivative01_b = "day_ds02";
    List<DerivativeDataSource> list = new ArrayList<>();
    list.add(new DerivativeDataSource(derivative_a, baseds_a, Granularities.HOUR));
    list.add(new DerivativeDataSource(derivative01_a, derivative_a, Granularities.TWO_HOUR));
    list.add(new DerivativeDataSource(derivative02_a, derivative01_a, Granularities.SIX_HOUR));
    list.add(new DerivativeDataSource(derivative03_a, derivative02_a, Granularities.DAY));

    list.add(new DerivativeDataSource(derivative_b, baseds_b, Granularities.HOUR));
    list.add(new DerivativeDataSource(derivative01_b, derivative_b, Granularities.DAY));

    DerivativeDataSourceManager derivativeDataSourceManager = new DerivativeDataSourceManager(null, null, null, null);
    ConcurrentHashMap<String, HashMap<String, DerivativeDataSource>> actualMap = derivativeDataSourceManager.groupAndSortedByGranularity(
        list);

    ConcurrentHashMap<String, HashMap<String, DerivativeDataSource>> expectedMap = new ConcurrentHashMap<>();
    expectedMap.computeIfAbsent(
        baseds_a,
        k -> new HashMap<>()
    );
    expectedMap.computeIfAbsent(
        baseds_b,
        k -> new HashMap<>()
    );
    // hour ->,,,-> day
    HashMap<String, DerivativeDataSource> ds_hour = expectedMap.computeIfAbsent(
        derivative_a,
        k -> new HashMap<>()
    );
    ds_hour.put(derivative_a, new DerivativeDataSource(derivative_a, baseds_a, Granularities.HOUR));

    HashMap<String, DerivativeDataSource> ds_twohour = expectedMap.computeIfAbsent(
        derivative01_a,
        k -> new HashMap<>()
    );
    ds_twohour.put(derivative01_a, new DerivativeDataSource(derivative01_a, derivative_a, Granularities.TWO_HOUR));
    ds_twohour.putAll(ds_hour);

    HashMap<String, DerivativeDataSource> ds_sixhour = expectedMap.computeIfAbsent(
        derivative02_a,
        k -> new HashMap<>()
    );
    ds_sixhour.put(derivative02_a, new DerivativeDataSource(derivative02_a, derivative01_a, Granularities.SIX_HOUR));
    ds_sixhour.putAll(ds_twohour);

    HashMap<String, DerivativeDataSource> ds_day = expectedMap.computeIfAbsent(
        derivative03_a,
        k -> new HashMap<>()
    );
    ds_day.put(derivative03_a, new DerivativeDataSource(derivative03_a, derivative02_a, Granularities.DAY));
    ds_day.putAll(ds_sixhour);

    //hour->day
    HashMap<String, DerivativeDataSource> ds_hour01 = expectedMap.computeIfAbsent(
        derivative_b,
        k -> new HashMap<>()
    );
    ds_hour01.put(derivative_b, new DerivativeDataSource(derivative_b, baseds_b, Granularities.HOUR));
    HashMap<String, DerivativeDataSource> ds_day01 = expectedMap.computeIfAbsent(
        derivative01_b,
        k -> new HashMap<>()
    );
    ds_day01.put(derivative01_b, new DerivativeDataSource(derivative01_b, derivative_b, Granularities.DAY));
    ds_day01.putAll(ds_hour01);

    DerivativeDataSourceManager.DERIVATIVES_REF.set(actualMap);
    Assert.assertEquals(true, expectedMap.size() == actualMap.size());
    Assert.assertEquals(expectedMap.entrySet(), actualMap.entrySet());
    Assert.assertEquals(baseds_a, derivativeDataSourceManager.getRootBaseDataSource(derivative_a));
    Assert.assertEquals(baseds_a, derivativeDataSourceManager.getRootBaseDataSource(derivative01_a));
    Assert.assertEquals(baseds_a, derivativeDataSourceManager.getRootBaseDataSource(derivative02_a));
    Assert.assertEquals(baseds_a, derivativeDataSourceManager.getRootBaseDataSource(derivative03_a));
  }

  @Test
  public void testGroupAndSortedByGranularity2()
  {
    String A = "ds1";
    String B = "ds2";
    String C = "ds3";
    String D = "ds4";
    List<DerivativeDataSource> list = new ArrayList<>();
    list.add(new DerivativeDataSource(A, B, Granularities.HOUR));
    list.add(new DerivativeDataSource(B, C, Granularities.DAY));
    list.add(new DerivativeDataSource(C, D, Granularities.DAY));

    DerivativeDataSourceManager derivativeDataSourceManager = new DerivativeDataSourceManager(null, null, null, null);
    ConcurrentHashMap<String, HashMap<String, DerivativeDataSource>> actualMap =
        derivativeDataSourceManager.groupAndSortedByGranularity(
            list);

    DerivativeDataSourceManager.DERIVATIVES_REF.set(actualMap);
    Assert.assertEquals(D, derivativeDataSourceManager.getRootBaseDataSource(A));
    Assert.assertEquals(D, derivativeDataSourceManager.getRootBaseDataSource(B));
    Assert.assertEquals(D, derivativeDataSourceManager.getRootBaseDataSource(C));

    Assert.assertEquals(B, derivativeDataSourceManager.getDirectBaseDataSource(A));
    Assert.assertEquals(C, derivativeDataSourceManager.getDirectBaseDataSource(B));
    Assert.assertEquals(D, derivativeDataSourceManager.getDirectBaseDataSource(C));
  }
}
