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

package org.apache.druid.client.reusecache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.druid.client.cache.CaffeineCacheConfig;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.utils.JvmUtils;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class CaffeineReuseCache implements org.apache.druid.client.cache.Cache<CacheKey, byte[]>
{
  private static final Logger log = new Logger(CaffeineReuseCache.class);
  private static final int FIXED_COST = 8; // Minimum cost in "weight" per entry;
  private static final int MAX_DEFAULT_BYTES = 1024 * 1024 * 1024;
  private static final LZ4Factory LZ4_FACTORY = LZ4Factory.fastestInstance();
  private static final LZ4FastDecompressor LZ4_DECOMPRESSOR = LZ4_FACTORY.fastDecompressor();
  private static final LZ4Compressor LZ4_COMPRESSOR = LZ4_FACTORY.fastCompressor();

  private final Cache<String,CopyOnWriteArrayList<CacheKey>> dimensionToKeys;
  private final Cache<CacheKey, byte[]> cache;
  private final AtomicReference<CacheStats> priorStats = new AtomicReference<>(CacheStats.empty());
  private final CaffeineCacheConfig config;

  public static CaffeineReuseCache create(final CaffeineCacheConfig config)
  {
    return create(config, config.createExecutor());
  }

  // Used in testing
  public static CaffeineReuseCache create(final CaffeineCacheConfig config, final Executor executor)
  {
    Caffeine<Object, Object> builder = Caffeine.newBuilder().recordStats();
    if (config.getExpireAfter() >= 0) {
      builder
          .expireAfterAccess(config.getExpireAfter(), TimeUnit.MILLISECONDS);
    }
    if (config.getSizeInBytes() >= 0) {
      builder.maximumWeight(config.getSizeInBytes());
    } else {
      builder.maximumWeight(Math.min(MAX_DEFAULT_BYTES, JvmUtils.getRuntimeInfo().getMaxHeapSizeBytes() / 20));
    }
    builder
        .weigher((CacheKey key, byte[] value) -> value.length
                                                 + key.length()
                                                 + FIXED_COST)
        .executor(executor);
    return new CaffeineReuseCache(builder.build(), config);
  }

  private CaffeineReuseCache(final Cache<CacheKey, byte[]> cache, CaffeineCacheConfig config)
  {
    this.cache = cache;
    this.config = config;
//    Cache<String, List<String>> builder =
//        Caffeine.newBuilder().maximumSize(10_000).build((String key)->new ArrayList<>());
    this.dimensionToKeys =
        Caffeine.newBuilder().maximumSize(config.getMaxDims()).build((String key)->new CopyOnWriteArrayList<>());
  }

  @Override
  public byte[] get(CacheKey key)
  {
    return deserialize(cache.getIfPresent(key));
  }

  @Override
  public void put(CacheKey key, byte[] value)
  {
    log.info("Put cache key: %s", key);
    cache.put(key, serialize(value));
    CopyOnWriteArrayList<CacheKey> ifPresent = dimensionToKeys.getIfPresent(key.namespace);
    if(ifPresent != null) {
      ifPresent.add(key);
    }else{
      CopyOnWriteArrayList<CacheKey> cachedKeys = new CopyOnWriteArrayList<>();
      cachedKeys.add(key);
      dimensionToKeys.put(key.namespace, cachedKeys);
    }
  }

  @Override
  public List<CacheKey> getDimensionToKeys(String namespace)
  {
    CopyOnWriteArrayList<CacheKey> cachedKeys=this.dimensionToKeys.getIfPresent(namespace);
    if(cachedKeys==null){
      return Collections.emptyList();
    }
    return cachedKeys;
  }

  @Override
  public Map<CacheKey, byte[]> getBulk(Iterable<CacheKey> keys)
  {
    // The assumption here is that every value is accessed at least once. Materializing here ensures deserialize is only
    // called *once* per value.
    return ImmutableMap.copyOf(Maps.transformValues(cache.getAllPresent(keys), this::deserialize));
  }

  // This is completely racy with put. Any values missed should be evicted later anyways. So no worries.
  @Override
  public void close(String namespace)
  {
    if (config.isEvictOnClose()) {
      cache.asMap().keySet().removeIf(key -> key.namespace.equals(namespace));
    }
  }

  @Override
  @LifecycleStop
  public void close()
  {
    cache.cleanUp();
  }

  @Override
  public org.apache.druid.client.cache.CacheStats getStats()
  {
    final CacheStats stats = cache.stats();
    final long size = cache
        .policy().eviction()
        .map(eviction -> eviction.isWeighted() ? eviction.weightedSize() : OptionalLong.empty())
        .orElse(OptionalLong.empty()).orElse(-1);
    return new org.apache.druid.client.cache.CacheStats(
        stats.hitCount(),
        stats.missCount(),
        cache.estimatedSize(),
        size,
        stats.evictionCount(),
        0,
        stats.loadFailureCount()
    );
  }

  @Override
  public boolean isLocal()
  {
    return true;
  }

  @Override
  public void doMonitor(ServiceEmitter emitter)
  {
    final CacheStats oldStats = priorStats.get();
    final CacheStats newStats = cache.stats();
    final CacheStats deltaStats = newStats.minus(oldStats);

    final ServiceMetricEvent.Builder builder = ServiceMetricEvent.builder();
    emitter.emit(builder.setMetric("query/cache/caffeine/delta/requests", deltaStats.requestCount()));
    emitter.emit(builder.setMetric("query/cache/caffeine/total/requests", newStats.requestCount()));
    emitter.emit(builder.setMetric("query/cache/caffeine/delta/loadTime", deltaStats.totalLoadTime()));
    emitter.emit(builder.setMetric("query/cache/caffeine/total/loadTime", newStats.totalLoadTime()));
    emitter.emit(builder.setMetric("query/cache/caffeine/delta/evictionBytes", deltaStats.evictionWeight()));
    emitter.emit(builder.setMetric("query/cache/caffeine/total/evictionBytes", newStats.evictionWeight()));
    if (!priorStats.compareAndSet(oldStats, newStats)) {
      // ISE for stack trace
      log.warn(
          new IllegalStateException("Multiple monitors"),
          "Multiple monitors on the same cache causing race conditions and unreliable stats reporting"
      );
    }
  }

  @VisibleForTesting
  Cache<CacheKey, byte[]> getCache()
  {
    return cache;
  }

  private byte[] deserialize(byte[] bytes)
  {
    if (bytes == null) {
      return null;
    }
    final int decompressedLen = ByteBuffer.wrap(bytes).getInt();
    final byte[] out = new byte[decompressedLen];
    LZ4_DECOMPRESSOR.decompress(bytes, Integer.BYTES, out, 0, out.length);
    return out;
  }

  private byte[] serialize(byte[] value)
  {
    final int len = LZ4_COMPRESSOR.maxCompressedLength(value.length);
    final byte[] out = new byte[len];
    final int compressedSize = LZ4_COMPRESSOR.compress(value, 0, value.length, out, 0);
    return ByteBuffer.allocate(compressedSize + Integer.BYTES)
                     .putInt(value.length)
                     .put(out, 0, compressedSize)
                     .array();
  }
  /*private byte[] serialize(CachedResult value)
  {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
         DataOutputStream dos = new DataOutputStream(bos)) {
      // 1. 序列化基础字段
      writeStringList(dos, value.getOriginalDimensions());  // 写入维度列表
      writeStringList(dos, value.getAggregatorNames());     // 写入聚合器名称
      dos.writeLong(value.getInterval().getStartMillis());  // 写入时间范围
      dos.writeLong(value.getInterval().getEndMillis());

      // 2. 序列化分组数据
      dos.writeInt(value.getGroupedData().size());
      for (Map.Entry<DimensionKey, AggregatedValue> entry : value.getGroupedData().entrySet()) {
        // 序列化DimensionKey
        byte[] dimKeyBytes = serializeDimensionKey(entry.getKey());
        dos.writeInt(dimKeyBytes.length);
        dos.write(dimKeyBytes);

        // 序列化AggregatedValue
        // 将对象写入ObjectOutputStream
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(entry.getValue());
        objectOutputStream.close();
        byte[] aggByteArray = byteArrayOutputStream.toByteArray();

        dos.writeInt(aggByteArray.length);
        dos.write(dimKeyBytes);
      }

      // 3. LZ4压缩
      byte[] rawData = bos.toByteArray();
      int maxCompressedSize = LZ4_COMPRESSOR.maxCompressedLength(rawData.length);
      byte[] compressed = new byte[maxCompressedSize];
      int compressedSize = LZ4_COMPRESSOR.compress(rawData, 0, rawData.length, compressed, 0, maxCompressedSize);
      return Arrays.copyOfRange(compressed, 0, compressedSize);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }*/

  // 辅助方法：写入字符串列表
  private static void writeStringList(DataOutputStream dos, List<String> list) throws IOException
  {
    dos.writeInt(list.size());
    for (String s : list) {
      dos.writeUTF(s);
    }
  }

  // 辅助方法：读取字符串列表
  private static List<String> readStringList(DataInputStream dis) throws IOException {
    int size = dis.readInt();
    List<String> list = new ArrayList<>(size);
    for (int i = 0; i < size; ++i) {
      list.add(dis.readUTF());
    }
    return list;
  }

  // 辅助方法：序列化DimensionKey
  private byte[] serializeDimensionKey(DimensionKey key) throws IOException {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
         ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      oos.writeObject(key.getDimensions());
      return bos.toByteArray();
    }
  }
}
