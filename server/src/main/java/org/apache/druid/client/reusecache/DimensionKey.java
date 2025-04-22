package org.apache.druid.client.reusecache;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

// 维度组合键（按维度名称排序序列化）
public class DimensionKey implements Serializable
{
  private final LinkedHashMap<String, String> dimensions;

  public DimensionKey(Map<String, String> dimensions) {
    this.dimensions = new LinkedHashMap<>();
    dimensions.entrySet().stream()
              .sorted(Map.Entry.comparingByKey()) // 确保键顺序一致
              .forEach(e -> this.dimensions.put(e.getKey(), e.getValue()));
  }

  public String getDimensionValue(String dim) {
    return dimensions.get(dim);
  }

  public LinkedHashMap<String, String> getDimensions()
  {
    return dimensions;
  }
}


