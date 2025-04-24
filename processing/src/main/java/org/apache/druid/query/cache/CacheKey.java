package org.apache.druid.query.cache;

import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.filter.DimFilter;
import org.joda.time.Interval;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

public abstract class CacheKey implements Serializable
{
  public String namespace;

  public CacheKey(String namespace) {
    this.namespace = namespace;
  }

  public abstract int length();
}
