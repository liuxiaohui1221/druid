package org.apache.druid.client.materializedview;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.DateTime;

import java.sql.Timestamp;
import java.util.Objects;

public class PreQueryTemplateMetadata
{
  private final String interval;
  private final Integer status;
  private final int lifetime;
  private final String insertTime;
  private String mergedLifetime;

  @JsonCreator
  public PreQueryTemplateMetadata(@JsonProperty("status") Integer status,
                                  @JsonProperty("interval") String interval,
                                  @JsonProperty("lifetime") int lifetime,
                                  @JsonProperty("inserttime") String insertTime)
  {
    this.interval = interval;
    this.status = status;
    this.lifetime = lifetime;
    this.insertTime = insertTime;
  }

  public boolean isValidLifeTime(boolean skipCheck){
    //获取当前小时，如果大于lifetime则已失效
    return IntervalUtils.isValidLifeTime(skipCheck,insertTime,lifetime);
  }

  public String getMergedLifetime()
  {
    return mergedLifetime;
  }

  public void setMergedLifetime(String mergedLifetime)
  {
    this.mergedLifetime = mergedLifetime;
  }

  public String getInterval()
  {
    return interval;
  }

  public Integer getStatus()
  {
    return status;
  }

  public int getLifetime()
  {
    return lifetime;
  }

  public String getInsertTime()
  {
    return insertTime;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PreQueryTemplateMetadata that = (PreQueryTemplateMetadata) o;
    return lifetime == that.lifetime && Objects.equals(interval, that.interval) && Objects.equals(
        status,
        that.status
    );
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(interval, status, lifetime);
  }
}
