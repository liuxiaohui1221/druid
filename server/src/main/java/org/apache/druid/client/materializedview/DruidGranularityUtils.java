package org.apache.druid.client.materializedview;

import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;

public class DruidGranularityUtils {

  /**
   * 根据时间跨度动态确定 Granularity
   * @return 对应的 PeriodGranularity 实例
   */
  public static String determineTruncGranularity(Granularity queryGranularity) {
    if (!(queryGranularity instanceof PeriodGranularity)) {
      throw new IllegalArgumentException("仅支持 PeriodGranularity 类型");
    }
    PeriodGranularity periodGranularity = (PeriodGranularity) queryGranularity;
    long durationMillis = periodGranularity.getPeriod().toStandardDuration().getMillis();

    // 判断时间跨度并选择粒度
    if (durationMillis >= 86400000L) { // >= 1天
      return "P1D";
    } else if (durationMillis >= 3600000L) { // >=1小时且 <1天
      return "PT1H";
    } else { // <1小时
      return "PT1M";
    }
  }
}

