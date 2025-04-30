package org.apache.druid.client.materializedview;

import org.joda.time.Interval;

import java.util.Set;

public class PreQueryTemplateMetadata
{
  private final String templateName;
  private final String tableName;
  private final Interval ingestInterval;
  private final ClientTaskGranularitySpec granularitySpec;
  private final Set<String> dimensions;
  private final Set<String> metrics;
  private final Integer qlStateId;
  private final String prequery;//payload

  public PreQueryTemplateMetadata(
      String templateName,
      String tableName,
      Interval ingestInterval,
      ClientTaskGranularitySpec granularitySpec,
      Set<String> dimensions,
      Set<String> metrics,
      Integer qlStateId,
      String prequery
  )
  {
    this.templateName = templateName;
    this.tableName = tableName;
    this.ingestInterval = ingestInterval;
    this.granularitySpec = granularitySpec;
    this.dimensions = dimensions;
    this.metrics = metrics;
    this.qlStateId = qlStateId;
    this.prequery = prequery;
  }
}
