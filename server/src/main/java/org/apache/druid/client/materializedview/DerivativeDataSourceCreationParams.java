package org.apache.druid.client.materializedview;

public class DerivativeDataSourceCreationParams
{
  private final String dataSource;
  private final String intervalStr;

  public DerivativeDataSourceCreationParams(String dataSource, String intervalStr) {
    this.dataSource = dataSource;
    this.intervalStr = intervalStr;
  }

  // Getters
  public String getDataSource() { return dataSource; }
  public String getIntervalStr() { return intervalStr; }

  public String getBaseDataSource()
  {
    return null;
  }
}
