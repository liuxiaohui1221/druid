package org.apache.druid.client.materializedview;

import com.google.inject.Inject;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.Query;
import org.apache.druid.query.materializedview.MaterializedViewOptimizer;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;

import java.util.Collections;
import java.util.List;

public class RealtimeQueryOptimizer implements MaterializedViewOptimizer
{
  private static final Logger log = new Logger(RealtimeQueryOptimizer.class);
  private final AppenderatorsManager appenderatorsManager;
  @Inject
  public RealtimeQueryOptimizer(AppenderatorsManager appenderatorsManager){
    this.appenderatorsManager = appenderatorsManager;
    log.info("appenderatorsManager: %s",appenderatorsManager);
  }
  @Override
  public List<Query> optimize(Query query)
  {
    log.info("RealtimeQueryOptimizer optimize query: %s",query);
    return Collections.singletonList(query);
  }
}
