package org.apache.druid.query.groupby;

import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnCapabilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SubDimensionAccumulator {
  private final Map<Map<String, Object>, Aggregator[]> groups = new HashMap<>();
  private final AggregatorFactory[] aggregatorFactories;
  private final List<String> dims;

  // 为每个聚合器绑定可变列选择器
  private final Map<Aggregator, MutableObjectColumnSelector> selectorMap = new HashMap<>();

  public SubDimensionAccumulator(List<String> dimensions, AggregatorFactory[] aggregatorFactories) {
    this.dims = dimensions;
    this.aggregatorFactories = aggregatorFactories;
  }

  public Aggregator[] getOrCreateAggregators(Map<String, Object> subKey) {
    return groups.computeIfAbsent(subKey, k -> {
      Aggregator[] aggs = new Aggregator[aggregatorFactories.length];
      for (int i = 0; i < aggregatorFactories.length; i++) {
        // 创建可变列选择器并绑定到聚合器
        MutableObjectColumnSelector selector = new MutableObjectColumnSelector();
        ColumnSelectorFactory dummyFactory = createDummyFactory(selector);
        aggs[i] = aggregatorFactories[i].factorize(dummyFactory);
        selectorMap.put(aggs[i], selector);
      }
      return aggs;
    });
  }
  private ColumnSelectorFactory createDummyFactory(MutableObjectColumnSelector selector) {
    return new ColumnSelectorFactory() {
      @Override public DimensionSelector makeDimensionSelector(DimensionSpec dim) { return null; }
      @Override public ColumnValueSelector<?> makeColumnValueSelector(String column) {
        return selector; // 绑定到可变选择器
      }
      @Override
      @Nullable
      public ColumnCapabilities getColumnCapabilities(String column)
      {
        return null;
      }
    };
  }

  public List<ResultRow> toRows(int size, int dimensionStart, int aggregatorStart) {
    List<ResultRow> rows = new ArrayList<>();
    for (Map.Entry<Map<String, Object>, Aggregator[]> entry : groups.entrySet()) {
      final ResultRow resultRow = ResultRow.create(size);
      Map<String, Object> key = entry.getKey();
      Aggregator[] aggregators = entry.getValue();

      resultRow.set(0,key.get("__time"));
      for(int i=0;i<dims.size();i++){
        resultRow.set(dimensionStart+i, key.get(dims.get(i)));
      }
      for (int i=0;i<aggregators.length;i++) {
        resultRow.set(aggregatorStart+i, aggregators[i].get());
      }
      rows.add(resultRow);
    }
    return rows;
  }

  public MutableObjectColumnSelector getSelectorForAggregator(Aggregator aggregator) {
    return selectorMap.get(aggregator);
  }
}