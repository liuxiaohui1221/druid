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

package org.apache.druid.segment;

import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.CloseQuietly;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.selector.settable.SettableColumnValueSelector;
import org.apache.druid.segment.selector.settable.SettableLongColumnValueSelector;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * On {@link #moveToNext()} and {@link #mark()}, this class copies all column values into a set of {@link
 * SettableColumnValueSelector} instances. Alternative approach was to save only offset in column and use the same
 * column value selectors as in {@link QueryableIndexStorageAdapter}. The approach with "caching" in {@link
 * SettableColumnValueSelector}s is chosen for two reasons:
 * 1) Avoid re-reading column values from serialized format multiple times (because they are accessed multiple times)
 * For comparison, it's not a factor for {@link QueryableIndexStorageAdapter} because during query processing,
 * column values are usually accessed just once per offset, if aggregator or query runner are written sanely.
 * Avoiding re-reads is especially important for object columns, because object deserialization is potentially
 * expensive.
 * 2) {@link #mark()} is a "lookbehind" style functionality, in compressed columnar format, that would cause
 * repetitive excessive decompressions on the block boundaries. E. g. see {@link
 * org.apache.druid.segment.data.BlockLayoutColumnarDoublesSupplier} and similar classes. Some special support for
 * "lookbehind" could be added to these classes, but it's significant extra complexity.
 */
public class RowIteratorImpl implements TransformableRowIterator
{
  private final Closer closer = Closer.create();
  private final Map<String, BaseColumn> columnCache = new HashMap<>();

  private SimpleAscendingOffset offset;
  private int maxValidOffset;

  private final ColumnValueSelector<?> offsetTimestampSelector;
  private final ColumnValueSelector<?>[] offsetDimensionValueSelectors;
  private final ColumnValueSelector<?>[] offsetMetricSelectors;

  private final SettableLongColumnValueSelector rowTimestampSelector = new SettableLongColumnValueSelector();
  private final SettableColumnValueSelector<?>[] rowDimensionValueSelectors;
  private final SettableColumnValueSelector<?>[] rowMetricSelectors;
  private final RowPointer rowPointer;

  private final SettableLongColumnValueSelector markedTimestampSelector = new SettableLongColumnValueSelector();
  private final SettableColumnValueSelector<?>[] markedDimensionValueSelectors;
  private final SettableColumnValueSelector<?>[] markedMetricSelectors;
  private final TimeAndDimsPointer markedRowPointer;

  boolean first = true;
  int memoizedOffset = -1;

  RowIteratorImpl(QueryableIndex input, int numRows, List<String> metricNames,
                  @Nullable Granularity compareTimeGran, List<String> targetDimensions)
  {
    maxValidOffset = numRows - 1;
    offset = new SimpleAscendingOffset(numRows);
    final ColumnSelectorFactory columnSelectorFactory = new QueryableIndexColumnSelectorFactory(
        input,
        VirtualColumns.EMPTY,
        false,
        closer,
        offset,
        columnCache
    );

    offsetTimestampSelector = columnSelectorFactory.makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME);

    final List<DimensionHandler> dimensionHandlers = new ArrayList<>(
        input.getDimensionHandlers().values().stream()
             .filter(dimensionHandler -> targetDimensions == null
                                         || targetDimensions.contains(dimensionHandler.getDimensionName()))
             .collect(Collectors.toList()));

    offsetDimensionValueSelectors = dimensionHandlers
        .stream()
        .map(DimensionHandler::getDimensionName)
        .map(columnSelectorFactory::makeColumnValueSelector)
        .toArray(ColumnValueSelector[]::new);

    // List<String> metricNames = getMetricNames();
    offsetMetricSelectors =
        metricNames.stream().map(columnSelectorFactory::makeColumnValueSelector).toArray(ColumnValueSelector[]::new);

    rowDimensionValueSelectors = dimensionHandlers
        .stream()
        .map(DimensionHandler::makeNewSettableEncodedValueSelector)
        .toArray(SettableColumnValueSelector[]::new);
    rowMetricSelectors = metricNames
        .stream()
        .map(metric -> input.getColumnHolder(metric).makeNewSettableColumnValueSelector())
        .toArray(SettableColumnValueSelector[]::new);

    rowPointer = new RowPointer(
        rowTimestampSelector,
        rowDimensionValueSelectors,
        dimensionHandlers,
        rowMetricSelectors,
        metricNames,
        offset::getOffset
    );

    markedDimensionValueSelectors = dimensionHandlers
        .stream()
        .map(DimensionHandler::makeNewSettableEncodedValueSelector)
        .toArray(SettableColumnValueSelector[]::new);
    markedMetricSelectors = metricNames
        .stream()
        .map(metric -> input.getColumnHolder(metric).makeNewSettableColumnValueSelector())
        .toArray(SettableColumnValueSelector[]::new);
    markedRowPointer = new TimeAndDimsPointer(
        markedTimestampSelector,
        markedDimensionValueSelectors,
        dimensionHandlers,
        markedMetricSelectors,
        metricNames,
        compareTimeGran
    );
  }

  @Override
  public TimeAndDimsPointer getMarkedPointer()
  {
    return markedRowPointer;
  }

  /**
   * When a segment is produced using "rollup", each row is guaranteed to have different dimensions, so this method
   * could be optimized to have just "return true;" body.
   * TODO record in the segment metadata if each row has different dims or not, to be able to apply this optimization.
   */
  @Override
  public boolean hasTimeAndDimsChangedSinceMark()
  {
    return markedRowPointer.compareTo(rowPointer) != 0;
  }

  @Override
  public void close()
  {
    CloseQuietly.close(closer);
  }

  @Override
  public RowPointer getPointer()
  {
    return rowPointer;
  }

  @Override
  public boolean moveToNext()
  {
    if (first) {
      first = false;
      if (offset.withinBounds()) {
        setRowPointerValues();
        return true;
      } else {
        return false;
      }
    } else {
      if (offset.getOffset() < maxValidOffset) {
        offset.increment();
        setRowPointerValues();
        return true;
      } else {
        // Don't update rowPointer's values here, to conform to the RowIterator.getPointer() specification.
        return false;
      }
    }
  }

  private void setRowPointerValues()
  {
    rowTimestampSelector.setValue(offsetTimestampSelector.getLong());
    for (int i = 0; i < offsetDimensionValueSelectors.length; i++) {
      rowDimensionValueSelectors[i].setValueFrom(offsetDimensionValueSelectors[i]);
    }
    for (int i = 0; i < offsetMetricSelectors.length; i++) {
      rowMetricSelectors[i].setValueFrom(offsetMetricSelectors[i]);
    }
  }

  @Override
  public void mark()
  {
    markedTimestampSelector.setValue(rowTimestampSelector.getLong());
    for (int i = 0; i < rowDimensionValueSelectors.length; i++) {
      markedDimensionValueSelectors[i].setValueFrom(rowDimensionValueSelectors[i]);
    }
    for (int i = 0; i < rowMetricSelectors.length; i++) {
      markedMetricSelectors[i].setValueFrom(rowMetricSelectors[i]);
    }
  }

  /**
   * Used in {@link RowFilteringIndexAdapter}
   */
  public void memoizeOffset()
  {
    memoizedOffset = offset.getOffset();
  }

  public void resetToMemoizedOffset()
  {
    offset.setCurrentOffset(memoizedOffset);
    setRowPointerValues();
  }
}