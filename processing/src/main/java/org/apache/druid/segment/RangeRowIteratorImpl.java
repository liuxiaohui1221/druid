package org.apache.druid.segment;

import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.CloseQuietly;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.selector.settable.SettableColumnValueSelector;
import org.apache.druid.segment.selector.settable.SettableLongColumnValueSelector;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RangeRowIteratorImpl implements TransformableRowIterator
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

  RangeRowIteratorImpl(QueryableIndex input, SimpleAscendingOffset offset, int numRows,
                       SettableColumnValueSelector<?>[] rowDimensionValueSelectors2,
                       SettableColumnValueSelector<?>[] rowMetricSelectors2,
                       ColumnValueSelector<?> offsetTimestampSelector2,
                       ColumnValueSelector<?>[] offsetDimensionValueSelectors2,
                       ColumnValueSelector<?>[] offsetMetricSelectors2,
                       List<DimensionHandler> dimensionHandlers, List<String> metricNames,
                       @Nullable Granularity compareTimeGran)
  {
    this.offset = offset;
    maxValidOffset = numRows - 1;
    offsetTimestampSelector = offsetTimestampSelector2;

    offsetDimensionValueSelectors = offsetDimensionValueSelectors2;

    // List<String> metricNames = getMetricNames();
    offsetMetricSelectors = offsetMetricSelectors2;
    rowDimensionValueSelectors = rowDimensionValueSelectors2;
    rowMetricSelectors = rowMetricSelectors2;

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
