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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.CloseQuietly;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ComplexColumn;
import org.apache.druid.segment.column.DictionaryEncodedColumn;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.BitmapValues;
import org.apache.druid.segment.data.CloseableIndexed;
import org.apache.druid.segment.data.ImmutableBitmapValues;
import org.apache.druid.segment.data.IndexedIterable;
import org.apache.druid.segment.selector.settable.SettableColumnValueSelector;
import org.apache.druid.segment.selector.settable.SettableLongColumnValueSelector;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 */
public class QueryableIndexIndexableAdapter implements IndexableAdapter
{
  private final int numRows;
  private final QueryableIndex input;
  private final ImmutableList<String> availableDimensions;
  private final Metadata metadata;
  private final List<String> targetDimensions;

  public QueryableIndexIndexableAdapter(QueryableIndex input)
  {
    this.input = input;
    numRows = input.getNumRows();
    availableDimensions = ImmutableList.copyOf(input.getAvailableDimensions());
    this.metadata = input.getMetadata();
    this.targetDimensions = null;
  }

  public QueryableIndexIndexableAdapter(QueryableIndex input, @Nullable List<String> targetDimensions)
  {
    this.input = input;
    numRows = input.getNumRows();
    availableDimensions = ImmutableList.copyOf(input.getAvailableDimensions());
    this.metadata = input.getMetadata();
    this.targetDimensions = targetDimensions;
  }

  @Override
  public Interval getDataInterval()
  {
    return input.getDataInterval();
  }

  @Override
  public int getNumRows()
  {
    return numRows;
  }

  @Override
  public List<String> getDimensionNames()
  {
    return availableDimensions;
  }

  @Override
  public List<String> getMetricNames()
  {
    final Set<String> columns = Sets.newLinkedHashSet(input.getColumnNames());
    final HashSet<String> dimensions = Sets.newHashSet(getDimensionNames());
    return ImmutableList.copyOf(Sets.difference(columns, dimensions));
  }

  @Nullable
  @Override
  public <T extends Comparable<? super T>> CloseableIndexed<T> getDimValueLookup(String dimension)
  {
    final ColumnHolder columnHolder = input.getColumnHolder(dimension);

    if (columnHolder == null) {
      return null;
    }

    final BaseColumn col = columnHolder.getColumn();

    if (!(col instanceof DictionaryEncodedColumn)) {
      return null;
    }

    @SuppressWarnings("unchecked")
    DictionaryEncodedColumn<T> dict = (DictionaryEncodedColumn<T>) col;

    return new CloseableIndexed<T>()
    {

      @Override
      public int size()
      {
        return dict.getCardinality();
      }

      @Override
      public T get(int index)
      {
        return dict.lookupName(index);
      }

      @Override
      public int indexOf(T value)
      {
        return dict.lookupId(value);
      }

      @Override
      public Iterator<T> iterator()
      {
        return IndexedIterable.create(this).iterator();
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("dict", dict);
      }

      @Override
      public void close() throws IOException
      {
        dict.close();
      }
    };
  }

  @Override
  public RowIteratorImpl getRows()
  {
    return new RowIteratorImpl();
  }

  /**
   * On {@link #moveToNext()} and {@link #mark()}, this class copies all column values into a set of {@link
   * SettableColumnValueSelector} instances. Alternative approach was to save only offset in column and use the same
   * column value selectors as in {@link QueryableIndexStorageAdapter}. The approach with "caching" in {@link
   * SettableColumnValueSelector}s is chosen for two reasons:
   *  1) Avoid re-reading column values from serialized format multiple times (because they are accessed multiple times)
   *     For comparison, it's not a factor for {@link QueryableIndexStorageAdapter} because during query processing,
   *     column values are usually accessed just once per offset, if aggregator or query runner are written sanely.
   *     Avoiding re-reads is especially important for object columns, because object deserialization is potentially
   *     expensive.
   *  2) {@link #mark()} is a "lookbehind" style functionality, in compressed columnar format, that would cause
   *     repetitive excessive decompressions on the block boundaries. E. g. see {@link
   *     org.apache.druid.segment.data.BlockLayoutColumnarDoublesSupplier} and similar classes. Some special support for
   *     "lookbehind" could be added to these classes, but it's significant extra complexity.
   */
  class RowIteratorImpl implements TransformableRowIterator
  {
    private final Closer closer = Closer.create();
    private final Map<String, BaseColumn> columnCache = new HashMap<>();

    private final int maxValidOffset = numRows - 1;

    // private final ColumnValueSelector<?> offsetTimestampSelector;
    // private final ColumnValueSelector<?>[] offsetDimensionValueSelectors;
    // private final ColumnValueSelector<?>[] offsetMetricSelectors;

    private final SettableLongColumnValueSelector rowTimestampSelector = new SettableLongColumnValueSelector();
    private final SettableColumnValueSelector<?>[] rowDimensionValueSelectors;
    private final SettableColumnValueSelector<?>[] rowMetricSelectors;
    private RowPointer rowPointer;
    private final SimpleAscendingOffset offset = new SimpleAscendingOffset(numRows);

    private final SettableLongColumnValueSelector markedTimestampSelector = new SettableLongColumnValueSelector();
    private final SettableColumnValueSelector<?>[] markedDimensionValueSelectors;
    private final SettableColumnValueSelector<?>[] markedMetricSelectors;
    private final TimeAndDimsPointer markedRowPointer;

    SimpleAscendingOffset offset1;
    RowPointer rowPointer1;
    SettableLongColumnValueSelector rowTimestampSelector1 = new SettableLongColumnValueSelector();
    SettableColumnValueSelector<?>[] rowDimensionValueSelectors1;
    SettableColumnValueSelector<?>[] rowMetricSelectors1;
    ColumnValueSelector<?> offsetTimestampSelector1;
    ColumnValueSelector<?>[] offsetDimensionValueSelectors1;
    ColumnValueSelector<?>[] offsetMetricSelectors1;

    SimpleAscendingOffset offset2;
    long point2TimestampPrev = 0;
    int offset2Start = numRows;
    RowPointer rowPointer2;
    SettableLongColumnValueSelector rowTimestampSelector2 = new SettableLongColumnValueSelector();
    SettableColumnValueSelector<?>[] rowDimensionValueSelectors2;
    SettableColumnValueSelector<?>[] rowMetricSelectors2;
    ColumnValueSelector<?> offsetTimestampSelector2;
    ColumnValueSelector<?>[] offsetDimensionValueSelectors2;
    ColumnValueSelector<?>[] offsetMetricSelectors2;
    boolean first = true;
    boolean isConsumedOffset1 = false;
    boolean isConsumedOffset2 = false;
    int memoizedOffset = -1;

    RowIteratorImpl()
    {
      // offsetTimestampSelector = columnSelectorFactory.makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME);

      final List<DimensionHandler> dimensionHandlers = new ArrayList<>(
          input.getDimensionHandlers().values().stream()
               .filter(dimensionHandler -> targetDimensions == null
                                           || targetDimensions.contains(dimensionHandler.getDimensionName()))
               .collect(Collectors.toList()));

      // offsetDimensionValueSelectors = dimensionHandlers
      //     .stream()
      //     .map(DimensionHandler::getDimensionName)
      //     .map(columnSelectorFactory::makeColumnValueSelector)
      //     .toArray(ColumnValueSelector[]::new);

      List<String> metricNames = getMetricNames();
      // offsetMetricSelectors =
      //     metricNames.stream().map(columnSelectorFactory::makeColumnValueSelector).toArray(ColumnValueSelector[]::new);

      rowDimensionValueSelectors = dimensionHandlers
          .stream()
          .filter(dimensionHandler -> targetDimensions == null
                                      || targetDimensions.contains(dimensionHandler.getDimensionName()))
          .map(DimensionHandler::makeNewSettableEncodedValueSelector)
          .toArray(SettableColumnValueSelector[]::new);
      rowMetricSelectors = metricNames
          .stream()
          .map(metric -> input.getColumnHolder(metric).makeNewSettableColumnValueSelector())
          .toArray(SettableColumnValueSelector[]::new);
      //多指针中获取最小：min(rowPointer,secondRowPointer)---小顶堆（max elements=60）。
      offset1 = new SimpleAscendingOffset(numRows);

      rowPointer = new RowPointer(
          rowTimestampSelector,
          rowDimensionValueSelectors,
          dimensionHandlers,
          rowMetricSelectors,
          metricNames,
          offset::getOffset,
          targetDimensions == null
      );

      initRowPointer1(dimensionHandlers, metricNames);
      offset2 = new SimpleAscendingOffset(numRows);
      if (targetDimensions != null) {
        initRowPointer2(dimensionHandlers, metricNames);
      }

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
          targetDimensions == null
      );
    }

    private void initRowPointer1(List<DimensionHandler> dimensionHandlers, List<String> metricNames)
    {
      final ColumnSelectorFactory columnSelectorFactory1 = new QueryableIndexColumnSelectorFactory(
          input,
          VirtualColumns.EMPTY,
          false,
          closer,
          offset1,
          columnCache
      );
      offsetTimestampSelector1 = columnSelectorFactory1.makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME);

      offsetDimensionValueSelectors1 = dimensionHandlers
          .stream()
          .map(DimensionHandler::getDimensionName)
          .map(columnSelectorFactory1::makeColumnValueSelector)
          .toArray(ColumnValueSelector[]::new);

      offsetMetricSelectors1 =
          metricNames.stream()
                     .map(columnSelectorFactory1::makeColumnValueSelector)
                     .toArray(ColumnValueSelector[]::new);

      rowDimensionValueSelectors1 = dimensionHandlers
          .stream()
          .filter(dimensionHandler -> targetDimensions == null
                                      || targetDimensions.contains(dimensionHandler.getDimensionName()))
          .map(DimensionHandler::makeNewSettableEncodedValueSelector)
          .toArray(SettableColumnValueSelector[]::new);
      rowMetricSelectors1 = metricNames
          .stream()
          .map(metric -> input.getColumnHolder(metric).makeNewSettableColumnValueSelector())
          .toArray(SettableColumnValueSelector[]::new);

      rowPointer1 = new RowPointer(
          rowTimestampSelector1,
          rowDimensionValueSelectors1,
          dimensionHandlers,
          rowMetricSelectors1,
          metricNames,
          offset1::getOffset,
          targetDimensions == null
      );
      setRowPointerValues1();
    }

    private void initRowPointer2(List<DimensionHandler> dimensionHandlers, List<String> metricNames)
    {
      boolean isFirst = false;
      long firstTime = 0L;
      final ColumnSelectorFactory columnSelectorFactory2 = new QueryableIndexColumnSelectorFactory(
          input,
          VirtualColumns.EMPTY,
          false,
          closer,
          offset2,
          columnCache
      );
      for (; ; ) {
        ColumnValueSelector offsetTimestampSelectorTemp = columnSelectorFactory2.makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME);
        rowTimestampSelector2.setValue(offsetTimestampSelectorTemp.getLong());

        if (!isFirst) {
          isFirst = true;
          firstTime = rowTimestampSelector2.getLong();
        }

        int timestampDiff = Long.compare(rowTimestampSelector2.getLong(), firstTime);
        if (timestampDiff != 0) {
          offsetTimestampSelector2 = columnSelectorFactory2.makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME);

          offsetDimensionValueSelectors2 = dimensionHandlers
              .stream()
              .map(DimensionHandler::getDimensionName)
              .map(columnSelectorFactory2::makeColumnValueSelector)
              .toArray(ColumnValueSelector[]::new);

          offsetMetricSelectors2 =
              metricNames.stream()
                         .map(columnSelectorFactory2::makeColumnValueSelector)
                         .toArray(ColumnValueSelector[]::new);

          rowDimensionValueSelectors2 = dimensionHandlers
              .stream()
              .filter(dimensionHandler -> targetDimensions == null
                                          || targetDimensions.contains(dimensionHandler.getDimensionName()))
              .map(DimensionHandler::makeNewSettableEncodedValueSelector)
              .toArray(SettableColumnValueSelector[]::new);
          rowMetricSelectors2 = metricNames
              .stream()
              .map(metric -> input.getColumnHolder(metric).makeNewSettableColumnValueSelector())
              .toArray(SettableColumnValueSelector[]::new);

          rowPointer2 = new RowPointer(
              rowTimestampSelector2,
              rowDimensionValueSelectors2,
              dimensionHandlers,
              rowMetricSelectors2,
              metricNames,
              offset2::getOffset,
              targetDimensions == null
          );
          setRowPointerValues2();
          offset2Start = offset2.getOffset();
          point2TimestampPrev = rowPointer2.getTimestamp();
          break;
        }
        offset2.increment();
        if (!offset2.withinBounds()) {
          break;
        }
      }
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
      if (targetDimensions != null) {
        if (first) {
          first = false;
          if (offset1.withinBounds()) {
            setRowPointerValues1(); // pointer1设置新offset1执行的行。
            setRowPointerValuesFromPointer1();
            isConsumedOffset1 = true;
            return true;
          } else {
            return false;
          }
        } else {

          //若当前有没消费的且未达到边界则移动指针，使得两个指针都是未消费状态，则继续比较大小
          if (isConsumedOffset1 == true && offset1.getOffset() < offset2Start) {
            offset1.increment();
            if (offset1.withinBounds()) {
              setRowPointerValues1();
            }
          }
          if (isConsumedOffset2 == true
              && rowPointer2.getTimestamp() == point2TimestampPrev
              && offset2.withinBounds()) {
            offset2.increment();
            if (offset2.withinBounds()) {
              setRowPointerValues2();
            }
          }

          if (offset1.getOffset() == offset2Start && !offset2.withinBounds()) {
            return false;
          }
          boolean point1ReachBorder = offset1.getOffset() == offset2Start || !offset1.withinBounds();
          boolean point2ReachBorder = rowPointer2.getTimestamp() != point2TimestampPrev || !offset2.withinBounds();
          if (point1ReachBorder && point2ReachBorder) {
            offset1.setCurrentOffset(offset2.getOffset());
            setRowPointerValues1();
            //查找offset2下个起始位置
            offset2Start = offset2.getOffset();
            point2TimestampPrev = rowPointer2.getTimestamp();
            initRowPointer2(rowPointer2.getDimensionHandlers(), rowPointer2.getMetricNames());
          }

          if (point1ReachBorder && !point2ReachBorder) {
            setRowPointerValuesFromPointer2();
            isConsumedOffset2 = true;
            return true;
          } else if (!point1ReachBorder && point2ReachBorder) {
            setRowPointerValuesFromPointer1();
            isConsumedOffset1 = true;
            return true;
          } else {
            //offset1,offset2中取最小指针（时间+维度比较）
            int rowCompare = rowPointer1.compareTo(rowPointer2);
            if (rowCompare <= 0) { // 赋值较指针的selector到rowPointer
              setRowPointerValuesFromPointer1();
              isConsumedOffset1 = true;
              isConsumedOffset2 = false;
              return true;
            } else {
              setRowPointerValuesFromPointer2();
              isConsumedOffset1 = false;
              isConsumedOffset2 = true;
              return true;
            }
          }
        }
      } else {
        if (first) {
          first = false;
          if (offset1.withinBounds()) {
            setRowPointerValues1(); // pointer1设置新offset1执行的行。
            setRowPointerValuesFromPointer1();
            return true;
          } else {
            return false;
          }
        } else {
          if (offset1.getOffset() < maxValidOffset) {
            offset1.increment();
            setRowPointerValues1(); // pointer1设置新offset1执行的行。
            setRowPointerValuesFromPointer1();
            return true;
          } else {
            // Don't update rowPointer's values here, to conform to the RowIterator.getPointer() specification.
            return false;
          }
        }
      }
    }

    private void setRowPointerValuesFromPointer1()
    {
      // rowPointer.setIndexNum(rowPointer1.getIndexNum());
      offset.setCurrentOffset(offset1.getOffset());
      rowTimestampSelector.setValue(rowTimestampSelector1.getLong());
      for (int i = 0; i < rowDimensionValueSelectors1.length; i++) {
        rowDimensionValueSelectors[i].setValueFrom(rowDimensionValueSelectors1[i]);
      }
      for (int i = 0; i < rowMetricSelectors1.length; i++) {
        rowMetricSelectors[i].setValueFrom(rowMetricSelectors1[i]);
      }
    }

    private void setRowPointerValuesFromPointer2()
    {
      // rowPointer.setIndexNum(rowPointer2.getIndexNum());
      offset.setCurrentOffset(offset2.getOffset());
      rowTimestampSelector.setValue(rowTimestampSelector2.getLong());
      for (int i = 0; i < rowDimensionValueSelectors2.length; i++) {
        rowDimensionValueSelectors[i].setValueFrom(rowDimensionValueSelectors2[i]);
      }
      for (int i = 0; i < rowMetricSelectors2.length; i++) {
        rowMetricSelectors[i].setValueFrom(rowMetricSelectors2[i]);
      }
    }

    private void setRowPointerValues1()
    {
      rowTimestampSelector1.setValue(offsetTimestampSelector1.getLong());
      for (int i = 0; i < offsetDimensionValueSelectors1.length; i++) {
        rowDimensionValueSelectors1[i].setValueFrom(offsetDimensionValueSelectors1[i]);
      }
      for (int i = 0; i < offsetMetricSelectors1.length; i++) {
        rowMetricSelectors1[i].setValueFrom(offsetMetricSelectors1[i]);
      }
    }

    private void setRowPointerValues2()
    {
      rowTimestampSelector2.setValue(offsetTimestampSelector2.getLong());
      for (int i = 0; i < offsetDimensionValueSelectors2.length; i++) {
        rowDimensionValueSelectors2[i].setValueFrom(offsetDimensionValueSelectors2[i]);
      }
      for (int i = 0; i < offsetMetricSelectors2.length; i++) {
        rowMetricSelectors2[i].setValueFrom(offsetMetricSelectors2[i]);
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
    void memoizeOffset()
    {
      memoizedOffset = offset1.getOffset();
    }

    void resetToMemoizedOffset()
    {
      offset1.setCurrentOffset(memoizedOffset);
      setRowPointerValues1();
    }
  }

  @Override
  public String getMetricType(String metric)
  {
    final ColumnHolder columnHolder = input.getColumnHolder(metric);

    final ValueType type = columnHolder.getCapabilities().getType();
    switch (type) {
      case FLOAT:
        return "float";
      case LONG:
        return "long";
      case DOUBLE:
        return "double";
      case COMPLEX: {
        try (ComplexColumn complexColumn = (ComplexColumn) columnHolder.getColumn()) {
          return complexColumn.getTypeName();
        }
      }
      default:
        throw new ISE("Unknown type[%s]", type);
    }
  }

  @Override
  public ColumnCapabilities getCapabilities(String column)
  {
    return input.getColumnHolder(column).getCapabilities();
  }

  @Override
  public BitmapValues getBitmapValues(String dimension, int dictId)
  {
    final ColumnHolder columnHolder = input.getColumnHolder(dimension);
    if (columnHolder == null) {
      return BitmapValues.EMPTY;
    }

    final BitmapIndex bitmaps = columnHolder.getBitmapIndex();
    if (bitmaps == null) {
      return BitmapValues.EMPTY;
    }

    if (dictId >= 0) {
      return new ImmutableBitmapValues(bitmaps.getBitmap(dictId));
    } else {
      return BitmapValues.EMPTY;
    }
  }

  @VisibleForTesting
  BitmapValues getBitmapIndex(String dimension, String value)
  {
    final ColumnHolder columnHolder = input.getColumnHolder(dimension);

    if (columnHolder == null) {
      return BitmapValues.EMPTY;
    }

    final BitmapIndex bitmaps = columnHolder.getBitmapIndex();
    if (bitmaps == null) {
      return BitmapValues.EMPTY;
    }

    return new ImmutableBitmapValues(bitmaps.getBitmap(bitmaps.getIndex(value)));
  }

  @Override
  public Metadata getMetadata()
  {
    return metadata;
  }
}
