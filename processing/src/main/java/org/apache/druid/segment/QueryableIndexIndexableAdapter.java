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
import org.apache.druid.java.util.common.granularity.Granularity;
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
  private final Closer closer = Closer.create();
  private final Map<String, BaseColumn> columnCache = new HashMap<>();
  private final int numRows;
  private final QueryableIndex input;
  private final ImmutableList<String> availableDimensions;
  private final Metadata metadata;
  private final List<String> targetDimensions;
  private final Granularity compareTimeGran;
  private final List<DimensionHandler> dimensionHandlers;
  private SimpleAscendingOffset lastOffset;
  private MergingRangeRowIterator mergingRangeRowIterator;
  private List<RangeRowIteratorImpl> rangeRowIterators = new ArrayList<>();

  public QueryableIndexIndexableAdapter(QueryableIndex input)
  {
    this(input, null, null);
  }

  public QueryableIndexIndexableAdapter(QueryableIndex input, @Nullable List<String> targetDimensions, @Nullable Granularity compareTimeGran)
  {
    this.input = input;
    numRows = input.getNumRows();
    availableDimensions = ImmutableList.copyOf(input.getAvailableDimensions());
    this.metadata = input.getMetadata();
    this.targetDimensions = targetDimensions;
    this.compareTimeGran = compareTimeGran;

    this.lastOffset = new SimpleAscendingOffset(numRows);

    this.dimensionHandlers = new ArrayList<>(
        input.getDimensionHandlers().values().stream()
            .filter(dimensionHandler -> targetDimensions == null
                || targetDimensions.contains(dimensionHandler.getDimensionName()))
            .collect(Collectors.toList()));

    if (compareTimeGran != null) {
      rangeRowIterators = initRangeOffsets(dimensionHandlers, lastOffset.getOffset());
      mergingRangeRowIterator = new MergingRangeRowIterator(rangeRowIterators);
    }
  }

  class MergingRowIterator implements TransformableRowIterator
  {
    private SimpleAscendingOffset offset;
    private final SettableLongColumnValueSelector rowTimestampSelector = new SettableLongColumnValueSelector();
    private final SettableColumnValueSelector<?>[] rowDimensionValueSelectors;
    private final SettableColumnValueSelector<?>[] rowMetricSelectors;
    private final RowPointer rowPointer;

    private final SettableLongColumnValueSelector markedTimestampSelector = new SettableLongColumnValueSelector();
    private final SettableColumnValueSelector<?>[] markedDimensionValueSelectors;
    private final SettableColumnValueSelector<?>[] markedMetricSelectors;
    private final TimeAndDimsPointer markedRowPointer;

    public MergingRowIterator()
    {
      offset = new SimpleAscendingOffset(numRows);

      rowDimensionValueSelectors = dimensionHandlers
          .stream()
          .map(DimensionHandler::makeNewSettableEncodedValueSelector)
          .toArray(SettableColumnValueSelector[]::new);
      rowMetricSelectors = getMetricNames()
          .stream()
          .map(metric -> input.getColumnHolder(metric).makeNewSettableColumnValueSelector())
          .toArray(SettableColumnValueSelector[]::new);

      rowPointer = new RowPointer(
          rowTimestampSelector,
          rowDimensionValueSelectors,
          dimensionHandlers,
          rowMetricSelectors,
          getMetricNames(),
          offset::getOffset,
          compareTimeGran
      );

      markedDimensionValueSelectors = dimensionHandlers
          .stream()
          .map(DimensionHandler::makeNewSettableEncodedValueSelector)
          .toArray(SettableColumnValueSelector[]::new);
      markedMetricSelectors = getMetricNames()
          .stream()
          .map(metric -> input.getColumnHolder(metric).makeNewSettableColumnValueSelector())
          .toArray(SettableColumnValueSelector[]::new);
      markedRowPointer = new TimeAndDimsPointer(
          markedTimestampSelector,
          markedDimensionValueSelectors,
          dimensionHandlers,
          markedMetricSelectors,
          getMetricNames(),
          compareTimeGran
      );
    }

    private void setRowPointerValues(RowPointer currRangeRowPointer)
    {
      rowTimestampSelector.setValue(currRangeRowPointer.timestampSelector.getLong());
      for (int i = 0; i < currRangeRowPointer.dimensionSelectors.length; i++) {
        rowDimensionValueSelectors[i].setValueFrom(currRangeRowPointer.dimensionSelectors[i]);
      }
      for (int i = 0; i < currRangeRowPointer.metricSelectors.length; i++) {
        rowMetricSelectors[i].setValueFrom(currRangeRowPointer.metricSelectors[i]);
      }
      offset.setCurrentOffset(currRangeRowPointer.getRowNum());
    }

    private void setMarkedRowPointerValues()
    {
      markedTimestampSelector.setValue(rowTimestampSelector.getLong());
      for (int i = 0; i < rowDimensionValueSelectors.length; i++) {
        markedDimensionValueSelectors[i].setValueFrom(rowDimensionValueSelectors[i]);
      }
      for (int i = 0; i < rowMetricSelectors.length; i++) {
        markedMetricSelectors[i].setValueFrom(rowMetricSelectors[i]);
      }
    }
    @Override
    public void mark()
    {
      mergingRangeRowIterator.mark();
      setMarkedRowPointerValues();
    }

    @Override
    public boolean hasTimeAndDimsChangedSinceMark()
    {
      // return mergingRangeRowIterator.hasTimeAndDimsChangedSinceMark();
      return markedRowPointer.compareTo(rowPointer) != 0;
    }

    @Override
    public boolean moveToNext()
    {
      boolean hasNext = mergingRangeRowIterator.moveToNext();
      if (hasNext == false) {
        final List<RangeRowIteratorImpl> nextRangeRowIterators = initRangeOffsets(dimensionHandlers, lastOffset.getOffset());
        rangeRowIterators.clear();
        if (nextRangeRowIterators == null || nextRangeRowIterators.size() == 0) {
          return false;
        }
        rangeRowIterators.addAll(nextRangeRowIterators);
        mergingRangeRowIterator.initRangeRowIterators(rangeRowIterators);
      }

      setRowPointerValues(mergingRangeRowIterator.getPointer());
      return true;
    }

    @Override
    public RowPointer getPointer()
    {
      return rowPointer;
      // return mergingRangeRowIterator.getPointer();
    }

    @Override
    public void close()
    {
      CloseQuietly.close(closer);
      mergingRangeRowIterator.close();
    }

    @Override
    public TimeAndDimsPointer getMarkedPointer()
    {
      return markedRowPointer;
      // return mergingRangeRowIterator.getMarkedPointer();
    }
  }

  private List<RangeRowIteratorImpl> initRangeOffsets(List<DimensionHandler> dimensionHandlers, int offsetStart)
  {
    boolean isFirst = false;
    long firstTime = 0;
    long nextRangeTime = 0;
    int nextRangeStartOffset = offsetStart;
    long firstMaterializeGranTime;
    long nextMaterializeGranTime;
    if (offsetStart == numRows - 1) {
      return null;
    }

    final ColumnSelectorFactory columnSelectorFactory = new QueryableIndexColumnSelectorFactory(
        input,
        VirtualColumns.EMPTY,
        false,
        closer,
        lastOffset,
        columnCache
    );

    List<RangeRowIteratorImpl> iterators = new ArrayList<>();
    for (; ; ) {
      ColumnValueSelector offsetTimestampSelectorTemp = columnSelectorFactory.makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME);

      if (!isFirst) {
        isFirst = true;
        nextRangeTime = firstTime = offsetTimestampSelectorTemp.getLong();
      }

      nextMaterializeGranTime = compareTimeGran.bucketStart(offsetTimestampSelectorTemp.getLong());
      firstMaterializeGranTime = compareTimeGran.bucketStart(firstTime);
      int timestampDiff = Long.compare(nextMaterializeGranTime, firstMaterializeGranTime);

      final int compareTimeDiff = Long.compare(offsetTimestampSelectorTemp.getLong(), nextRangeTime);
      if (compareTimeDiff != 0 && lastOffset.getOffset() < numRows - 1) {
        RangeRowIteratorImpl rangeRowIterator = new RangeRowIteratorImpl(input,
                                                                         nextRangeStartOffset,
                                                                         lastOffset.getOffset(),
                                                                         dimensionHandlers, getMetricNames(), compareTimeGran);
        iterators.add(rangeRowIterator);
        nextRangeTime = offsetTimestampSelectorTemp.getLong();
        nextRangeStartOffset = lastOffset.getOffset();
        if (timestampDiff != 0) {
          break;
        }
      } else if (lastOffset.getOffset() == numRows - 1) {
        RangeRowIteratorImpl rangeRowIterator = new RangeRowIteratorImpl(input,
                                                                         nextRangeStartOffset,
                                                                         numRows,
                                                                         dimensionHandlers, getMetricNames(), compareTimeGran);
        iterators.add(rangeRowIterator);
        break;
      }
      lastOffset.increment();
    }
    return iterators;
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
  public TransformableRowIterator getRows()
  {
    if (compareTimeGran == null) {

      return new RowIteratorImpl(input, numRows, getMetricNames(), compareTimeGran, targetDimensions);
    } else {
      return new MergingRowIterator();
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
