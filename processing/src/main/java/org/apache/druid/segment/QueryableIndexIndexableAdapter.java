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
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 */
public class QueryableIndexIndexableAdapter implements IndexableAdapter
{
  private final int numRows;
  private final QueryableIndex input;
  private final ImmutableList<String> availableDimensions;
  private final Metadata metadata;
  private final List<String> compareDimensions;
  private final Granularity compareTimeGran;

  public QueryableIndexIndexableAdapter(QueryableIndex input)
  {
    this.input = input;
    numRows = input.getNumRows();
    availableDimensions = ImmutableList.copyOf(input.getAvailableDimensions());
    this.metadata = input.getMetadata();
    this.compareDimensions = null;
    this.compareTimeGran = null;
  }

  public QueryableIndexIndexableAdapter(QueryableIndex input, @Nullable List<String> compareDimensions, @Nullable Granularity compareTimeGran)
  {
    this.input = input;
    numRows = input.getNumRows();
    availableDimensions = ImmutableList.copyOf(input.getAvailableDimensions());
    this.metadata = input.getMetadata();
    this.compareDimensions = compareDimensions;
    this.compareTimeGran = compareTimeGran;
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
    return new RowIteratorImpl(input, numRows, getMetricNames(), compareTimeGran, compareDimensions);
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
