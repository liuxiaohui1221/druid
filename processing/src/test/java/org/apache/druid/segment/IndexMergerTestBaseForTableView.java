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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import it.unimi.dsi.fastutil.ints.IntIterator;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionSchema.MultiValueHandling;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.DictionaryEncodedColumn;
import org.apache.druid.segment.column.StringDictionaryEncodedColumn;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.BitmapValues;
import org.apache.druid.segment.data.CompressionFactory;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.ConciseBitmapSerdeFactory;
import org.apache.druid.segment.data.IncrementalIndexTest;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexAdapter;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.File;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IndexMergerTestBaseForTableView extends InitializedNullHandlingTest
{
  public final String basePath = "D:\\test\\merge";
  public final File temporaryFolder = new File(basePath);

  public final ObjectMapper objectMapper = new ObjectMapper();
  protected IndexMerger indexMerger;

  @Parameterized.Parameters(name = "{index}: ")
  public static Collection<Object[]> data()
  {
    return Collections2.transform(
        Sets.cartesianProduct(
            ImmutableList.of(
            )
        ), new Function<List<?>, Object[]>()
        {
          @Nullable
          @Override
          public Object[] apply(List<?> input)
          {
            return input.toArray();
          }
        }
    );
  }

  static IndexSpec makeIndexSpec(
      BitmapSerdeFactory bitmapSerdeFactory,
      CompressionStrategy compressionStrategy,
      CompressionStrategy dimCompressionStrategy,
      CompressionFactory.LongEncodingStrategy longEncodingStrategy
  )
  {
    if (bitmapSerdeFactory != null || compressionStrategy != null) {
      return new IndexSpec(
          bitmapSerdeFactory,
          dimCompressionStrategy,
          compressionStrategy,
          longEncodingStrategy
      );
    } else {
      return new IndexSpec();
    }
  }

  private final IndexSpec indexSpec;
  private final IndexIO indexIO;
  private final boolean useBitmapIndexes;

  @Rule
  public final CloserRule closer = new CloserRule(false);

  protected IndexMergerTestBaseForTableView(
      @Nullable BitmapSerdeFactory bitmapSerdeFactory,
      CompressionStrategy compressionStrategy,
      CompressionStrategy dimCompressionStrategy,
      CompressionFactory.LongEncodingStrategy longEncodingStrategy
  )
  {
    this.indexSpec = makeIndexSpec(
        bitmapSerdeFactory != null ? bitmapSerdeFactory : new ConciseBitmapSerdeFactory(),
        compressionStrategy,
        dimCompressionStrategy,
        longEncodingStrategy
    );
    this.indexIO = TestHelper.getTestIndexIO();
    this.useBitmapIndexes = bitmapSerdeFactory != null;
  }


  @Test
  public void testNonLexicographicDimOrderMerge() throws Exception
  {
    // IncrementalIndex toPersist1 = getIndexD3();
    IncrementalIndex toPersist1 = getIndexD2();

    final File tmpDir = new File(temporaryFolder, "tempDir");
    final File tmpDir2 = new File(temporaryFolder, "tempDir2");
    final File tmpDir3 = new File(temporaryFolder, "tempDir3");
    final File tmpDirMerged = new File(temporaryFolder, "tempDirMerged");
    final File tmpDirMerged2 = new File(temporaryFolder, "tempDirMerged2");

    QueryableIndex index1 = closer.closeLater(
        indexIO.loadIndex(indexMerger.persist(toPersist1, tmpDir, indexSpec, null))
    );

    // QueryableIndex index2 = closer.closeLater(
    //     indexIO.loadIndex(indexMerger.persist(toPersist2, tmpDir2, indexSpec, null))
    // );
    System.out.println("getNumRows1()=" + index1.getNumRows());
    // System.out.println("getNumRows2()=" + index2.getNumRows());
    List<String> targetDimensions = Arrays.asList("d3", "d1");
    final QueryableIndex merged2 = closer.closeLater(
        indexIO.loadIndex(
            indexMerger.mergeQueryableIndex(
                Collections.singletonList(index1),
                true,
                targetDimensions,
                new AggregatorFactory[]{new LongSumAggregatorFactory("sum", "sum")},
                tmpDirMerged2,
                indexSpec,
                null,
                -1,
                true
            )
        )
    );

    // final QueryableIndexIndexableAdapter adapter = new QueryableIndexIndexableAdapter(merged);
    // final List<DebugRow> rowList = RowIteratorHelper.toList(adapter.getRows());

    final QueryableIndexIndexableAdapter adapter2 = new QueryableIndexIndexableAdapter(merged2, null);
    final List<DebugRow> rowList2 = RowIteratorHelper.toList(adapter2.getRows());

    // rowList.forEach(key -> System.out.println(key.dimensions + "," + key.metrics));
    rowList2.forEach(key -> System.out.println(key.dimensions + "," + key.metrics));

    // Assert.assertEquals(Arrays.asList("d3", "d1"), ImmutableList.copyOf(adapter.getDimensionNames()));
    Assert.assertEquals(3, rowList2.size());

    Assert.assertEquals(Arrays.asList("30000", "100"), rowList2.get(0).dimensionValues());
    Assert.assertEquals(Collections.singletonList(2L), rowList2.get(0).metricValues());

    Assert.assertEquals(Arrays.asList("40000", "300"), rowList2.get(1).dimensionValues());
    Assert.assertEquals(Collections.singletonList(1L), rowList2.get(1).metricValues());

    Assert.assertEquals(Arrays.asList("50000", "200"), rowList2.get(2).dimensionValues());
    Assert.assertEquals(Collections.singletonList(1L), rowList2.get(2).metricValues());

    checkBitmapIndex(Collections.emptyList(), adapter2.getBitmapIndex("d3", null));
    checkBitmapIndex(Collections.singletonList(0), adapter2.getBitmapIndex("d3", "30000"));
    checkBitmapIndex(Collections.singletonList(1), adapter2.getBitmapIndex("d3", "40000"));
    checkBitmapIndex(Collections.singletonList(2), adapter2.getBitmapIndex("d3", "50000"));

    checkBitmapIndex(Collections.emptyList(), adapter2.getBitmapIndex("d1", null));
    checkBitmapIndex(Collections.singletonList(0), adapter2.getBitmapIndex("d1", "100"));
    checkBitmapIndex(Collections.singletonList(2), adapter2.getBitmapIndex("d1", "200"));
    checkBitmapIndex(Collections.singletonList(1), adapter2.getBitmapIndex("d1", "300"));

  }

  // @Test
  public void testConvertDifferent() throws Exception
  {
    final long timestamp = System.currentTimeMillis();
    final AggregatorFactory[] aggregators = new AggregatorFactory[]{
        new LongSumAggregatorFactory(
            "longSum1",
            "dim1"
        ),
        new LongSumAggregatorFactory("longSum2", "dim2")
    };

    IncrementalIndex toPersist1 = IncrementalIndexTest.createIndex(aggregators);
    IncrementalIndexTest.populateIndex(timestamp, toPersist1);

    final File tempDir1 = new File(temporaryFolder, "tempDir1");
    final File convertDir = new File(temporaryFolder, "tempDirConvert");
    final IndexableAdapter incrementalAdapter = new IncrementalIndexAdapter(
        toPersist1.getInterval(),
        toPersist1,
        indexSpec.getBitmapSerdeFactory()
                 .getBitmapFactory()
    );

    QueryableIndex index1 = closer.closeLater(
        indexIO.loadIndex(indexMerger.persist(toPersist1, tempDir1, indexSpec, null))
    );


    final IndexableAdapter queryableAdapter = new QueryableIndexIndexableAdapter(index1);

    indexIO.validateTwoSegments(incrementalAdapter, queryableAdapter);

    Assert.assertEquals(2, index1.getColumnHolder(ColumnHolder.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index1.getAvailableDimensions()));
    Assert.assertEquals(4, index1.getColumnNames().size());


    IndexSpec newSpec = new IndexSpec(
        indexSpec.getBitmapSerdeFactory(),
        CompressionStrategy.LZ4.equals(indexSpec.getDimensionCompression()) ?
        CompressionStrategy.LZF :
        CompressionStrategy.LZ4,
        CompressionStrategy.LZ4.equals(indexSpec.getDimensionCompression()) ?
        CompressionStrategy.LZF :
        CompressionStrategy.LZ4,
        CompressionFactory.LongEncodingStrategy.LONGS.equals(indexSpec.getLongEncoding()) ?
        CompressionFactory.LongEncodingStrategy.AUTO :
        CompressionFactory.LongEncodingStrategy.LONGS
    );

    QueryableIndex converted = closer.closeLater(
        indexIO.loadIndex(indexMerger.convert(tempDir1, convertDir, newSpec))
    );

    Assert.assertEquals(2, converted.getColumnHolder(ColumnHolder.TIME_COLUMN_NAME).getLength());
    Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(converted.getAvailableDimensions()));
    Assert.assertEquals(4, converted.getColumnNames().size());

    indexIO.validateTwoSegments(tempDir1, convertDir);

    assertDimCompression(index1, indexSpec.getDimensionCompression());
    assertDimCompression(converted, newSpec.getDimensionCompression());

    Assert.assertArrayEquals(
        getCombiningAggregators(aggregators),
        converted.getMetadata().getAggregators()
    );
  }

  private AggregatorFactory[] getCombiningAggregators(AggregatorFactory[] aggregators)
  {
    AggregatorFactory[] combiningAggregators = new AggregatorFactory[aggregators.length];
    for (int i = 0; i < aggregators.length; i++) {
      combiningAggregators[i] = aggregators[i].getCombiningFactory();
    }
    return combiningAggregators;
  }

  private void assertDimCompression(QueryableIndex index, CompressionStrategy expectedStrategy)
      throws Exception
  {
    // Java voodoo
    if (expectedStrategy == null || expectedStrategy == CompressionStrategy.UNCOMPRESSED) {
      return;
    }

    DictionaryEncodedColumn encodedColumn = (DictionaryEncodedColumn) index.getColumnHolder("dim2").getColumn();
    Object obj;
    if (encodedColumn.hasMultipleValues()) {
      Field field = StringDictionaryEncodedColumn.class.getDeclaredField("multiValueColumn");
      field.setAccessible(true);

      obj = field.get(encodedColumn);
    } else {
      Field field = StringDictionaryEncodedColumn.class.getDeclaredField("column");
      field.setAccessible(true);

      obj = field.get(encodedColumn);
    }
    // CompressedVSizeColumnarIntsSupplier$CompressedByteSizeColumnarInts
    // CompressedVSizeColumnarMultiIntsSupplier$CompressedVSizeColumnarMultiInts
    Field compressedSupplierField = obj.getClass().getDeclaredField("this$0");
    compressedSupplierField.setAccessible(true);

    Object supplier = compressedSupplierField.get(obj);

    Field compressionField = supplier.getClass().getDeclaredField("compression");
    compressionField.setAccessible(true);

    Object strategy = compressionField.get(supplier);

    Assert.assertEquals(expectedStrategy, strategy);
  }

  // @Test
  public void testNoRollupMergeWithDuplicateRow() throws Exception
  {
    // (d3, d6, d8, d9) as actually data from index1 and index2
    // index1 has two duplicate rows
    // index2 has 1 row which is same as index1 row and another different row
    // then we can test
    // 1. incrementalIndex with duplicate rows
    // 2. incrementalIndex without duplicate rows
    // 3. merge 2 indexes with duplicate rows

    IncrementalIndexSchema indexSchema = new IncrementalIndexSchema.Builder()
        .withMetrics(new CountAggregatorFactory("count"))
        .withRollup(false)
        .build();
    IncrementalIndex toPersistA = new OnheapIncrementalIndex.Builder()
        .setIndexSchema(indexSchema)
        .setMaxRowCount(1000)
        .build();

    toPersistA.add(
        new MapBasedInputRow(
            1,
            Arrays.asList("d1", "d2", "d3", "d4", "d5", "d6", "d7", "d8", "d9"),
            ImmutableMap.of(
                "d1", "", "d2", "", "d3", "310", "d7", "", "d9", "910"
            )
        )
    );
    toPersistA.add(
        new MapBasedInputRow(
            1,
            Arrays.asList("d1", "d2", "d3", "d4", "d5", "d6", "d7", "d8", "d9"),
            ImmutableMap.of(
                "d1", "", "d2", "", "d3", "310", "d7", "", "d9", "910"
            )
        )
    );

    IncrementalIndex toPersistB = new OnheapIncrementalIndex.Builder()
        .setIndexSchema(indexSchema)
        .setMaxRowCount(1000)
        .build();

    toPersistB.add(
        new MapBasedInputRow(
            1,
            Arrays.asList("d1", "d2", "d3", "d4", "d5", "d6", "d7", "d8", "d9"),
            ImmutableMap.of(
                "d1", "", "d2", "", "d3", "310", "d7", "", "d9", "910"
            )
        )
    );
    toPersistB.add(
        new MapBasedInputRow(
            4,
            Arrays.asList("d4", "d5", "d6", "d7", "d8", "d9"),
            ImmutableMap.of(
                "d5", "", "d6", "621", "d7", "", "d8", "821", "d9", "921"
            )
        )
    );
    final File tmpDirA = new File(temporaryFolder, "tempDirA");
    final File tmpDirB = new File(temporaryFolder, "tempDirB");
    final File tmpDirMerged = new File(temporaryFolder, "tempDirMerged");
    final File tmpDirMergedRullup = new File(temporaryFolder, "tempDirMerged2");


    QueryableIndex indexA = closer.closeLater(
        indexIO.loadIndex(indexMerger.persist(toPersistA, tmpDirA, indexSpec, null))
    );

    QueryableIndex indexB = closer.closeLater(
        indexIO.loadIndex(indexMerger.persist(toPersistB, tmpDirB, indexSpec, null))
    );

    final QueryableIndex merged = closer.closeLater(
        indexIO.loadIndex(
            indexMerger.mergeQueryableIndex(
                Arrays.asList(indexA, indexB),
                false,
                new AggregatorFactory[]{new CountAggregatorFactory("count")},
                tmpDirMerged,
                indexSpec,
                null,
                -1
            )
        )
    );
    // final QueryableIndex mergedRollup = closer.closeLater(
    //     indexIO.loadIndex(
    //         indexMerger.mergeQueryableIndex(
    //             Arrays.asList(indexA, indexB),
    //             true,
    //             new AggregatorFactory[]{new CountAggregatorFactory("count")},
    //             tmpDirMergedRullup,
    //             indexSpec,
    //             null,
    //             -1
    //         )
    //     )
    // );

    final QueryableIndexIndexableAdapter adapter = new QueryableIndexIndexableAdapter(merged, null);
    final List<DebugRow> rowList = RowIteratorHelper.toList(adapter.getRows());

    // final QueryableIndexIndexableAdapter adapter2 = new QueryableIndexIndexableAdapter(mergedRollup, null);
    // final List<DebugRow> rowList2 = RowIteratorHelper.toList(adapter2.getRows());

    rowList.forEach(key -> System.out.println(key.dimensions + "," + key.metrics));
    // rowList2.forEach(key -> System.out.println(key.dimensions + "," + key.metrics));
    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals(
          ImmutableList.of("d3", "d6", "d8", "d9"),
          ImmutableList.copyOf(adapter.getDimensionNames())
      );
    } else {
      Assert.assertEquals(
          ImmutableList.of("d1", "d2", "d3", "d5", "d6", "d7", "d8", "d9"),
          ImmutableList.copyOf(adapter.getDimensionNames())
      );
    }

    Assert.assertEquals(4, rowList.size());
    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals(Arrays.asList("310", null, null, "910"), rowList.get(0).dimensionValues());
      Assert.assertEquals(Arrays.asList("310", null, null, "910"), rowList.get(1).dimensionValues());
      Assert.assertEquals(Arrays.asList("310", null, null, "910"), rowList.get(2).dimensionValues());
      Assert.assertEquals(Arrays.asList(null, "621", "821", "921"), rowList.get(3).dimensionValues());
    } else {
      Assert.assertEquals(Arrays.asList("", "", "310", null, null, "", null, "910"), rowList.get(0).dimensionValues());
      Assert.assertEquals(Arrays.asList("", "", "310", null, null, "", null, "910"), rowList.get(1).dimensionValues());
      Assert.assertEquals(Arrays.asList("", "", "310", null, null, "", null, "910"), rowList.get(2).dimensionValues());
      Assert.assertEquals(
          Arrays.asList(null, null, null, "", "621", "", "821", "921"),
          rowList.get(3).dimensionValues()
      );
    }

    checkBitmapIndex(Collections.singletonList(3), adapter.getBitmapIndex("d3", null));
    checkBitmapIndex(Arrays.asList(0, 1, 2), adapter.getBitmapIndex("d3", "310"));

    checkBitmapIndex(Arrays.asList(0, 1, 2), adapter.getBitmapIndex("d6", null));
    checkBitmapIndex(Collections.singletonList(3), adapter.getBitmapIndex("d6", "621"));

    checkBitmapIndex(Arrays.asList(0, 1, 2), adapter.getBitmapIndex("d8", null));
    checkBitmapIndex(Collections.singletonList(3), adapter.getBitmapIndex("d8", "821"));

    checkBitmapIndex(Collections.emptyList(), adapter.getBitmapIndex("d9", null));
    checkBitmapIndex(Arrays.asList(0, 1, 2), adapter.getBitmapIndex("d9", "910"));
    checkBitmapIndex(Collections.singletonList(3), adapter.getBitmapIndex("d9", "921"));
  }


  // @Test
  public void testMergeWithDimensionsList() throws Exception
  {
    IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withDimensionsSpec(new DimensionsSpec(
            makeDimensionSchemas(Arrays.asList("dimA", "dimB", "dimC")),
            null,
            null
        ))
        .withMetrics(new CountAggregatorFactory("count"))
        .build();


    IncrementalIndex toPersist1 = new OnheapIncrementalIndex.Builder()
        .setIndexSchema(schema)
        .setMaxRowCount(1000)
        .build();
    IncrementalIndex toPersist2 = new OnheapIncrementalIndex.Builder()
        .setIndexSchema(schema)
        .setMaxRowCount(1000)
        .build();
    IncrementalIndex toPersist3 = new OnheapIncrementalIndex.Builder()
        .setIndexSchema(schema)
        .setMaxRowCount(1000)
        .build();

    addDimValuesToIndex(toPersist1, "dimA", Arrays.asList("1", "2"));
    addDimValuesToIndex(toPersist2, "dimA", Arrays.asList("1", "2"));
    addDimValuesToIndex(toPersist3, "dimC", Arrays.asList("1", "2"));


    final File tmpDir = new File(temporaryFolder, "tempDirA");
    final File tmpDir2 = new File(temporaryFolder, "tempDirB");
    final File tmpDir3 = new File(temporaryFolder, "tempDirC");
    final File tmpDirMerged = new File(temporaryFolder, "tempDirMerged");

    QueryableIndex index1 = closer.closeLater(
        indexIO.loadIndex(indexMerger.persist(toPersist1, tmpDir, indexSpec, null))
    );

    QueryableIndex index2 = closer.closeLater(
        indexIO.loadIndex(indexMerger.persist(toPersist2, tmpDir2, indexSpec, null))
    );

    QueryableIndex index3 = closer.closeLater(
        indexIO.loadIndex(indexMerger.persist(toPersist3, tmpDir3, indexSpec, null))
    );

    final QueryableIndex merged = closer.closeLater(
        indexIO.loadIndex(
            indexMerger.mergeQueryableIndex(
                Arrays.asList(index1, index2, index3),
                true,
                new AggregatorFactory[]{new CountAggregatorFactory("count")},
                tmpDirMerged,
                indexSpec,
                null,
                -1
            )
        )
    );

    final QueryableIndexIndexableAdapter adapter = new QueryableIndexIndexableAdapter(merged);
    final List<DebugRow> rowList = RowIteratorHelper.toList(adapter.getRows());

    Assert.assertEquals(ImmutableList.of("dimA", "dimC"), ImmutableList.copyOf(adapter.getDimensionNames()));
    Assert.assertEquals(4, rowList.size());

    Assert.assertEquals(Arrays.asList(null, "1"), rowList.get(0).dimensionValues());
    Assert.assertEquals(Collections.singletonList(1L), rowList.get(0).metricValues());

    Assert.assertEquals(Arrays.asList(null, "2"), rowList.get(1).dimensionValues());
    Assert.assertEquals(Collections.singletonList(1L), rowList.get(1).metricValues());

    Assert.assertEquals(Arrays.asList("1", null), rowList.get(2).dimensionValues());
    Assert.assertEquals(Collections.singletonList(2L), rowList.get(2).metricValues());

    Assert.assertEquals(Arrays.asList("2", null), rowList.get(3).dimensionValues());
    Assert.assertEquals(Collections.singletonList(2L), rowList.get(3).metricValues());

    Assert.assertEquals(useBitmapIndexes, adapter.getCapabilities("dimA").hasBitmapIndexes());
    Assert.assertEquals(useBitmapIndexes, adapter.getCapabilities("dimC").hasBitmapIndexes());

    if (useBitmapIndexes) {
      checkBitmapIndex(Arrays.asList(0, 1), adapter.getBitmapIndex("dimA", null));
      checkBitmapIndex(Collections.singletonList(2), adapter.getBitmapIndex("dimA", "1"));
      checkBitmapIndex(Collections.singletonList(3), adapter.getBitmapIndex("dimA", "2"));

      checkBitmapIndex(Collections.emptyList(), adapter.getBitmapIndex("dimB", null));

      checkBitmapIndex(Arrays.asList(2, 3), adapter.getBitmapIndex("dimC", null));
      checkBitmapIndex(Collections.singletonList(0), adapter.getBitmapIndex("dimC", "1"));
      checkBitmapIndex(Collections.singletonList(1), adapter.getBitmapIndex("dimC", "2"));
    }

    checkBitmapIndex(Collections.emptyList(), adapter.getBitmapIndex("dimB", ""));
  }

  private List<DimensionSchema> makeDimensionSchemas(final List<String> dimensions)
  {
    return makeDimensionSchemas(dimensions, MultiValueHandling.SORTED_ARRAY);
  }

  private List<DimensionSchema> makeDimensionSchemas(
      final List<String> dimensions,
      final MultiValueHandling multiValueHandling
  )
  {
    return dimensions.stream()
                     .map(
                         dimension -> new StringDimensionSchema(
                             dimension,
                             multiValueHandling,
                             useBitmapIndexes
                         )
                     )
                     .collect(Collectors.toList());
  }

  private void checkBitmapIndex(List<Integer> expected, BitmapValues real)
  {
    Assert.assertEquals("bitmap size", expected.size(), real.size());
    int i = 0;
    for (IntIterator iterator = real.iterator(); iterator.hasNext(); ) {
      int index = iterator.nextInt();
      Assert.assertEquals(expected.get(i++), (Integer) index);
    }
  }

  private IncrementalIndex getIndexD2() throws Exception
  {
    IncrementalIndex toPersist1 = new OnheapIncrementalIndex.Builder()
        .setSimpleTestingIndexSchema(new LongSumAggregatorFactory("sum", "sum"))
        .setMaxRowCount(1000)
        .build();

    toPersist1.add(
        new MapBasedInputRow(
            1,
            Arrays.asList("d3", "d1", "d2"),
            ImmutableMap.of("d1", "100", "d2", "4000", "d3", "30000", "sum", 1)
        )
    );
    toPersist1.add(
        new MapBasedInputRow(
            1,
            Arrays.asList("d3", "d1", "d2"),
            ImmutableMap.of("d1", "100", "d2", "4000", "d3", "30000", "sum", 1)
        )
    );

    toPersist1.add(
        new MapBasedInputRow(
            2,
            Arrays.asList("d3", "d1", "d2"),
            ImmutableMap.of("d1", "100", "d2", "40000", "d3", "30000", "sum", 10)
        )
    );
    toPersist1.add(
        new MapBasedInputRow(
            2,
            Arrays.asList("d3", "d1", "d2"),
            ImmutableMap.of("d1", "100", "d2", "40000", "d3", "30000", "sum", 10)
        )
    );

    toPersist1.add(
        new MapBasedInputRow(
            2,
            Arrays.asList("d3", "d1", "d2"),
            ImmutableMap.of("d1", "200", "d2", "3000", "d3", "50000", "sum", 100)
        )
    );
    toPersist1.add(
        new MapBasedInputRow(
            2,
            Arrays.asList("d3", "d1", "d2"),
            ImmutableMap.of("d1", "200", "d2", "3000", "d3", "50000", "sum", 100)
        )
    );
    return toPersist1;
  }


  private IncrementalIndex getIndexD3() throws Exception
  {
    IncrementalIndex toPersist1 = new OnheapIncrementalIndex.Builder()
        .setSimpleTestingIndexSchema(new LongSumAggregatorFactory("sum", "sum"))
        .setMaxRowCount(1000)
        .build();

    toPersist1.add(
        new MapBasedInputRow(
            1,
            Arrays.asList("d3", "d1", "d2"),
            ImmutableMap.of("d1", "100", "d2", "4000", "d3", "30000", "sum", 2)
        )
    );
    toPersist1.add(
        new MapBasedInputRow(
            1,
            Arrays.asList("d3", "d1", "d2"),
            ImmutableMap.of("d1", "100", "d2", "40000", "d3", "30000", "sum", 10)
        )
    );
    toPersist1.add(
        new MapBasedInputRow(
            2,
            Arrays.asList("d3", "d1", "d2"),
            ImmutableMap.of("d1", "100", "d2", "40000", "d3", "30000", "sum", 100)
        )
    );
    toPersist1.add(
        new MapBasedInputRow(
            2,
            Arrays.asList("d3", "d1", "d2"),
            ImmutableMap.of("d1", "100", "d2", "40000", "d3", "30000", "sum", 1000)
        )
    );


    toPersist1.add(
        new MapBasedInputRow(
            1,
            Arrays.asList("d3", "d1", "d2"),
            ImmutableMap.of("d1", "300", "d2", "2000", "d3", "40000", "sum", 10000)
        )
    );

    toPersist1.add(
        new MapBasedInputRow(
            1,
            Arrays.asList("d3", "d1", "d2"),
            ImmutableMap.of("d1", "200", "d2", "3000", "d3", "50000", "sum", 100000)
        )
    );

    return toPersist1;
  }

  private IncrementalIndex getSingleDimIndex(String dimName, List<String> values) throws Exception
  {
    IncrementalIndex toPersist1 = new OnheapIncrementalIndex.Builder()
        .setSimpleTestingIndexSchema(new CountAggregatorFactory("count"))
        .setMaxRowCount(1000)
        .build();

    addDimValuesToIndex(toPersist1, dimName, values);
    return toPersist1;
  }

  private void addDimValuesToIndex(IncrementalIndex index, String dimName, List<String> values) throws Exception
  {
    for (String val : values) {
      index.add(new MapBasedInputRow(1, Collections.singletonList(dimName), ImmutableMap.of(dimName, val)));
    }
  }

  //  @Test
  public void testMultivalDim_mergeAcrossSegments_rollupWorks() throws Exception
  {
    List<String> dims = Arrays.asList(
        "dimA",
        "dimMultiVal"
    );

    IncrementalIndexSchema indexSchema = new IncrementalIndexSchema.Builder()
        .withDimensionsSpec(
            new DimensionsSpec(
                ImmutableList.of(
                    new StringDimensionSchema("dimA", MultiValueHandling.SORTED_ARRAY, true),
                    new StringDimensionSchema("dimMultiVal", MultiValueHandling.SORTED_ARRAY, true)
                )
            )
        )
        .withMetrics(
            new LongSumAggregatorFactory("sumCount", "sumCount")
        )
        .withRollup(true)
        .build();

    IncrementalIndex toPersistA = new OnheapIncrementalIndex.Builder()
        .setIndexSchema(indexSchema)
        .setMaxRowCount(1000)
        .build();

    Map<String, Object> event1 = new HashMap<>();
    event1.put("dimA", "leek");
    event1.put("dimMultiVal", ImmutableList.of("1", "2", "4"));
    event1.put("sumCount", 1L);

    Map<String, Object> event2 = new HashMap<>();
    event2.put("dimA", "leek");
    event2.put("dimMultiVal", ImmutableList.of("1", "2", "3", "5"));
    event2.put("sumCount", 1L);

    toPersistA.add(new MapBasedInputRow(1, dims, event1));
    toPersistA.add(new MapBasedInputRow(1, dims, event2));

    IncrementalIndex toPersistB = new OnheapIncrementalIndex.Builder()
        .setIndexSchema(indexSchema)
        .setMaxRowCount(1000)
        .build();

    Map<String, Object> event3 = new HashMap<>();
    event3.put("dimA", "leek");
    event3.put("dimMultiVal", ImmutableList.of("1", "2", "4"));
    event3.put("sumCount", 1L);

    Map<String, Object> event4 = new HashMap<>();
    event4.put("dimA", "potato");
    event4.put("dimMultiVal", ImmutableList.of("0", "1", "4"));
    event4.put("sumCount", 1L);

    toPersistB.add(new MapBasedInputRow(1, dims, event3));
    toPersistB.add(new MapBasedInputRow(1, dims, event4));

    final File tmpDirA = new File(temporaryFolder, "tempDirA");
    final File tmpDirB = new File(temporaryFolder, "tempDirB");
    final File tmpDirMerged = new File(temporaryFolder, "tempDirMerged");

    QueryableIndex indexA = closer.closeLater(
        indexIO.loadIndex(indexMerger.persist(toPersistA, tmpDirA, indexSpec, null))
    );

    QueryableIndex indexB = closer.closeLater(
        indexIO.loadIndex(indexMerger.persist(toPersistB, tmpDirB, indexSpec, null))
    );

    final QueryableIndex merged = closer.closeLater(
        indexIO.loadIndex(
            indexMerger.mergeQueryableIndex(
                Arrays.asList(indexA, indexB),
                true,
                new AggregatorFactory[]{
                    new LongSumAggregatorFactory("sumCount", "sumCount")
                },
                tmpDirMerged,
                indexSpec,
                null,
                -1
            )
        )
    );

    final QueryableIndexIndexableAdapter adapter = new QueryableIndexIndexableAdapter(merged, null);
    final List<DebugRow> rowList = RowIteratorHelper.toList(adapter.getRows());

    Assert.assertEquals(
        ImmutableList.of("dimA", "dimMultiVal"),
        ImmutableList.copyOf(adapter.getDimensionNames())
    );

    Assert.assertEquals(3, rowList.size());
    Assert.assertEquals(Arrays.asList("leek", Arrays.asList("1", "2", "3", "5")), rowList.get(0).dimensionValues());
    Assert.assertEquals(1L, rowList.get(0).metricValues().get(0));
    Assert.assertEquals(Arrays.asList("leek", Arrays.asList("1", "2", "4")), rowList.get(1).dimensionValues());
    Assert.assertEquals(2L, rowList.get(1).metricValues().get(0));
    Assert.assertEquals(Arrays.asList("potato", Arrays.asList("0", "1", "4")), rowList.get(2).dimensionValues());
    Assert.assertEquals(1L, rowList.get(2).metricValues().get(0));

    checkBitmapIndex(Arrays.asList(0, 1), adapter.getBitmapIndex("dimA", "leek"));
    checkBitmapIndex(Collections.singletonList(2), adapter.getBitmapIndex("dimA", "potato"));

    checkBitmapIndex(Collections.singletonList(2), adapter.getBitmapIndex("dimMultiVal", "0"));
    checkBitmapIndex(Arrays.asList(0, 1, 2), adapter.getBitmapIndex("dimMultiVal", "1"));
    checkBitmapIndex(Arrays.asList(0, 1), adapter.getBitmapIndex("dimMultiVal", "2"));
    checkBitmapIndex(Collections.singletonList(0), adapter.getBitmapIndex("dimMultiVal", "3"));
    checkBitmapIndex(Arrays.asList(1, 2), adapter.getBitmapIndex("dimMultiVal", "4"));
    checkBitmapIndex(Collections.singletonList(0), adapter.getBitmapIndex("dimMultiVal", "5"));
  }

}
