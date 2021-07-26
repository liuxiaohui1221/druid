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

import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

import java.nio.IntBuffer;
import java.util.Arrays;
import java.util.Iterator;

public class MegeTest
{
  @Test
  public void testBuffer()
  {
    int[] arr = {1, 2, 3, 4};
    Arrays.fill(arr, -1);
    IntBuffer wrap = IntBuffer.wrap(arr);
    wrap.put(1, 11);
    System.out.println(Arrays.toString(wrap.array()));
    wrap.array()[2] = 100;
    System.out.println(Arrays.toString(wrap.array()));
    wrap.put(111);
  }

  @Test
  public void testBitMap()
  {
    RoaringBitmap roaringBitmap = new RoaringBitmap();
    roaringBitmap.add(10);
    roaringBitmap.add(11);
    roaringBitmap.clear();
    roaringBitmap.add(19);
    roaringBitmap.add(1);
    roaringBitmap.add(1);
    Iterator<Integer> iterator = roaringBitmap.iterator();
    while (iterator.hasNext()) {
      System.out.println(iterator.next());
    }

    RoaringBitmap[] combinedRowNumsOfOriginalIteratorIndexs = new RoaringBitmap[3];
    for (int i = 0; i < 3; i++) {
      combinedRowNumsOfOriginalIteratorIndexs[i] = new RoaringBitmap();
    }
    // combinedRowNumsOfOriginalIteratorIndexs[1] = new RoaringBitmap();
    RoaringBitmap combinedRowNumsOfOriginalIteratorIndex = combinedRowNumsOfOriginalIteratorIndexs[1];
    combinedRowNumsOfOriginalIteratorIndex.add(11);
    System.out.println(Arrays.toString(combinedRowNumsOfOriginalIteratorIndexs));

    combinedRowNumsOfOriginalIteratorIndex.clear();
    combinedRowNumsOfOriginalIteratorIndex.add(1);
    combinedRowNumsOfOriginalIteratorIndex.add(2);

    System.out.println(Arrays.toString(combinedRowNumsOfOriginalIteratorIndexs));
  }
}
