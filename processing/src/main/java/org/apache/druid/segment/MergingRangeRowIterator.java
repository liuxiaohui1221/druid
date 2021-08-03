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

import it.unimi.dsi.fastutil.objects.ObjectHeaps;
import org.apache.druid.java.util.common.guava.CloseQuietly;
import org.apache.druid.java.util.common.io.Closer;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.List;
import java.util.stream.IntStream;

/**
 * MergingRangeRowIterator sort-merges rows of several {@link RangeRowIteratorImpl}s, assuming that each of them is already sorted
 * (i. e. as {@link RangeRowIteratorImpl#moveToNext()} is called, the pointer returned from {@link RangeRowIteratorImpl#getPointer()} is
 * "greater" than the previous, in terms of {@link TimeAndDimsPointer#compareTo}). Equivalent points from different
 * input iterators are _not_ deduplicated.
 *
 * Conceptually MergingRangeRowIterator is an equivalent to {@link com.google.common.collect.Iterators#mergeSorted}, but for
 * {@link RangeRowIteratorImpl}s rather than simple {@link java.util.Iterator}s.
 */
final class MergingRangeRowIterator implements TransformableRowIterator
{
  private static final Comparator<RowIterator> ROW_ITERATOR_COMPARATOR =
      Comparator.comparing(RowIterator::getPointer);

  /** Used to close {@link #originalIterators} */
  private final Closer closer = Closer.create();

  private RangeRowIteratorImpl[] originalIterators;

  /**
   * Binary heap (priority queue)
   */
  private RangeRowIteratorImpl[] pQueue;
  private int pQueueSize;

  /**
   * Boolean flags corresponding to the elements of {@link #pQueue} binary heap, signifying if the element is equal
   * to one or both children in the binary heap. For leaf elements (i. e. no children), the value should be false.
   */
  private boolean[] equalToChild;

  /**
   * true while {@link #moveToNext()} is not called yet.
   */
  private boolean first = true;

  /**
   * True by default, so that if {@link #mark()} is never called, the extra work to compute the value for this field is
   * never done.
   */
  private boolean changedSinceMark = true;

  /** This field is needed for some optimization, see the comment where it is used. */
  @Nullable
  private RowIterator lastMarkedHead = null;

  MergingRangeRowIterator(List<RangeRowIteratorImpl> iterators)
  {
    initRangeRowIterators(iterators);
  }

  public void initRangeRowIterators(List<RangeRowIteratorImpl> iterators)
  {
    iterators.forEach(closer::register);
    originalIterators = new RangeRowIteratorImpl[iterators.size()];
    pQueue = IntStream
        .range(0, iterators.size())
        .filter(indexNum -> iterators.get(indexNum).moveToNext())
        .mapToObj(indexNum -> {
          RangeRowIteratorImpl rowIterator = iterators.get(indexNum);
          // Can call rowIterator.getPointer() only here, after moveToNext() returned true on the filter() step
          rowIterator.getPointer().setIndexNum(indexNum);
          // rowIterator.mark();
          originalIterators[indexNum] = rowIterator;
          return rowIterator;
        })
        .toArray(RangeRowIteratorImpl[]::new);
    equalToChild = new boolean[pQueue.length];
    heapify();
    initEqualToChildStates();
  }

  @Nullable
  TransformableRowIterator getOriginalIterator(int indexNum)
  {
    return originalIterators[indexNum];
  }

  @Override
  public RowPointer getPointer()
  {
    // Current MergingRowIterator's pointer is the pointer of the head of the priority queue.
    return pQueue[0].getPointer();
  }

  @Override
  public TimeAndDimsPointer getMarkedPointer()
  {
    return pQueue[0].getMarkedPointer();
  }

  @Override
  public boolean moveToNext()
  {
    if (pQueueSize == 0) {
      if (first) {
        first = false;
        return false;
      }
      throw new IllegalStateException("Don't call moveToNext() after it returned false once");
    }
    if (first) {
      first = false;
      return true;
    }

    RowIterator head = pQueue[0];
    if (!changedSinceMark) {
      // lastMarkedHead field allows small optimization: avoiding many re-marks of the rows of the head iterator,
      // if it has many elements with equal dimensions.
      //noinspection ObjectEquality: checking specifically if lastMarkedHead and head is the same object
      if (lastMarkedHead != head) {
        head.mark();
        lastMarkedHead = head;
      }
    }
    boolean headUsedToBeEqualToChild = equalToChild[0];
    if (head.moveToNext()) {
      if (sinkHeap(0) == 0) { // The head iterator didn't change
        if (!changedSinceMark && head.hasTimeAndDimsChangedSinceMark()) {
          changedSinceMark = true;
        }
      } else { // The head iterator changed
        // If the head iterator changed, the changedSinceMark property could still be "unchanged", if there were several
        // iterators pointing to equal "time and dims", that is what the following line checks:
        changedSinceMark |= !headUsedToBeEqualToChild;
      }
      return true;
    } else {
      pQueueSize--;
      if (pQueueSize > 0) {
        pQueue[0] = pQueue[pQueueSize];
        pQueue[pQueueSize] = null;

        // The head iterator is going to change, so the changedSinceMark property could still be "unchanged", if there
        // were several iterators pointing to equal "time and dims", that is what the following line checks:
        changedSinceMark |= !headUsedToBeEqualToChild;

        int parentOfLast = (pQueueSize - 1) >> 1;
        // This sinkHeap() call is guaranteed to not move any heap elements, but it is used as a shortcut to fix the
        // equalToChild[parentOfLast] status, as a side-effect. equalToChild[parentOfLast] could have changed, e. g. if
        // the last element was equal to parentOfLast, and parentOfLast had only one child, or the other child is
        // different. In this case, equalToChild[parentOfLast] is going to be changed from true to false, because the
        // last element is now moved to the head. equalToChild[parentOfLast] must have correct value before the
        // sinkHeap(0) call a few lines below, because it's an assumed invariant of sinkHeap().
        sinkHeap(parentOfLast);
        sinkHeap(0);
        return true;
      } else {
        // Don't clear pQueue[0], to conform to RowIterator.getPointer() specification.
        // Don't care about changedSinceMark, because according to the RowIterator specification the behaviour of
        // hasTimeAndDimsChangedSinceMark() is undefined now.
        return false;
      }
    }
  }

  @Override
  public void mark()
  {
    changedSinceMark = false;
    lastMarkedHead = null;
  }

  @Override
  public boolean hasTimeAndDimsChangedSinceMark()
  {
    return changedSinceMark;
  }

  /**
   * Sinks (pushes down) the iterator at the given index i, because it's value just changed due to {@link
   * RowIterator#moveToNext()} call.
   *
   * This method implementation is originally based on {@link ObjectHeaps#downHeap} implementation, plus it updates
   * {@link #equalToChild} states along the way.
   */
  private int sinkHeap(int i)
  {
    final RangeRowIteratorImpl iteratorToSink = pQueue[i];
    while (true) {
      int left = (i << 1) + 1;
      if (left >= pQueueSize) {
        break;
      }
      int child = left;
      RangeRowIteratorImpl childIterator = pQueue[left];
      final int right = left + 1;
      // Setting childrenDiff to non-zero initially, so that if there is just one child (right == pQueueSize),
      // the expression `childrenDiff == 0` below is false.
      int childrenDiff = -1; // any non-zero number
      if (right < pQueueSize) {
        RangeRowIteratorImpl rightChildIterator = pQueue[right];
        childrenDiff = rightChildIterator.getPointer().compareTo(childIterator.getPointer());
        if (childrenDiff < 0) {
          child = right;
          childIterator = rightChildIterator;
        }
      }
      int parentDiff = iteratorToSink.getPointer().compareTo(childIterator.getPointer());
      if (parentDiff == 0) {
        equalToChild[i] = true;
        pQueue[i] = iteratorToSink;
        return i;
      }
      if (parentDiff < 0) {
        equalToChild[i] = false;
        pQueue[i] = iteratorToSink;
        return i;
      }
      equalToChild[i] = childrenDiff == 0 || equalToChild[child];
      pQueue[i] = childIterator;
      i = child;
    }
    equalToChild[i] = false; // Since we exited from the above loop, there are no more children
    pQueue[i] = iteratorToSink;
    return i;
  }

  private void heapify()
  {
    pQueueSize = 0;
    while (pQueueSize < pQueue.length) {
      pQueueSize++;
      ObjectHeaps.upHeap(pQueue, pQueueSize, pQueueSize - 1, ROW_ITERATOR_COMPARATOR);
    }
  }

  private void initEqualToChildStates()
  {
    for (int i = 0; i < pQueueSize; i++) {
      int left = (i << 1) + 1;
      int right = left + 1;
      equalToChild[i] = (left < pQueueSize && iteratorsEqual(i, left)) ||
                        (right < pQueueSize && iteratorsEqual(i, right));
    }
  }

  private boolean iteratorsEqual(int i1, int i2)
  {
    return pQueue[i1].getPointer().compareTo(pQueue[i2].getPointer()) == 0;
  }

  @Override
  public void close()
  {
    CloseQuietly.close(closer);
  }
}
