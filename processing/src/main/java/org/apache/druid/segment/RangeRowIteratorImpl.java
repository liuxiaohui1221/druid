package org.apache.druid.segment;

public class RangeRowIteratorImpl implements TransformableRowIterator
{
  @Override
  public void mark()
  {

  }

  @Override
  public boolean hasTimeAndDimsChangedSinceMark()
  {
    return false;
  }

  @Override
  public boolean moveToNext()
  {
    return false;
  }

  @Override
  public RowPointer getPointer()
  {
    return null;
  }

  @Override
  public void close()
  {

  }

  @Override
  public TimeAndDimsPointer getMarkedPointer()
  {
    return null;
  }
}
