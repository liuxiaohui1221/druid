package org.apache.druid.query.groupby;

import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;

public class MutableObjectColumnSelector implements
    ColumnValueSelector<Object>
{
  private Object value;
  public void setValue(Object value) {
    this.value = value;
  }
  @Override
  public double getDouble()
  {
    return value instanceof Number ? ((Number) value).doubleValue() : 0.0;
  }

  @Override
  public float getFloat()
  {
    return value instanceof Number ? ((Number) value).floatValue() : 0.0f;
  }

  @Override
  public long getLong()
  {
    return value instanceof Number ? ((Number) value).longValue() : 0;
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {

  }

  @Override
  public boolean isNull()
  {
    return false;
  }

  @Nullable
  @Override
  public Object getObject()
  {
    return value;
  }

  @Override
  public Class<?> classOfObject()
  {
    return value != null ? value.getClass() : Object.class;
  }
}
