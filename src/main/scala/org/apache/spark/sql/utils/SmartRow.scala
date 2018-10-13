package org.apache.spark.sql.utils

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow

class SmartRow(values: Array[Any]) extends GenericInternalRow(values)

object SmartRow {

  def fromSeq(values: Seq[Any]): SmartRow =
    new SmartRow(values.toArray)
}