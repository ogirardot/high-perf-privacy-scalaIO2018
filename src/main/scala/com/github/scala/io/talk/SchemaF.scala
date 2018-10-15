package com.github.scala.io.talk

import com.github.scala.io.talk.ColumnMetadata.SemanticTag
import matryoshka.{Algebra, Birecursive, Coalgebra}
import scalaz.Functor


case class ColumnMetadata(nullable: Boolean, tags: List[SemanticTag])

object ColumnMetadata {
  type SemanticTag = (String, String)

  def empty = ColumnMetadata(nullable = true, Nil)

}
/**
  * Without further ado, let's define our main pattern-functor for the remaining of the session.
  */
sealed trait SchemaF[A] {
  val metadata: ColumnMetadata
}

// we'll use a ListMap to keep the ordering of the fields
final case class StructF[A](fields: List[(String, A)], metadata: ColumnMetadata) extends SchemaF[A]
final case class ArrayF[A](element: A, metadata: ColumnMetadata)                  extends SchemaF[A]

sealed trait ValueF[A] extends SchemaF[A] {
  val metadata: ColumnMetadata
}
final case class BooleanF[A](metadata: ColumnMetadata)                          extends ValueF[A]
final case class DateF[A](metadata: ColumnMetadata)                             extends ValueF[A]
final case class DoubleF[A](metadata: ColumnMetadata)                           extends ValueF[A]
final case class FloatF[A](metadata: ColumnMetadata)                            extends ValueF[A]
final case class IntegerF[A](metadata: ColumnMetadata)                          extends ValueF[A]
final case class LongF[A](metadata: ColumnMetadata)                             extends ValueF[A]
final case class StringF[A](metadata: ColumnMetadata)                           extends ValueF[A]

object SchemaF extends SchemaFToDataTypeAlgebras {

  /**
    * As usual, we need to define a Functor instance for our pattern.
    */
  implicit val schemaFScalazFunctor: Functor[SchemaF] = new Functor[SchemaF] {
    def map[A, B](fa: SchemaF[A])(f: A => B): SchemaF[B] = fa match {
      case StructF(fields, m) => StructF(List(
        fields
          .map{ case (name, value) => name -> f(value) }:_*
      ), m)
      case ArrayF(elem, m)  => ArrayF(f(elem), m)
      case BooleanF(m)      => BooleanF(m)
      case DateF(m)         => DateF(m)
      case DoubleF(m)       => DoubleF(m)
      case FloatF(m)        => FloatF(m)
      case IntegerF(m)      => IntegerF(m)
      case LongF(m)         => LongF(m)
      case StringF(m)       => StringF(m)
    }
  }
}

/**
  * Now that we have a proper pattern-functor, we need (co)algebras to go from our "standard" schemas to
  * our new and shiny SchemaF (and vice versa).
  *
  * Lets focus on Parquet schemas first. Parquet is a columnar data format that allows efficient processing
  * of large datasets in a distributed environment (eg Spark). In the Spark API, Parquet schemas are represented
  * as instances of the DataType type. So what we want to write here is a pair of (co)algebras that go from/to
  * SchemaF/DataType.
  *
  * NOTE: in order not to depend directly on Spark (and, hence, transitively on half of maven-central), we've copied
  * the definition of the DataType trait and its subclasses in the current project under
  * `spark/src/main/scala/DataType.scala`.
  */
trait SchemaFToDataTypeAlgebras {

  import org.apache.spark.sql.types._

  /**
    * As usual, simply a function from SchemaF[DataType] to DataType
    */
  def schemaFToDataType: Algebra[SchemaF, DataType] = {
    case StructF(fields, _) => StructType(fields.map { case (name, value) => StructField(name, value) }.toArray)
    case ArrayF(elem, m)    => ArrayType(elem, containsNull = false)
    case BooleanF(_)      => BooleanType
    case DateF(_)         => DateType
    case DoubleF(_)       => DoubleType
    case FloatF(_)        => FloatType
    case IntegerF(_)      => IntegerType
    case LongF(_)         => LongType
    case StringF(_)       => StringType

  }

  /**
    * And the other way around, a function from DataType to SchemaF[DataType]
    */
  def dataTypeToSchemaF: Coalgebra[SchemaF, DataType] = {
    case StructType(fields) => StructF(List(fields.map(f => f.name -> f.dataType): _*), ColumnMetadata.empty)
    case ArrayType(elem, _) => ArrayF(elem, ColumnMetadata.empty)
    case BooleanType        => BooleanF(ColumnMetadata.empty)
    case DateType           => DateF(ColumnMetadata.empty)
    case DoubleType         => DoubleF(ColumnMetadata.empty)
    case FloatType          => FloatF(ColumnMetadata.empty)
    case IntegerType        => IntegerF(ColumnMetadata.empty)
    case LongType           => LongF(ColumnMetadata.empty)
    case StringType         => StringF(ColumnMetadata.empty)

  }

  /**
    * This pair of (co)algebras allows us to create a Birecursive[DataType, SchemaF] instance "for free".
    *
    * Such instance witnesses the fact that we can use a DataType in schemes that would normally apply to SchemaF.
    * For example, suppose that we have:
    *
    * {{{
    *   val parquet: DataType = ???
    *   val toAvro: Algebra[SchemaF, avro.Schema] = ???
    * }}}
    *
    * If we have the instance bellow in scope (and the necessary implicits from matryoshka.implicits), we can now write
    *
    * {{{
    *   parquet.cata(toAvro)
    * }}}
    *
    * Instead of
    *
    * {{{
    *   parquet.hylo(dataTypeToSchemaf, toAvro)
    * }}}
    *
    * And the same goes with `ana` and any Coalgebra[SchemaF, X].
    */
  implicit val dataTypeSchemaBirecursive: Birecursive.Aux[DataType, SchemaF] =
    Birecursive.algebraIso(schemaFToDataType, dataTypeToSchemaF)
}