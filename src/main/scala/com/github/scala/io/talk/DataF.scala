package com.github.scala.io.talk

import com.github.scala.io.api.DataWithSchema
import matryoshka.{CoalgebraM, Recursive}
import matryoshka.data.Fix
import matryoshka.implicits._
import matryoshka.patterns.EnvT
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, DataType, StructType}
import org.apache.spark.unsafe.types.UTF8String
import scalaz.Scalaz._
import scalaz._

sealed trait DataF[A]

/**
  * Marker trait for "terminal" data types
  */
sealed trait GValueF[A] extends DataF[A] {

  def value: Any
}

final case class GNullF[A]() extends GValueF[A] {

  def value: Any = null
}

final case class GArrayF[A](elems: Seq[A]) extends DataF[A]

final case class GStructF[A](fields: List[(String, A)]) extends DataF[A]

final case class GStringF[A](value: String) extends GValueF[A]

final case class GLongF[A](value: Long) extends GValueF[A]

final case class GIntF[A](value: Int) extends GValueF[A]

final case class GDoubleF[A](value: Double) extends GValueF[A]

final case class GFloatF[A](value: Float) extends GValueF[A]

final case class GDateF[A](value: java.sql.Date) extends GValueF[A]

final case class GTimestampF[A](value: java.sql.Timestamp) extends GValueF[A]

final case class GBooleanF[A](value: Boolean) extends GValueF[A]

trait DataFInstances {
  implicit val genericDataFTraverse: Traverse[DataF] = new Traverse[DataF] {

    override def traverseImpl[G[_], A, B](
                                           fa: DataF[A]
                                         )(f: A => G[B])(implicit evidence$1: Applicative[G]): G[DataF[B]] = fa match {
      case GNullF() => Applicative[G].point(GNullF[B]())
      case GArrayF(elems) =>
        Functor[G].map(elems.toList traverse f)(GArrayF.apply)

      case GStructF(fields) =>
        val (keys, values) = fields.unzip
        Functor[G].map(values.toList traverse f)(v => GStructF(List((keys zip v).toSeq: _*)))

      case GStringF(value) => Applicative[G].point(GStringF[B](value))
      case GLongF(value) => Applicative[G].point(GLongF[B](value))
      case GIntF(value) => Applicative[G].point(GIntF[B](value))
      case GDoubleF(value) => Applicative[G].point(GDoubleF[B](value))
      case GFloatF(value) => Applicative[G].point(GFloatF[B](value))
      case GDateF(value) => Applicative[G].point(GDateF[B](value))
      case GTimestampF(value) => Applicative[G].point(GTimestampF[B](value))
      case GBooleanF(value) => Applicative[G].point(GBooleanF[B](value))
    }
  }
}

trait DataFunctions {

  /**
    * @group coalgebras
    *
    *        This coalgebra can be used to label each element of a `Fix[DataF]` with its schema.
    *
    *        Given a schema and some data, return either a [[DataWithSchema]] or a [[Incompatibility]].
    */
  def zipWithSchema: CoalgebraM[\/[Incompatibility, ?], DataWithSchema, (Fix[SchemaF], Fix[DataF])] = {

    case (structf@Fix(StructF(fields, metadata)), Fix(GStructF(values))) =>
      val fieldMap = fields
      val zipped = values.map { case (name, value) => (name, (fieldMap.toMap.apply(name), value)) }
      EnvT[Fix[SchemaF], DataF, (Fix[SchemaF], Fix[DataF])]((structf, DataF.struct(zipped))).right[Incompatibility]

    case (structf@Fix(StructF(_, _)), Fix(GNullF())) =>
      EnvT[Fix[SchemaF], DataF, (Fix[SchemaF], Fix[DataF])]((structf, GNullF())).right[Incompatibility]

    case (arrayF@Fix(ArrayF(n, m)), Fix(GArrayF(elements))) =>
      val fieldSchema: Fix[SchemaF] = arrayF // schemaFor(arrayF) FIXME
    // no patch infos allowed on an array
    val arrayColumnSchema = arrayF
      //.copy(metadata = m.copy(patchInfo = None)) FIXME
      val arrayFa = DataF.array(elements.toList map { e =>
        fieldSchema -> e
      })
      EnvT[Fix[SchemaF], DataF, (Fix[SchemaF], Fix[DataF])]((arrayColumnSchema, arrayFa)).right[Incompatibility]

    case (arrayF@Fix(ArrayF(_, m)), Fix(GNullF())) =>
      // no patch infos allowed on an array
      val arrayColumnSchema = arrayF //.copy(metadata = m.copy(patchInfo = None)) FIXME
      EnvT[Fix[SchemaF], DataF, (Fix[SchemaF], Fix[DataF])]((arrayColumnSchema, GNullF())).right[Incompatibility]

    case (valueF, Fix(lower)) =>
      val dataF = lower.map((valueF, _))
      EnvT[Fix[SchemaF], DataF, (Fix[SchemaF], Fix[DataF])]((valueF, dataF)).right[Incompatibility]

    case (s, d) => Incompatibility(s, d).left
  }
}

object DataF extends DataFInstances with DataFunctions {

  def struct[A](fields: List[(String, A)]): DataF[A] = GStructF(fields)

  def array[A](elements: List[A]): DataF[A] = GArrayF(elements)
}

object SparkDataConverter {
  /**
    * Convert from our GenericData container to a Spark SQL compatible Row
    * first and last step before creating a dataframe
    *
    * @param row data
    * @return spark's Row
    */
  def fromGenericData[T](row: T)(implicit T: Recursive.Aux[T, DataF]): Row = {
    import matryoshka._

    import scala.language.higherKinds

    val gAlgebra: GAlgebra[(T, ?), DataF, Row] = {
      case GArrayF(elems) =>
        val values = elems.map {
          case (previous, current) =>
            if (previous.project.isInstanceOf[GValueF[_]])
              current.get(0)
            else
              current
        }
        Row(values)

      case GStructF(fields) =>
        val values = fields.map { field =>
          val (fx, value) = field._2
          if (fx.project.isInstanceOf[GValueF[_]] || fx.project.isInstanceOf[GArrayF[_]]) {
            value.get(0)
          } else {
            value
          }
        }
        Row(values: _*)

      case el: GValueF[_] =>
        Row(el.value)
    }

    row.para[Row](gAlgebra)
  }

  def toGenericData(row: Row, schema: StructType): Fix[DataF] = {
    def handleElement(element: Any, schema: DataType): Fix[DataF] = {
      element match {
        case arr: Seq[Any] =>
          val arrayType = schema.asInstanceOf[ArrayType]
          Fix(GArrayF(arr.map(el => handleElement(el, arrayType.elementType))))

        case struct: Row =>
          val structType = schema.asInstanceOf[StructType]
          val dataset = struct.toSeq.zipWithIndex.map {
            case (el, idx) =>
              val field = structType(idx)
              val elementType = field.dataType
              (field.name, handleElement(el, elementType))
          }
          Fix(GStructF(dataset.toList))
        case value: java.sql.Timestamp =>
          Fix(GTimestampF(value))
        case value: java.sql.Date =>
          Fix(GDateF(value))
        case value: Boolean =>
          Fix(GBooleanF(value))
        case value: Int =>
          Fix(GIntF(value))
        case value: Float =>
          Fix(GFloatF(value))
        case value: Double =>
          Fix(GDoubleF(value))
        case value: Long =>
          Fix(GLongF(value))
        case value: String =>
          Fix(GStringF(value))
        case value: UTF8String =>
          Fix(GStringF(value.toString))
        case null =>
          Fix(GNullF())
      }
    }

    handleElement(row, schema)
  }
}