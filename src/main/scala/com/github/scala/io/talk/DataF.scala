package com.github.scala.io.talk

import matryoshka.{Birecursive, CoalgebraM}
import matryoshka.data.Fix
import scalaz._
import Scalaz._
import com.github.scala.io.api.DataWithSchema
import matryoshka.patterns.EnvT

import scala.collection.immutable.ListMap

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

final case class GStructF[A](fields: ListMap[String, A]) extends DataF[A]

final case class GStringF[A](value: String) extends GValueF[A]

final case class GLongF[A](value: Long) extends GValueF[A]

final case class GIntF[A](value: Int) extends GValueF[A]

final case class GDoubleF[A](value: Double) extends GValueF[A]

final case class GFloatF[A](value: Float) extends GValueF[A]

final case class GDateF[A](value: java.sql.Date) extends GValueF[A]

final case class GTimestampF[A](value: java.sql.Timestamp) extends GValueF[A]

final case class GBooleanF[A](value: Boolean) extends GValueF[A]

trait DataFInstances {
 /* val genericDataCoalgebra: Coalgebra[DataF, GenericData] = {
    case GNull | null      => GNullF()
    case GArray(elems)     => GArrayF(elems)
    case GStruct(fields)   => GStructF(fields)
    case GString(value)    => GStringF(value)
    case GLong(value)      => GLongF(value)
    case GInt(value)       => GIntF(value)
    case GDouble(value)    => GDoubleF(value)
    case GFloat(value)     => GFloatF(value)
    case GDate(value)      => GDateF(value)
    case GTimestamp(value) => GTimestampF(value)
    case GBoolean(value)   => GBooleanF(value)
  }

  val genericDataAlgebra: Algebra[DataF, GenericData] = {
    case GNullF()           => GNull
    case GArrayF(elems)     => GArray(elems)
    case GStructF(fields)   => GStruct(fields)
    case GStringF(value)    => GString(value)
    case GLongF(value)      => GLong(value)
    case GIntF(value)       => GInt(value)
    case GDoubleF(value)    => GDouble(value)
    case GFloatF(value)     => GFloat(value)
    case GDateF(value)      => GDate(value)
    case GTimestampF(value) => GTimestamp(value)
    case GBooleanF(value)   => GBoolean(value)
  }

  implicit val genericDataBirecursive: Birecursive.Aux[GenericData, DataF] =
    Birecursive.algebraIso(genericDataAlgebra, genericDataCoalgebra)*/

  implicit val genericDataFTraverse: Traverse[DataF] = new Traverse[DataF] {

    override def traverseImpl[G[_], A, B](
                                           fa: DataF[A]
                                         )(f: A => G[B])(implicit evidence$1: Applicative[G]): G[DataF[B]] = fa match {
      case GNullF() => Applicative[G].point(GNullF[B]())
      case GArrayF(elems) =>
        Functor[G].map(elems.toList traverse f)(GArrayF.apply)

      case GStructF(fields) =>
        val (keys, values) = fields.unzip
        Functor[G].map(values.toList traverse f)(v => GStructF(ListMap((keys zip v).toSeq: _*)))

      case GStringF(value)    => Applicative[G].point(GStringF[B](value))
      case GLongF(value)      => Applicative[G].point(GLongF[B](value))
      case GIntF(value)       => Applicative[G].point(GIntF[B](value))
      case GDoubleF(value)    => Applicative[G].point(GDoubleF[B](value))
      case GFloatF(value)     => Applicative[G].point(GFloatF[B](value))
      case GDateF(value)      => Applicative[G].point(GDateF[B](value))
      case GTimestampF(value) => Applicative[G].point(GTimestampF[B](value))
      case GBooleanF(value)   => Applicative[G].point(GBooleanF[B](value))
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

    case (structf @ Fix(StructF(fields, metadata)), Fix(GStructF(values))) =>
      val fieldMap = fields
      val zipped = values.map { case (name, value) => (name, (fieldMap(name), value)) }
      EnvT[Fix[SchemaF], DataF, (Fix[SchemaF], Fix[DataF])]((structf, DataF.struct(zipped))).right[Incompatibility]

    case (structf @ Fix(StructF(_, _)), Fix(GNullF())) =>
      EnvT[Fix[SchemaF], DataF, (Fix[SchemaF], Fix[DataF])]((structf, GNullF())).right[Incompatibility]

    case (arrayF @ Fix(ArrayF(n, m)), Fix(GArrayF(elements))) =>
      val fieldSchema: Fix[SchemaF] = arrayF // schemaFor(arrayF) FIXME
      // no patch infos allowed on an array
      val arrayColumnSchema = arrayF//.copy(metadata = m.copy(patchInfo = None)) FIXME
      val arrayFa = DataF.array(elements.toList map { e =>
        fieldSchema -> e
      })
      EnvT[Fix[SchemaF], DataF, (Fix[SchemaF], Fix[DataF])]((arrayColumnSchema, arrayFa)).right[Incompatibility]

    case (arrayF @ Fix(ArrayF(_, m)), Fix(GNullF())) =>
      // no patch infos allowed on an array
      val arrayColumnSchema = arrayF//.copy(metadata = m.copy(patchInfo = None)) FIXME
      EnvT[Fix[SchemaF], DataF, (Fix[SchemaF], Fix[DataF])]((arrayColumnSchema, GNullF())).right[Incompatibility]

    case (valueF, Fix(lower)) =>
      val dataF = lower.map((valueF, _))
      EnvT[Fix[SchemaF], DataF, (Fix[SchemaF], Fix[DataF])]((valueF, dataF)).right[Incompatibility]

    case (s, d) => Incompatibility(s, d).left
  }
}

object DataF extends DataFInstances with DataFunctions {

  def struct[A](fields: ListMap[String, A]): DataF[A] = GStructF(fields)

  def array[A](elements: List[A]): DataF[A] = GArrayF(elements)

}