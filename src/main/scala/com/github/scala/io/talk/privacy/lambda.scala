package com.github.scala.io.talk.privacy

import com.github.scala.io.talk._
import com.github.scala.io.talk.privacy.PrivacyStrategy.{PrivacyMethod, PrivacyStrategies}
import matryoshka.Algebra
import matryoshka.data.Fix
import org.slf4j.LoggerFactory

object ApplyPrivacyLambda {
  private val logger = LoggerFactory.getLogger("ApplyPrivacyLambda")

  def prepareTransform(schema: Fix[SchemaF], privacyStrategies: PrivacyStrategies): LensOp = {

    val alg: Algebra[SchemaF, LensOp] = {

      case StructF(fields, _) =>
        if (fields.map(_._2).forall(_ == NoLensOp)) {
          // all fields are not to be privacied
          NoLensOp
        } else {
          val lambda: Fix[DataF] => Fix[DataF] = {
            case Fix(GStructF(dataFields)) =>
              val newFields = fields.zip(dataFields).map {
                case ((fieldName, innerOp), (_, data)) =>
                  if (innerOp == NoLensOp || data == Fix[DataF](GNullF())) {
                    (fieldName, data)
                  } else {
                    val privacied = innerOp(data)
                    (fieldName, privacied)
                  }
              }
              Fix(GStructF(newFields))

            case gdata =>
              gdata // should not happen
          }
          GoDown(lambda)
        }

      case ArrayF(elementType, metadata) =>
        privacyStrategies.foldLeft(NoLensOp: LensOp) {
          case (op, (keys, (methodName, cypher))) =>
            if (keys.map(metadata.tags.contains).reduce(_ && _)) {
              val lambda: Fix[DataF] => Fix[DataF] =
                cypherWithContext(methodName, cypher)
              op.andThen(lambda)
            } else {
              op
            }
        } match {
          case NoLensOp =>
            val lambda: Fix[DataF] => Fix[DataF] = {
              case Fix(GArrayF(elems)) =>
                val results = elems.map(elementType.apply)
                Fix(GArrayF(results))

              case gdata =>
                gdata// should not happen
            }
            GoDown(lambda)

          case op =>
            op
        }

      case value: ValueF[LensOp] if value.metadata.tags.nonEmpty =>
        privacyStrategies.foldLeft(NoLensOp: LensOp) {
          case (op, (keys, (methodName, cypher))) =>
            if (keys.map(value.metadata.tags.contains).reduce(_ && _)) {
              val lambda: Fix[DataF] => Fix[DataF] =
                cypherWithContext(methodName, cypher)
              op.andThen(lambda)
            } else {
              op
            }
        }
      case _ => NoLensOp
    }

    Fix.birecursiveT.cataT(schema)(alg)
  }

  private def cypherWithContext(methodName: PrivacyMethod,
                                cypher: PrivacyStrategy
                               )(value: Fix[DataF]): Fix[DataF] = {
    cypher(value).fold(
      errors => {
        if (value != Fix[DataF](GNullF())) {
          errors.foreach(err => logger.warn(s"Error while applying privacy on $value : $err"))
        }
        Fix[DataF](GNullF())
      },
      x => x
    )
  }
}

/**
  * Represents a nested op that applies a function to a GenericData and
  * can be composed
  */
sealed trait LensOp extends Serializable {

  def andThen(f: Fix[DataF] => Fix[DataF]): LensOp

  def apply(gdata: Fix[DataF]): Fix[DataF]
}

/**
  * A specific [[LensOp]] that goes "down" and apply a function to the data
  *
  * @param apply0 the function to apply
  */
private case class GoDown(apply0: Fix[DataF] => Fix[DataF])
  extends LensOp
    with Serializable {

  override def andThen(f: Fix[DataF] => Fix[DataF]): LensOp = {
    GoDown(apply0.andThen(f))
  }

  override def apply(gdata: Fix[DataF]): Fix[DataF] = apply0(gdata)
}

/**
  * NoOp - nothing comes out of this - there's nothing to do !
  */
private case object NoLensOp extends LensOp with Serializable {

  override def andThen(f: Fix[DataF] => Fix[DataF]): LensOp = GoDown(f)

  override def apply(gdata: Fix[DataF]): Fix[DataF] = gdata
}
