package com.github.scala.io.talk.privacy

import com.github.scala.io.talk._
import com.github.scala.io.talk.privacy.PrivacyStrategy.PrivacyStrategies
import matryoshka.Algebra
import matryoshka.data.Fix
import org.slf4j.LoggerFactory

object ApplyPrivacyLambda {
  private val logger = LoggerFactory.getLogger("ApplyPrivacyLambda")

  def prepareTransform(schema: Fix[SchemaF],
                       privacyStrategies: PrivacyStrategies): MutationOp = {

    val alg: Algebra[SchemaF, MutationOp] = {

      case StructF(fields, _) =>
        if (fields.map(_._2).forall(_ == NoMutationOp)) {
          // all fields are not to be privacied
          NoMutationOp
        } else {
          val lambda: Fix[DataF] => Fix[DataF] = {
            case Fix(GStructF(dataFields)) =>
              val newFields = fields.zip(dataFields).map {
                case ((fieldName, innerOp), (_, data)) =>
                  if (innerOp == NoMutationOp || data == Fix[DataF](GNullF())) {
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
        elementType match {
          case NoMutationOp =>
            NoMutationOp

          case op =>
            GoDown {
              case Fix(GArrayF(elems)) =>
                val result = elems.map(elementType.apply)
                Fix(GArrayF(result))
              case otherData =>
                otherData // should not happen
            }
        }

      case value: ValueF[MutationOp] if value.metadata.tags.nonEmpty =>
        privacyStrategies
          .get(value.metadata.tags)
          .map { strat =>
            val lambda: Fix[DataF] => Fix[DataF] =
              cypherWithContext(strat)
            GoDown(lambda)
          }
          .getOrElse(NoMutationOp)

      case _ => NoMutationOp
    }

    Fix.birecursiveT.cataT(schema)(alg)
  }

  private def cypherWithContext(cypher: PrivacyStrategy)(
      value: Fix[DataF]): Fix[DataF] = {
    cypher(value).fold(
      errors => {
        if (value != Fix[DataF](GNullF())) {
          errors.foreach(err =>
            logger.warn(s"Error while applying privacy on $value : $err"))
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
sealed trait MutationOp extends Serializable {

  def andThen(f: Fix[DataF] => Fix[DataF]): MutationOp

  def apply(gdata: Fix[DataF]): Fix[DataF]
}

/**
  * A specific [[MutationOp]] that goes "down" and apply a function to the data
  *
  * @param apply0 the function to apply
  */
private case class GoDown(apply0: Fix[DataF] => Fix[DataF])
    extends MutationOp
    with Serializable {

  override def andThen(f: Fix[DataF] => Fix[DataF]): MutationOp = {
    GoDown(apply0.andThen(f))
  }

  override def apply(gdata: Fix[DataF]): Fix[DataF] = apply0(gdata)
}

/**
  * NoOp - nothing comes out of this - there's nothing to do !
  */
private case object NoMutationOp extends MutationOp with Serializable {

  override def andThen(f: Fix[DataF] => Fix[DataF]): MutationOp = GoDown(f)

  override def apply(gdata: Fix[DataF]): Fix[DataF] = gdata
}
