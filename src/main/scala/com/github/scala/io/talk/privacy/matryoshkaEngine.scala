package com.github.scala.io.talk.privacy

import com.github.scala.io.api.DataWithSchema
import com.github.scala.io.talk._
import com.github.scala.io.talk.privacy.PrivacyStrategy.PrivacyStrategies
import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import matryoshka.patterns.EnvT
import org.slf4j.LoggerFactory
import scalaz._

object matryoshkaEngine {

  private val logger = LoggerFactory.getLogger("ApplyPrivacyMatryoshka")

  def transform(schema: Fix[SchemaF],
                data: Fix[DataF],
                privacyStrategies: PrivacyStrategies): Fix[DataF] = {
    import Scalaz._
    val privacyAlg
      : AlgebraM[\/[Incompatibility, ?], DataWithSchema, Fix[DataF]] = {

      case EnvT((Fix(StructF(fieldsType, meta)), gdata @ GStructF(fields))) =>
        Fix(gdata).right

      case EnvT((Fix(ArrayF(elementType, meta)), gdata @ GArrayF(elems))) =>
        Fix(gdata).right

      case EnvT((vSchema, value)) =>
        val tags = vSchema.unFix.metadata.tags
        val fixedValue = Fix(value)
        privacyStrategies
          .get(tags)
          .map { privacyStrategy =>
            privacyStrategy.applyOrFail(fixedValue)(logger.error)
          }
          .getOrElse(fixedValue)
          .right
    }

    (schema, data).hyloM[\/[Incompatibility, ?], DataWithSchema, Fix[DataF]](
      privacyAlg,
      DataF.zipWithSchema) match {
      case -\/(incompatibilities) =>
        throw new IllegalStateException(
          s"Found incompatibilities between the observed data and its expected schema : $incompatibilities")

      case \/-(result) =>
        result
    }
  }

  // TODO same as com.github.scala.io.talk.ApplyPrivacyExpression.dataType without spark
  def transformSchema(schema: Fix[SchemaF],
                      privacyStrategies: PrivacyStrategies): Fix[SchemaF] = {
    def alg: Algebra[SchemaF, Fix[SchemaF]] = s => changeSchema(privacyStrategies, Fix(s))

    schema.cata(alg)
  }

  def changeSchema(privacyStrategies: PrivacyStrategies,
                   schemaF: Fix[SchemaF]): Fix[SchemaF] = {
    val s = schemaF.unFix
    privacyStrategies
      .find {
        case (tags, _) => tags.size == s.metadata.tags.size && tags.toSet == s.metadata.tags.toSet
      }
      .fold(schemaF) { case (_, strategy) => Fix(strategy.schema(s)) }
  }

}
