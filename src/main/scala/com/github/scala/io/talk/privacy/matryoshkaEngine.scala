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
    val privacyAlg: AlgebraM[\/[Incompatibility, ?], DataWithSchema, Fix[DataF]] = {

      case EnvT((Fix(StructF(fieldsType, meta)), gdata @ GStructF(fields))) =>
        val tags = meta.tags

        privacyStrategies.foldLeft(Fix(gdata)) {
          case (item, (keys, cypher)) =>
            if (keys.map(tags.contains).reduce(_ && _)) {
              logger.debug(
                s"matched item : $item for target $keys - found tags $tags")
              Fix(GStructF(fields.map { case (name, d) => name -> cypher.get(d)(logger.error) }))
            } else item
        }.right

      case EnvT((Fix(ArrayF(elementType, meta)), gdata @ GArrayF(elems))) =>
        val tags = meta.tags

        privacyStrategies.foldLeft(Fix(gdata)) {
          case (item, (keys, cypher)) =>
            if (keys.map(tags.contains).reduce(_ && _)) {
              logger.debug(
                s"matched item : $item for target $keys - found tags $tags")
              Fix(GArrayF(elems.map(cypher.get(_)(logger.error))))
            } else item
        }.right
      case  EnvT((vSchema, value)) /*is a value*/ =>
        val tags = vSchema.unFix.metadata.tags

        privacyStrategies.foldLeft(data) {
          case (item, (keys, cypher)) =>
            if (keys.map(tags.contains).reduce(_ && _)) {
              logger.debug(
                s"matched item : $item for target $keys - found tags $tags")
              cypher.get(Fix(value))(logger.error)
            } else item
        }.right
    }

    (schema, data).hyloM[\/[Incompatibility, ?], DataWithSchema, Fix[DataF]](privacyAlg, DataF.zipWithSchema) match {
      case -\/(incompatibilities) =>
        throw new IllegalStateException(
          s"Found incompatibilities between the observed data and its expected schema : $incompatibilities")

      case \/-(result) =>
        result
    }
  }

  // TODO same as com.github.scala.io.talk.ApplyPrivacyExpression.dataType without spark
  def transformSchema(schema: Fix[SchemaF], privacyStrategies: PrivacyStrategies): Fix[SchemaF] = {
    def alg: Algebra[SchemaF, Fix[SchemaF]] = {
      case s @ StructF(_, metadata) =>
        val newSchema = privacyStrategies.find { case (tags, _) =>
                tags.size == metadata.tags.size && tags.toSet == metadata.tags.toSet
              }.map { case (_, strategy) =>
                strategy.schema(s)
              }.getOrElse(s)
        Fix(newSchema)
      case s @ ArrayF(_, metadata) =>
        val newSchema = privacyStrategies.find { case (tags, _) =>
        tags.size == metadata.tags.size && tags.toSet == metadata.tags.toSet
      }.map { case (_, strategy) =>
        strategy.schema(s)
      }.getOrElse(s)
        Fix(newSchema)

      case s =>
        val newSchema = privacyStrategies.find { case (tags, _) =>
          tags.size == s.metadata.tags.size && tags.toSet == s.metadata.tags.toSet
        }.map { case (_, strategy) =>
          strategy.schema(s)
        }.getOrElse(s)
        Fix(newSchema)
    }

    schema.cata(alg)
  }
}
