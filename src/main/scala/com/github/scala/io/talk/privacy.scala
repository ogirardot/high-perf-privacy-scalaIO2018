package com.github.scala.io.talk

import com.github.scala.io.api.DataWithSchema
import com.github.scala.io.talk.privacy.PrivacyStrategy.PrivacyStrategies
import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import scalaz._

// TODO Matryoshka Engine & Lambda Engine
object ApplyPrivacy {

  def transform(schema: Fix[SchemaF],
                data: Fix[DataF],
                privacyStrategies: PrivacyStrategies): Fix[DataF] = {
    val privacyAlg: AlgebraM[\/[Incompatibility, ?], DataWithSchema, Fix[DataF]] = ???

    (schema, data).hyloM[\/[Incompatibility, ?], DataWithSchema, Fix[DataF]](privacyAlg, DataF.zipWithSchema) match {
      case -\/(incompatibilities) =>
        throw new IllegalStateException(
          s"Found incompatibilities between the observed data and its expected schema : $incompatibilities")

      case \/-(result) =>
        result
    }
  }

  // TODO same as com.github.scala.io.talk.ApplyPrivacyExpression.dataType without spark
  def transformSchema(schema: Fix[SchemaF]): Fix[SchemaF] = ???
}