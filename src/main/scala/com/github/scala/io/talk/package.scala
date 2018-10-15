package com.github.scala.io

import com.github.scala.io.talk.privacy.PrivacyStrategy.PrivacyStrategies
import com.github.scala.io.talk.privacy._
import com.github.scala.io.talk.{DataF, SchemaF, SparkDataConverter}
import matryoshka.data.Fix
import matryoshka.patterns.EnvT
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame}

package object api {

  type DataWithSchema[A] = EnvT[Fix[SchemaF], DataF, A]

  type SchemaWithPath[A] = EnvT[Fix[SchemaF], DataF, A]

  implicit class DFEncrypt(val df: DataFrame) extends AnyVal {

    def encrypt(schema: Fix[SchemaF],
                privacyStrategies: PrivacyStrategies,
                engine: PrivacyEngine) = {
      engine match {
        case MatryoshkaEngine =>
          val structSchema = df.schema
          val mutated = df.rdd.map { row =>
            val gdata = SparkDataConverter.toGenericData(row, structSchema)
            val result = matryoshkaEngine.transform(schema, gdata, privacyStrategies)
            SparkDataConverter.fromGenericData(result)
          }
          val mutatedSchema = matryoshkaEngine.transformSchema(schema, privacyStrategies)
          val mutatedDataType = Fix.birecursiveT.cataT(mutatedSchema)(SchemaF.schemaFToDataType)
          df.sparkSession.createDataFrame(mutated, mutatedDataType.asInstanceOf[StructType])

        case LambdaEngine =>
          val mutatedSchema = matryoshkaEngine.transformSchema(schema, privacyStrategies)
          val mutatedDataType = Fix.birecursiveT.cataT(mutatedSchema)(SchemaF.schemaFToDataType)
          val preparedLambda = ApplyPrivacyLambda.prepareTransform(schema, privacyStrategies)
          val structSchema = df.schema
          val mutated = df.rdd.map { row =>
            val gdata = SparkDataConverter.toGenericData(row, structSchema)
            val result = preparedLambda.apply(gdata)
            SparkDataConverter.fromGenericData(result)
          }
          df.sparkSession.createDataFrame(mutated, mutatedDataType.asInstanceOf[StructType])

        case CodegenEngine =>
          val expression = ApplyPrivacyExpression(
            schema,
            privacyStrategies,
            df.schema.fieldNames.map(c => df.col(c).expr)
          )

          df.withColumn(
            "structMeUp",
            new Column(
              expression
            )
          ).select("structMeUp.*")
      }
    }
  }

}
