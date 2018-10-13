package com.github.scala.io.talk

import com.github.scala.io.api.DataWithSchema
import com.github.scala.io.talk.PrivacyStrategy.PrivacyStrategies
import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.DataType
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


case class InputVariable(name: String) extends AnyVal

sealed trait CatalystOp

case class CatalystCode(code: InputVariable => String, outputVariable: String)

// TODO Spark expression
case class ApplyPrivacyExpression(schema: Fix[SchemaF],
                                  privacyStrategies: PrivacyStrategies,
                                  children: Seq[Expression]) extends Expression {

  override def nullable: Boolean = children.forall(_.nullable)

  // TODO delegate to matryoshka or lambda
  override def eval(input: InternalRow): Any = ??? // privacy "manually" #DelegateToMatryoshka

  /**
    * The mutate schema :
    * TODO mutate the schema through privacy .schema application
    *
    * @return
    */
  override def dataType: DataType = {
    import SchemaF._
    import matryoshka.data._
    // check if any privacy strategy needs to be applied an mutate the schema accordingly
    def ifPrivacy[A](input: SchemaF[A], metadata: ColumnMetadata): SchemaF[A] = {
      privacyStrategies.find { case (tags, _) =>
        // we do not check here if the strat is "applicable" only if the tags match
        tags.size == metadata.tags.size && tags.toSet == metadata.tags.toSet
      }.map { case (_, (_, strategy)) =>
        strategy.schema(input)
      }.getOrElse(input)
    }

    val alg: Algebra[SchemaF, DataType] = {
      case struct@StructF(fields, metadata) =>
        val res = ifPrivacy(struct, metadata)
        schemaFToDataType.apply(res)

      case v@ArrayF(element, metadata) =>
        val res = ifPrivacy(v, metadata)
        schemaFToDataType.apply(res)

      case v: ValueF[DataType] =>
        val res = ifPrivacy(v, v.metadata)
        schemaFToDataType.apply(res)
    }
    Fix.birecursiveT.cataT(schema)(alg)
  }

  // TODO codegen
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = ??? // codegeneration
}

sealed trait PrivacyEngine

case object MatryoshkaEngine extends PrivacyEngine

case object LambdaEngine extends PrivacyEngine

case object CodegenEngine extends PrivacyEngine

object PrivacyStrategy {
  type PrivacyMethod = String

  type PrivacyStrategies = Map[Seq[(String, String)], (PrivacyMethod, PrivacyStrategy)]
}

sealed trait PrivacyStrategy {

  val allowedInputTypes: Set[String]

  def apply(data: Fix[DataF]): Either[List[PrivacyApplicationFailure], Option[Fix[DataF]]]

  def schema[A](input: SchemaF[A]): SchemaF[A] = input
}

case class PrivacyApplicationFailure(reason: String)