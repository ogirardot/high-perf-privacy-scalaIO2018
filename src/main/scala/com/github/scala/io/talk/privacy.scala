package com.github.scala.io.talk

import com.github.scala.io.api.DataWithSchema
import scalaz._
import Scalaz._
import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.DataType

// TODO Matryoshka Engine
object ApplyPrivacy {

  def transform(schema: Fix[SchemaF],
                data: Fix[DataF],
                privacyStrategies: Set[PrivacyStrategy]): Fix[DataF] = {
    val privacyAlg: AlgebraM[\/[Incompatibility, ?], DataWithSchema, Fix[DataF]] = ???

    (schema, data).hyloM[\/[Incompatibility, ?], DataWithSchema, Fix[DataF]](privacyAlg, DataF.zipWithSchema) match {
      case -\/(incompatibilities) =>
        throw new IllegalStateException(
          s"Found incompatibilities between the observed data and its expected schema : $incompatibilities")

      case \/-(result) =>
        result
    }
  }

  def transformSchema(schema: Fix[SchemaF]): Fix[SchemaF] = ???
}


case class InputVariable(name: String) extends AnyVal

sealed trait CatalystOp
case class CatalystCode(code: InputVariable => String, outputVariable: String)

case class ApplyPrivacyExpression(schema: Fix[SchemaF],
                                  privacyStrategies: Set[PrivacyStrategy],
                                   children: Seq[Expression]) extends Expression {

  override def nullable: Boolean = children.forall(_.nullable)

  override def eval(input: InternalRow): Any = ??? // privacy "manually" #DelegateToMatryoshka

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = ??? // codegeneration

  /**
    * The mutate schema :
    * @return
    */
  override def dataType: DataType = {
    import SchemaF._
    import matryoshka.implicits._
    import matryoshka.data._
    def ifPrivacy[A](input: SchemaF[A], metadata: ColumnMetadata) = {
      privacyStrategies.head.schema(input)
    }

    val alg:  Algebra[SchemaF, DataType] = {
      case struct @ StructF(fields, metadata) =>
        val privaciedSchema = ifPrivacy(struct, metadata)
        schemaFToDataType.apply(privaciedSchema)

      case v@ ArrayF(element, metadata) =>
        val privaciedSchema = ifPrivacy(v, metadata)
        schemaFToDataType.apply(v)

      case v: ValueF[DataType] =>
        val privaciedSchema = ifPrivacy(v, v.metadata)
        schemaFToDataType.apply(privaciedSchema)
    }
    Fix.birecursiveT.cataT(schema)(alg)
  }
}

sealed trait PrivacyEngine
case object MatryoshkaEngine extends PrivacyEngine
case object LambdaEngine extends PrivacyEngine
case object CodegenEngine extends PrivacyEngine

sealed trait PrivacyStrategy {

  type PrivacyMethod = String

  type PrivacyStrategies = Map[Seq[(String, String)], (PrivacyMethod, PrivacyStrategy)]

  val allowedInputTypes: Set[String]

  def apply(data: Fix[DataF]): Either[List[PrivacyApplicationFailure], Option[Fix[DataF]]]

  def schema[A](input: SchemaF[A]): SchemaF[A] = input
}

case class PrivacyApplicationFailure(reason: String)