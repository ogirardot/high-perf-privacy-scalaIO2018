package com.github.scala.io.talk.privacy

import com.github.scala.io.talk._
import com.github.scala.io.talk.privacy.PrivacyStrategy.PrivacyStrategies
import matryoshka.Algebra
import matryoshka.data.Fix
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.{
  CodegenContext,
  ExprCode
}
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}
import org.apache.spark.sql.utils.SmartRow
import org.apache.spark.unsafe.types.UTF8String

case class InputVariable(name: String) extends AnyVal

sealed trait CatalystOp

case class CatalystCode(code: InputVariable => String, outputVariable: String)
    extends CatalystOp

case object NoOp extends CatalystOp

case class ApplyMe(lambda: Any => Any) {
  def apply(value: Any): Any = lambda(value)
}

case class ApplyPrivacyExpression(schema: Fix[SchemaF],
                                  privacyStrategies: PrivacyStrategies,
                                  children: Seq[Expression])
    extends Expression {

  type FieldName = String
  type FieldWithInfos = (DataType, CatalystOp)

  override def nullable: Boolean = children.forall(_.nullable)

  override def eval(input: InternalRow): Any = {
    // privacy "manually" #DelegateToMatryoshka
    val structType = Fix.birecursiveT
      .cataT(schema)(SchemaF.schemaFToDataType)
      .asInstanceOf[StructType]
    val gdata = SparkDataConverter.toGenericData(
      Row(input.toSeq(structType): _*),
      structType)
    val res = matryoshkaEngine.transform(schema, gdata, privacyStrategies)
    SmartRow.fromSeq(SparkDataConverter.fromGenericData(res).toSeq.map {
      case s: String => UTF8String.fromString(s)
      case a         => a
    })
  }

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
    def ifPrivacy[A](input: SchemaF[A],
                     metadata: ColumnMetadata): SchemaF[A] = {
      privacyStrategies
        .find {
          case (tags, _) =>
            // we do not check here if the strat is "applicable" only if the tags match
            tags.size == metadata.tags.size && tags.toSet == metadata.tags.toSet
        }
        .map {
          case (_, strategy) =>
            strategy.schema(input)
        }
        .getOrElse(input)
    }

    val alg: Algebra[SchemaF, (Boolean, DataType)] = {
      case struct @ StructF(fields, metadata) =>
        val res = StructType(fields.map {
          case (name, (isNullable, field)) =>
            StructField(name, field, isNullable)
        })
        (metadata.nullable, res)

      case v @ ArrayF(element, metadata) =>
        val res = ArrayType(element._2, element._1)
        (metadata.nullable, res)

      case v: ValueF[(Boolean, DataType)] =>
        val res = ifPrivacy(v, v.metadata)
        (v.metadata.nullable,
         schemaFToDataType.apply(schemaFScalazFunctor.map(res)(_._2)))
    }
    val res = Fix.birecursiveT.cataT(schema)(alg)
    res._2
  }

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode): ExprCode = {
    import SchemaF._

    val input = "inputadapter_row_0"

    val privacyAlg: Algebra[SchemaF, FieldWithInfos] = {
      case StructF(fieldsWithDataTypes, metadata) =>
        val tmp = ctx.freshName("toto")
        val inputTmp = ctx.freshName("inputTmp")

        val CatalystCode(fieldsCode, _) =
          generateCodeForStruct(ctx, fieldsWithDataTypes, tmp)
        val outputDataType = fieldsToSparkDataType(fieldsWithDataTypes)
        val outputDataTypeForCodegen =
          ctx.addReferenceObj("outputDataType", outputDataType)
        val code = (inputVariable: InputVariable) => {
          s"""
             org.apache.spark.sql.catalyst.InternalRow  $inputTmp = (org.apache.spark.sql.catalyst.InternalRow ) ${inputVariable.name};
             org.apache.spark.sql.utils.SmartRow $tmp = (org.apache.spark.sql.utils.SmartRow) org.apache.spark.sql.utils.SmartRow.fromSeq($inputTmp.toSeq($outputDataTypeForCodegen));
             ${fieldsCode.apply(InputVariable(tmp))}
            """
        }
        (outputDataType, CatalystCode(code, tmp))

      case ArrayF(elementType, metadata) =>
        val (elementSparkDataType, innerOp) = elementType
        val arrayDataType = ArrayType(elementSparkDataType)
        val resOp = if (innerOp == NoOp) {
          innerOp
        } else {
          val tags = metadata.tags
          val elementTypeBoxed = ctx.boxedType(elementSparkDataType)
          val tpeName = ctx.addReferenceObj("tpe", elementSparkDataType)
          val CatalystCode(innerCode, innerOuput) = innerOp
          val tempVariable = ctx.freshName("tmp")
          val pos = ctx.freshName("pos")
          val output = ctx.freshName("output")
          val code = (inputVariable: InputVariable) =>
            s"""
              Object[] $tempVariable = new Object[${inputVariable.name}.numElements()];
              for (int $pos = 0; $pos < ${inputVariable.name}.numElements(); $pos++) {
                if (!${inputVariable.name}.isNullAt($pos)) {
                  ${innerCode.apply(
              InputVariable(s"(${inputVariable.name}.get($pos, $tpeName))")
            )}
                  $tempVariable[$pos] = $innerOuput;
                } else {
                  $tempVariable[$pos] = null;
                }
              }
              org.apache.spark.sql.catalyst.util.ArrayData $output = new org.apache.spark.sql.catalyst.util.GenericArrayData($tempVariable);
            """
          CatalystCode(code, output)
        }
        (arrayDataType, resOp)

      case valueColumnSchema: ValueF[FieldWithInfos]
          if valueColumnSchema.metadata.tags.nonEmpty =>
        val tags: List[(String, String)] = valueColumnSchema.metadata.tags
        val elementDataType: DataType =
          schemaFToDataType.apply(schemaFScalazFunctor(valueColumnSchema)(_._1))
        val resOp = privacyStrategies
          .get(tags)
          .map { strat =>
            val output = ctx.freshName("output")
            val outputSchema = strat.schema(valueColumnSchema)
            val outputDataType =
              schemaFToDataType.apply(schemaFScalazFunctor(outputSchema)(_._1))
            val javaType = ctx.boxedType(outputDataType)
            val cypherInSpark =
              ctx.addReferenceObj("cypherMe", transTypePrivacyStrategy(strat))
            val code = (inputVariable: InputVariable) => s"""
                      $javaType $output = ($javaType) $cypherInSpark.apply(${inputVariable.name});
                    """
            CatalystCode(code, output)

          }
          .getOrElse(NoOp)
        (elementDataType, resOp)

      case value: ValueF[FieldWithInfos] if value.metadata.tags.isEmpty =>
        val elementDataType =
          schemaFToDataType.apply(schemaFScalazFunctor(value)(_._1))
        (elementDataType, NoOp)
    }

    ev.copy(code = Fix.birecursiveT.cataT(schema)(privacyAlg) match {
      case (_, NoOp) =>
        s"""
           final boolean ${ev.isNull} = ($input != null) ? false : true;
           final InternalRow  ${ev.value} = $input;
          """

      case rec @ (topLevelDataType, CatalystCode(method, outputVariable)) =>
        s"""
              ${method(InputVariable(input))}
              final boolean ${ev.isNull} = ($input != null) ? false : true;
              final InternalRow ${ev.value} = $outputVariable;
            """
    })
  }

  /**
    * Generate Catalyst Code for a struct
    *
    * @param fieldsWithDataType all the fields of the inner struct
    * @param tmp                the variable we want to mutate
    * @return the code necessary to mutate a struct
    */
  def generateCodeForStruct(
      ctx: CodegenContext,
      fieldsWithDataType: Seq[(FieldName, FieldWithInfos)],
      tmp: String
  ): CatalystCode = {
    fieldsWithDataType.zipWithIndex.foldLeft(CatalystCode(_ => "", tmp)) {
      case (buffer, ((fieldName, (elementDataType, op)), idx)) =>
        if (op == NoOp) {
          buffer
        } else {
          val CatalystCode(code, intermediateOutput) = op
          val fieldTpe = ctx.addReferenceObj("dt", elementDataType)
          // we need top extract the data properly according to its element type
          val fieldExtractor = elementDataType match {
            case StructType(fields) =>
              val numFields = fields.length
              s"getStruct($idx, $numFields)"
            case ArrayType(_, _) =>
              s"getArray($idx)"
            case _ =>
              s"get($idx, $fieldTpe)"
          }

          CatalystCode(
            (inputVariable: InputVariable) =>
              s"""
                 ${buffer.code(inputVariable)}
                 if (!${inputVariable.name}.isNullAt($idx)) {
                 // $fieldName
                   ${code.apply(
                InputVariable(s"${inputVariable.name}.$fieldExtractor"))}
                   $tmp.update($idx, $intermediateOutput);
                 }
              """,
            tmp
          )
        }
    }
  }

  def transTypePrivacyStrategy(strat: PrivacyStrategy): ApplyMe = {
    ApplyMe((value: Any) => {
      strat
        .apply(wrap(value))
        .fold(
          errors => {
            errors.foreach(println)
            null
          },
          x => unwrap(x.unFix)
        )
    })
  }

  def wrap(input: Any): Fix[DataF] = {
    input match {
      case null                  => Fix(GNullF())
      case a: String             => Fix(GStringF(a))
      case a: Long               => Fix(GLongF(a))
      case a: java.lang.Long     => Fix(GLongF(a))
      case a: UTF8String         => Fix(GStringF(a.toString))
      case a: Double             => Fix(GDoubleF(a))
      case a: java.lang.Double   => Fix(GDoubleF(a))
      case a: Int                => Fix(GIntF(a))
      case a: java.lang.Integer  => Fix(GIntF(a))
      case a: Float              => Fix(GFloatF(a))
      case a: java.lang.Float    => Fix(GFloatF(a))
      case a: java.sql.Date      => Fix(GDateF(a))
      case a: java.sql.Timestamp => Fix(GTimestampF(a))
      case _ =>
        throw new UnsupportedOperationException(
          s"Input data is not supported : $input of type ${input.getClass}")
    }
  }

  def unwrap[A](input: DataF[A]): Any = input match {
    case GNullF()       => null
    case x: GStringF[A] => UTF8String.fromString(x.value)
    case x: GValueF[A]  => x.value
    case _ =>
      throw new UnsupportedOperationException(
        s"Input data is not supported : $input of type ${input.getClass}")
  }

  /**
    * Re-construct the Spark StructType data type, from the fields after privacy
    *
    * @param fieldsWithDataType all the fields transformed after privacy
    * @return
    */
  private def fieldsToSparkDataType(
      fieldsWithDataType: List[(FieldName, FieldWithInfos)]): StructType = {
    StructType(fieldsWithDataType.map {
      case (fieldName, (fieldDataType, _)) =>
        StructField(fieldName, fieldDataType, nullable = true)
    })
  }
}
