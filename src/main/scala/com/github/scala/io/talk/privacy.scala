package com.github.scala.io.talk

import matryoshka.data.Fix

// TODO Matryoshka Engine
object ApplyPrivacy {

  def transform(schema: Fix[SchemaF],
                data: Fix[DataF],
                privacyStrategies: Set[PrivacyStrategy]): Fix[DataF] = ???

  def transformSchema(schema: Fix[SchemaF]): Fix[SchemaF] = ???
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

  def schema(input: Fix[SchemaF]): Option[Fix[SchemaF]] = Some(input)
}

case class PrivacyApplicationFailure(reason: String)