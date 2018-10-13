package com.github.scala.io.talk

import matryoshka.data.Fix

package object privacy {
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

    def apply(data: Fix[DataF]): Either[List[PrivacyApplicationFailure], Fix[DataF]]

    def schema[A](input: SchemaF[A]): SchemaF[A] = input
  }

  case class PrivacyApplicationFailure(reason: String)
}
