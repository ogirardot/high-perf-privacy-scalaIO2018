package com.github.scala.io.talk

import matryoshka.data.Fix

package object privacy {
  sealed trait PrivacyEngine

  case object MatryoshkaEngine extends PrivacyEngine

  case object LambdaEngine extends PrivacyEngine

  case object CodegenEngine extends PrivacyEngine

  object PrivacyStrategy {
    type PrivacyStrategies = Map[Seq[(String, String)], PrivacyStrategy]
  }

  sealed trait PrivacyStrategy {

    val allowedInputTypes: Set[String]

    def apply(data: Fix[DataF]): Either[List[PrivacyApplicationFailure], Fix[DataF]]

    def schema[A](input: SchemaF[A]): SchemaF[A] = input

    def get(value: Fix[DataF])(onError: String => Unit): Fix[DataF] = apply(value).fold (
      errors => {
        if (value != Fix[DataF](GNullF())) {
          errors.foreach(err =>  println(s"Error while applying privacy on $value : $err"))
        }
        Fix[DataF](GNullF())
      }
      ,
      identity
    )
  }

  case class PrivacyApplicationFailure(reason: String)
}
