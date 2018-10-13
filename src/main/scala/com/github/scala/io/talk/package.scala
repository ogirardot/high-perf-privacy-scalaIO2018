package com.github.scala.io

import com.github.scala.io.talk.{DataF, SchemaF}
import matryoshka.data.Fix
import matryoshka.patterns.EnvT

package object api {

  type DataWithSchema[A] = EnvT[Fix[SchemaF], DataF, A]

  type SchemaWithPath[A] = EnvT[Fix[SchemaF], DataF, A]

}
