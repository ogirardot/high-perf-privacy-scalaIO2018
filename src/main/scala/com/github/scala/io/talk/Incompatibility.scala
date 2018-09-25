package com.github.scala.io.talk

import matryoshka.data.Fix

case class Incompatibility(schema: Fix[SchemaF], data: Fix[DataF])
