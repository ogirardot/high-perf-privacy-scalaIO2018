package com.github.scala.io.talk

import java.io.ByteArrayOutputStream
import java.security.MessageDigest
import java.util.Base64

import com.github.scala.io.talk.privacy._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import com.github.scala.io.api._
import javax.crypto.Cipher
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}
import matryoshka.data.Fix
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

class PrivacyIntegrationTest
    extends FlatSpec
    with Matchers
    with BeforeAndAfterAll {

  var spark: SparkSession = _
  val engines = List(LambdaEngine, CodegenEngine, MatryoshkaEngine)

  override def beforeAll {
    spark = SparkSession
      .builder()
      .appName("Dataframe encryption test")
      .master("local[2]")
      .getOrCreate()
    Logger.getLogger("org.apache.spark.executor.Executor").setLevel(Level.OFF)
  }

  override def afterAll {
    spark.stop()
    Logger.getLogger("org.apache.spark.executor.Executor").setLevel(Level.WARN)
  }

  def testWithEngine(engine: PrivacyEngine): Unit = {
    it should s"handle simple flat datasets with engine: $engine" in {
      val dataset =
        List(("AAAA", "BBBB", "CCCC", "DDDD"), ("EEEE", "FFFF", "GGGG", "HHHH"))
      val input = spark
        .createDataFrame(dataset)
        .toDF("first", "second", "third", "fourth")

      val strategies = Map(
        Seq(("rdfs:type", "http://schema.org/Person#pseudo")) -> new PrivacyStrategy {
          override val allowedInputTypes: Set[String] = Set()

          override def apply(data: Fix[DataF])
            : Either[List[PrivacyApplicationFailure], Fix[DataF]] = {
            data match {
              case Fix(GStringF(value)) =>
                val res = new String(
                  Base64.getEncoder.encode(
                    MessageDigest
                      .getInstance("SHA1")
                      .digest(value.toString.getBytes("UTF-8"))))
                Right(Fix(GStringF(res)))

              case _ =>
                Right(Fix[DataF](GNullF()))
            }
          }
        }
      )

      val schemaFix = Fix[SchemaF](
        StructF(
          List(
            (
              "first",
              Fix(
                StringF(
                  ColumnMetadata.empty.copy(tags =
                    List(("rdfs:type", "http://schema.org/Person#pseudo")))
                ))
            ),
            ("second", Fix(StringF(ColumnMetadata.empty))),
            ("third", Fix(StringF(ColumnMetadata.empty))),
            ("fourth", Fix(StringF(ColumnMetadata.empty)))
          ),
          ColumnMetadata.empty
        ))

      val output = input.encrypt(schemaFix, strategies, engine)
      input.schema should be(input.schema)

      output.first() should be(
        Row("4lEhcqv4zJ9n/dSetsrPLfcbutM=", "BBBB", "CCCC", "DDDD"))
      output.collect()(1) should be(
        Row("xJw04gFKLaJw7q4H1zByb/3dMZY=", "FFFF", "GGGG", "HHHH"))
    }

    it should s"handle complex nested structs with engine: $engine" in {
      val data =
        """{"civility": {"familyName": "MARTIN", "gender": 1, "givenName": "FABIEN"}, "kind": "user#part", "lastUpdatedBy": "FICHECLIENT", "userId": "0211123586445"}"""

      val input = spark.read.json(
        spark.createDataset[String](List(data))(Encoders.STRING))

      val cypher = new PrivacyStrategy {
        override val allowedInputTypes: Set[String] = Set()

        override def apply(data: Fix[DataF])
          : Either[List[PrivacyApplicationFailure], Fix[DataF]] = {
          data match {
            case Fix(value: GValueF[_]) =>
              val res = new String(
                Base64.getEncoder.encode(
                  MessageDigest
                    .getInstance("SHA1")
                    .digest(value.value.toString.getBytes("UTF-8"))))
              Right(Fix(GStringF(res)))

            case _ =>
              Right(Fix[DataF](GNullF()))
          }
        }

        override def schema[A](input: SchemaF[A]): SchemaF[A] =
          StringF(input.metadata)
      }
      val strategies = Map(
        Seq(("rdfs:type", "http://schema.org/Person#pseudo")) -> cypher,
        Seq(("rdfs:type", "http://schema.org/Person#interv")) -> cypher
      )

      val tableSchema = Fix[SchemaF](
        StructF(
          List(
            (
              "civility",
              Fix(StructF(
                List(
                  (
                    "familyName",
                    Fix(StringF(
                      ColumnMetadata.empty.copy(tags =
                        List(("rdfs:type", "http://schema.org/Person#mask")))
                    ))
                  ),
                  (
                    "gender",
                    Fix(LongF(
                      ColumnMetadata.empty.copy(tags =
                        List(("rdfs:type", "http://schema.org/Person#interv")))
                    ))
                  ),
                  (
                    "givenName",
                    Fix(StringF(
                      ColumnMetadata.empty.copy(tags =
                        List(("rdfs:type", "http://schema.org/Person#pseudo")))
                    ))
                  )
                ),
                ColumnMetadata.empty
              ))
            ),
            (
              "kind",
              Fix(
                StringF(
                  ColumnMetadata.empty.copy(tags =
                    List(("rdfs:type", "http://schema.org/Person#pseudo")))
                ))
            ),
            ("lastUpdatedBy", Fix(StringF(ColumnMetadata.empty))),
            ("userId", Fix(StringF(ColumnMetadata.empty)))
          ),
          ColumnMetadata.empty
        ))

      val outputSchemaWithPrivacy = Fix[SchemaF](
        StructF(
          List(
            (
              "civility",
              Fix(StructF(
                List(
                  (
                    "familyName",
                    Fix(StringF(
                      ColumnMetadata.empty.copy(tags =
                        List(("rdfs:type", "http://schema.org/Person#mask")))
                    ))
                  ),
                  (
                    "gender",
                    Fix(StringF(
                      ColumnMetadata.empty.copy(tags =
                        List(("rdfs:type", "http://schema.org/Person#interv")))
                    ))
                  ),
                  (
                    "givenName",
                    Fix(StringF(
                      ColumnMetadata.empty.copy(tags =
                        List(("rdfs:type", "http://schema.org/Person#pseudo")))
                    ))
                  )
                ),
                ColumnMetadata.empty
              ))
            ),
            (
              "kind",
              Fix(
                StringF(
                  ColumnMetadata.empty.copy(tags =
                    List(("rdfs:type", "http://schema.org/Person#pseudo")))
                ))
            ),
            ("lastUpdatedBy", Fix(StringF(ColumnMetadata.empty))),
            ("userId", Fix(StringF(ColumnMetadata.empty)))
          ),
          ColumnMetadata.empty
        ))

      val output = input.encrypt(tableSchema, strategies, engine)

      val schemaAsDT = Fix.birecursiveT.cataT(outputSchemaWithPrivacy)(
        SchemaF.schemaFToDataType)
      output.schema should be(schemaAsDT.asInstanceOf[StructType])

      output.first() should be(
        Row(Row("MARTIN",
                "NWoZK3kTsExUV00Ywo1G5jlUKKs=",
                "ZHmSvjodAvqIT7x0Lu6YDXA8D9g="),
            "HgoSOFkFjIGGbMqW1Uz6LPIwG/M=",
            "FICHECLIENT",
            "0211123586445"))
    }
  }

  engines.foreach(testWithEngine)
}

object SymmetricCrypt extends Serializable {

  def cryptoSecretToBytes(cryptoSecret: String,
                          hexSecret: Boolean): Array[Byte] = {
    if (hexSecret) hexToBytes(cryptoSecret)
    else cryptoSecret.getBytes
  }

  def encrypt(clearText: String,
              cryptoSecret: String,
              cryptoAlgorithm: String,
              hexSecret: Boolean = false): String = {
    val textToEncrypt = Option(clearText).getOrElse("")
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream
    stream.write(textToEncrypt.getBytes)
    var bytes: Array[Byte] = stream.toByteArray
    val cipher: Cipher = Cipher.getInstance(cryptoAlgorithm)
    val cryptoKey: SecretKeySpec =
      new SecretKeySpec(cryptoSecretToBytes(cryptoSecret, hexSecret),
                        cryptoAlgorithm.split("/")(0))
    cipher.init(Cipher.ENCRYPT_MODE, cryptoKey)
    bytes = cipher.doFinal(bytes)
    val useInitializationVector: Boolean =
      if (cryptoAlgorithm.indexOf('/') < 0) false
      else cryptoAlgorithm.split("/")(1).toUpperCase ne "ECB"
    if (useInitializationVector) {
      val iv: Array[Byte] = cipher.getIV
      val out2: Array[Byte] = new Array[Byte](iv.length + 1 + bytes.length)
      out2(0) = iv.length.asInstanceOf[Byte]
      System.arraycopy(iv, 0, out2, 1, iv.length)
      System.arraycopy(bytes, 0, out2, 1 + iv.length, bytes.length)
      bytes = out2
    }
    val cryptedData: String = Base64.getUrlEncoder.encodeToString(bytes)
    return cryptedData
  }

  def hexToBytes(str: String): Array[Byte] = {
    if (str == null) {
      null
    } else if (str.length < 2) {
      null
    } else {
      val len = str.length / 2
      val buffer = new Array[Byte](len)
      var i = 0
      while (i < len) {
        buffer(i) = Integer.parseInt(str.substring(i * 2, i * 2 + 2), 16).toByte
        i = i + 1
      }
      buffer
    }
  }

  def decrypt(
      cryptedData: String,
      cryptoSecret: String,
      cryptoAlgorithm: String,
      hexSecret: Boolean = false
  ): String = {
    val cipher: Cipher = Cipher.getInstance(cryptoAlgorithm)
    val cryptoKey: SecretKeySpec =
      new SecretKeySpec(cryptoSecretToBytes(cryptoSecret, hexSecret),
                        cryptoAlgorithm.split("/")(0))
    val useInitializationVector: Boolean =
      if (cryptoAlgorithm.indexOf('/') < 0) false
      else cryptoAlgorithm.split("/")(1).toUpperCase ne "ECB"
    var cryptedBytes: Array[Byte] = Base64.getUrlDecoder().decode(cryptedData)
    if (useInitializationVector) {
      val ivLen: Int = cryptedBytes(0)
      val ivSpec: IvParameterSpec = new IvParameterSpec(cryptedBytes, 1, ivLen)
      cipher.init(Cipher.DECRYPT_MODE, cryptoKey, ivSpec)
      cryptedBytes =
        cipher.doFinal(cryptedBytes, 1 + ivLen, cryptedBytes.length - 1 - ivLen)
    } else {
      cipher.init(Cipher.DECRYPT_MODE, cryptoKey)
      cryptedBytes = cipher.doFinal(cryptedBytes)
    }
    new String(cryptedBytes)
  }
}
