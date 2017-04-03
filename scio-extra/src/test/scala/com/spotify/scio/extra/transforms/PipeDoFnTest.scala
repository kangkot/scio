/*
 * Copyright 2017 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.extra.transforms

import java.nio.file.Files

import com.google.common.base.Charsets
import com.google.common.io.{Files => GFiles}
import com.spotify.scio.testing._

class PipeDoFnTest extends PipelineSpec {

  private val input = Seq("a", "b", "c")

  "PipeDoFn" should "work" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(input).pipe("tr '[:lower:]' '[:upper:]'")
      val p2 = sc.parallelize(input)
        .pipe(Array("tr", "[:lower:]", "[:upper:]"), null, null, null, null)
      p1 should containInAnyOrder (input.map(_.toUpperCase))
      p2 should containInAnyOrder (input.map(_.toUpperCase))
    }
  }

  it should "support environment" in {
    val tmpDir = Files.createTempDirectory("pipedofn")
    val file = tmpDir.resolve("tr.sh")
    GFiles.write("tr $PARG1 $PARG2", file.toFile, Charsets.UTF_8)
    val env = Map("PARG1" -> "[:lower:]", "PARG2" -> "[:upper:]")

    runWithContext { sc =>
      val p1 = sc.parallelize(input).pipe(s"bash $file", env, null)
      val p2 = sc.parallelize(input)
        .pipe(Array("bash", file.toString), env, null, null, null)
      p1 should containInAnyOrder (input.map(_.toUpperCase))
      p2 should containInAnyOrder (input.map(_.toUpperCase))
    }

    Files.delete(file)
    Files.delete(tmpDir)
  }

  it should "support working directory" in {
    val tmpDir = Files.createTempDirectory("pipedofn")
    val file = tmpDir.resolve("tr.sh")
    GFiles.write("tr '[:lower:]' '[:upper:]'", file.toFile, Charsets.UTF_8)

    runWithContext { sc =>
      val p1 = sc.parallelize(input).pipe("bash tr.sh", null, tmpDir.toFile)
      val p2 = sc.parallelize(input)
        .pipe(Array("bash", "tr.sh"), null, tmpDir.toFile, null, null)
      p1 should containInAnyOrder (input.map(_.toUpperCase))
      p2 should containInAnyOrder (input.map(_.toUpperCase))
    }

    Files.delete(file)
    Files.delete(tmpDir)
  }

  it should "support setup and teardown commands" in {
    val tmpDir = Files.createTempDirectory("pipedofn")
    runWithContext { sc =>
      val p1 = sc.parallelize(input).pipe(
        "tr '[:lower:]' '[:upper:]'", null, tmpDir.toFile,
        Seq("touch tmp1.txt", "wc tmp1.txt"),
        Seq("wc tmp1.txt", "rm tmp1.txt"))
      val p2 = sc.parallelize(input).pipe(
        Array("tr", "[:lower:]", "[:upper:]"), null, tmpDir.toFile,
        Seq(Array("touch", "tmp2.txt"), Array("wc", "tmp2.txt")),
        Seq(Array("wc", "tmp2.txt"), Array("rm", "tmp2.txt")))
      p1 should containInAnyOrder (input.map(_.toUpperCase))
      p2 should containInAnyOrder (input.map(_.toUpperCase))
    }

    Files.delete(tmpDir)
  }

  // scalastyle:off no.whitespace.before.left.bracket
  it should "fail if command fails" in {
    // the exception thrown could be UncheckedIOException for broken pipe or IllegalStateException
    // for non-zero exit code, depending on which happens first
    an [Exception] should be thrownBy {
      runWithContext { _.parallelize(input).pipe("ls /non-existent-path") }
    }

    an [Exception] should be thrownBy {
      runWithContext {
        _.parallelize(input).pipe(Array("ls", "/non-existent-path"), null, null, null, null)
      }
    }
  }
  // scalastyle:off no.whitespace.before.left.bracket

  it should "fail if setup commands fail" in {
    // the exception thrown could be UncheckedIOException for broken pipe or IllegalStateException
    // for non-zero exit code, depending on which happens first
    an [Exception] should be thrownBy {
      runWithContext {
        _.parallelize(input).pipe("cat", null, null, Seq("ls /non-exist-path"), null)
      }
    }

    an [Exception] should be thrownBy {
      runWithContext {
        _.parallelize(input).pipe(
          Array("cat"), null, null, Seq(Array("ls", "/non-exist-path")), null)
      }
    }
  }

  it should "fail if teardown commands fail" in {
    // Beam swallows user exception in `@Teardown`
    the [RuntimeException] thrownBy {
      runWithContext {
        _.parallelize(input).pipe("cat", null, null, null, Seq("ls /non-exist-path"))
      }
    } should have message "java.lang.Exception: Exceptions thrown while tearing down DoFns"

    the [RuntimeException] thrownBy {
      runWithContext {
        _.parallelize(input).pipe(
          Array("cat"), null, null, null, Seq(Array("ls", "/non-exist-path")))
      }
    } should have message "java.lang.Exception: Exceptions thrown while tearing down DoFns"
  }

}
