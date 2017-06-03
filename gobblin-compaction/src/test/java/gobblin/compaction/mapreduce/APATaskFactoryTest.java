/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gobblin.compaction.mapreduce;

import java.io.File;
import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

import lombok.extern.slf4j.Slf4j;

import gobblin.compaction.source.WordCountSource;
import gobblin.configuration.ConfigurationKeys;
import gobblin.runtime.api.JobExecutionResult;
import gobblin.runtime.embedded.EmbeddedGobblin;


@Slf4j
public class APATaskFactoryTest {

  @Test
  public void test() throws Exception {

    File inputSuperPath = new File("/tmp/apa/input");
    File outputSuperPath = new File("/tmp/apa/output");

    log.info ("#####  inputDir " + inputSuperPath.toPath().toString());

    log.info ("#####  output " + outputSuperPath.toPath().toString());

    EmbeddedGobblin embeddedGobblin = new EmbeddedGobblin("WordCounter")
        .setConfiguration(ConfigurationKeys.SOURCE_CLASS_KEY, WordCountSource.class.getName())
        .setConfiguration(WordCountSource.OUTPUT_LOCATION, outputSuperPath.getAbsolutePath())
        .setConfiguration(ConfigurationKeys.SOURCE_FILEBASED_DATA_DIRECTORY, "/tmp/apa/input")
        .setConfiguration(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, "file:///")
        .setConfiguration(ConfigurationKeys.STATE_STORE_ENABLED, "true")
        .setConfiguration(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY, "/tmp/apa/state_store");
    JobExecutionResult result = embeddedGobblin.run();
    Assert.assertTrue(result.isSuccessful());
  }

}
