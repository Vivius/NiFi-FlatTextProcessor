/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sopra.processors.FlatText;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.Assert.assertTrue;


public class FlatTextToCsvTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(FlatTextToCsv.class);
    }

    @Test
    public void testProcessor() throws IOException {
        Path file = Paths.get("C:\\Users\\Vincent\\Desktop\\NiFi\\data\\CREPER01-ALL.DAT");
        testRunner.setProperty(FlatTextToCsv.HEADERS, "HEADER");

        testRunner.enqueue(Files.readAllBytes(file));
        testRunner.run(1);
        testRunner.assertQueueEmpty();

        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(FlatTextToCsv.CONVERSION_SUCCESS);
        assertTrue("Conversion failed", results.size() == 1);
        MockFlowFile result = results.get(0);

        System.out.println("Résultat = \n" + IOUtils.toString(testRunner.getContentAsByteArray(result)));
        System.out.println("Longueur du résultat = " + result.getSize());

        // Test attributes and content
        /*
        result.assertAttributeEquals(JsonProcessor.MATCH_ATTR, "nifi rocks");
        result.assertContentEquals("nifi rocks");
        */
    }

}
