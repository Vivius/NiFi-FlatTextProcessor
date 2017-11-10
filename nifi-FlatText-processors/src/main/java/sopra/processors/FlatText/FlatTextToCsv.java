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

import javafx.util.Pair;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"FlatTextToCsv"})
@CapabilityDescription("Transforms a positioned flat text into CSV.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class FlatTextToCsv extends AbstractProcessor {
    private HashMap<String, Integer> flatFileFormat;

    public static final PropertyDescriptor HEADERS = new PropertyDescriptor
            .Builder().name("HEADERS")
            .displayName("Headers")
            .description("Path to the .json file headers descriptor.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship CONVERSION_SUCCESS = new Relationship.Builder()
            .name("CONVERSION SUCCESS")
            .description("Relationship used when succeed")
            .build();

    public static final Relationship CONVERSION_FAILED = new Relationship.Builder()
            .name("CONVERSION FAILED")
            .description("Relationship used when failed")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(HEADERS);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(CONVERSION_SUCCESS);
        relationships.add(CONVERSION_FAILED);
        this.relationships = Collections.unmodifiableSet(relationships);

        flatFileFormat = new HashMap<>();

        // Set de test
        /*
        flatFileFormat.put("FILLER", 1);
        flatFileFormat.put("CDTYCRE",5);
        flatFileFormat.put("DTAPPL", 8);
        flatFileFormat.put("CDVSTYCRE-D", 3);
        flatFileFormat.put("CDTYENR", 10);
        flatFileFormat.put("FILLER", 3);
        flatFileFormat.put("CDCTPASS", 1);
        flatFileFormat.put("CDNOTIF", 1);
        flatFileFormat.put("CDCTLEQ", 1);
        flatFileFormat.put("CDPHASE", 25);
        flatFileFormat.put("NOCOMP", 25);
        flatFileFormat.put("NMSERVOR", 15);
        flatFileFormat.put("CDFILOR", 5);
        flatFileFormat.put("CDLOTCRE", 34);
        flatFileFormat.put("CDINSTCRE", 34);
        flatFileFormat.put("CDINSTENR", 25);
        flatFileFormat.put("CDSOC", 5);
        flatFileFormat.put("DVPIVCPT", 3);
        flatFileFormat.put("CDUSER", 15);
        flatFileFormat.put("IDECLI", 20);
        flatFileFormat.put("IDECLX", 20);
        flatFileFormat.put("INDCRZ", 1);
        flatFileFormat.put("CPTIMP", 34);
        flatFileFormat.put("DEVIMP", 3);
        flatFileFormat.put("TYPIMP", 1);
        flatFileFormat.put("INDCLD", 1);
        flatFileFormat.put("NUMCTA", 20);
        flatFileFormat.put("NUMCVT", 20);
        flatFileFormat.put("IDEMFC", 8);
        flatFileFormat.put("TYPMFC", 1);
        flatFileFormat.put("DATVAL", 8);
        flatFileFormat.put("DATOPE", 8);
        flatFileFormat.put("DATRGT", 8);
        flatFileFormat.put("DATOPE", 8);
        flatFileFormat.put("DATRGT", 8);
        flatFileFormat.put("DATFAC", 8);
        flatFileFormat.put("DATDEB", 8);
        flatFileFormat.put("DATFIN", 8);
        flatFileFormat.put("NIVRGP", 1);
        flatFileFormat.put("PERRGP", 1);
        flatFileFormat.put("NBRLDF-D", 6);
        flatFileFormat.put("DEVEXP", 3);
        flatFileFormat.put("NBRDEC-D", 2);
        flatFileFormat.put("MTSCHT-T", 16);
        flatFileFormat.put("MTSCTC-T", 16);
        flatFileFormat.put("MTCLHT-T", 16);
        flatFileFormat.put("MTCLTC-T", 16);
        flatFileFormat.put("MTEXHT-T", 16);
        flatFileFormat.put("MTEXTC-T", 16);
        flatFileFormat.put("MTDCHT-T", 16);
        flatFileFormat.put("MTDCTC-T", 16);
        flatFileFormat.put("MTPCHT-T", 16);
        flatFileFormat.put("MTPCTC-T", 16);
        flatFileFormat.put("CODTTA-1", 4);
        flatFileFormat.put("TAUTAX-D1", 34);
        flatFileFormat.put("MNTTAX-T1", 16);
        flatFileFormat.put("CODTTA-2", 4);
        flatFileFormat.put("TAUTAX-D2", 34);
        flatFileFormat.put("MNTTAX-T2", 16);
        flatFileFormat.put("CODTTA-3", 4);
        flatFileFormat.put("TAUTAX-D3", 7);
        flatFileFormat.put("MNTTAX-T3", 16);
        flatFileFormat.put("CODTTA-4", 4);
        flatFileFormat.put("TAUTAX-D4", 7);
        flatFileFormat.put("MNTTAX-T4", 16);
        flatFileFormat.put("MODRGT", 1);
        flatFileFormat.put("DELRGT", 3);
        flatFileFormat.put("SUPFAC", 1);
        flatFileFormat.put("TYPFCT", 1);
        flatFileFormat.put("NUMFAC", 20);
        flatFileFormat.put("INDFAA", 1);
        flatFileFormat.put("FILLER", 96);
        */

        flatFileFormat.put("IDESOC X(5)", 5);
        flatFileFormat.put("IDECRP", 16);
        flatFileFormat.put("FILLER", 4);
        flatFileFormat.put("NUMlFF", 6);
        flatFileFormat.put("FILLER3", 2);
        flatFileFormat.put("NUCEC-D", 6);
        flatFileFormat.put("TYPTRA", 2);
        flatFileFormat.put("DaTTLT", 8);
        flatFileFormat.put("IDELOT", 8);
        flatFileFormat.put("IDECLI", 20);
        flatFileFormat.put("IDECLX", 20);
        flatFileFormat.put("NUMCTS", 20);
        flatFileFormat.put("NUMCPS", 20);
        flatFileFormat.put("IDEOPT", 8);
        flatFileFormat.put("IDECOM", 8);
        flatFileFormat.put("CODORI", 5);
        flatFileFormat.put("DATOPE", 8);
        flatFileFormat.put("HEUOPE", 6);
        flatFileFormat.put("MNTEVE-D", 15);
        flatFileFormat.put("NBREVE-D", 9);
        flatFileFormat.put("DEVEVE", 3);
        flatFileFormat.put("SENIMP", 1);
        flatFileFormat.put("INDEXO", 1);
        flatFileFormat.put("PRTTMS-D", 7);
        flatFileFormat.put("REFEVE", 40);
        flatFileFormat.put("LSTCRI", 250);
        flatFileFormat.put("ORIECT", 1);
        flatFileFormat.put("FILLER2", 41);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final AtomicReference<String> data = new AtomicReference<>();
        data.set("");

        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        // Lecture des données en entrée en conversion.
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                try{
                    // String content = IOUtils.toString(in, "UTF-8");
                    Scanner scanner = new Scanner(in);
                    while (scanner.hasNextLine()) {
                        String line = scanner.nextLine();
                        int cursor = 0;
                        for(Integer size : flatFileFormat.values()) {
                            if(cursor + size < line.length())
                                data.set(data.get() + line.substring(cursor, cursor + size));
                            cursor += size;
                        }
                        data.set(data.get() + "\nLength =" + line.length() + "\n");
                    }

                } catch(Exception ex){
                    ex.printStackTrace();
                    getLogger().error("Failed to read input file.");
                }
            }
        });

        // Ecriture des données dans le flowFile.
        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write(data.get().getBytes());
            }
        });

        session.transfer(flowFile, CONVERSION_SUCCESS);
    }
}
