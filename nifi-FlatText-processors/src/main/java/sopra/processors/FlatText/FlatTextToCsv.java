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

    private List<Column> fileFormat;

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

        // Exemple static
        fileFormat = new ArrayList<>();
        fileFormat.add(new Column("nctycre", 1, 5));
        fileFormat.add(new Column("dtappl", 6, 8));
        fileFormat.add(new Column("cdvstycre", 14, 3));
        fileFormat.add(new Column("cdtyenr", 17, 10));
        fileFormat.add(new Column("cdlotcre", 103, 34));
        fileFormat.add(new Column("cdinstcre", 137, 34));
        fileFormat.add(new Column("mtscht", 409, 16));
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
        data.set(new String());

        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        System.out.println("Nom fichier = " + flowFile.getAttribute("filename"));

        // Lecture des données en entrée en conversion.
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                try{
                    // String content = IOUtils.toString(in, "UTF-8");

                    // Ecriture de l'entête
                    for(Column c : fileFormat)
                        data.set(data.get() + c.getName() + ";");
                    data.set(data.get().substring(0, data.get().length()-1) + "\n");

                    Scanner scanner = new Scanner(in);
                    while (scanner.hasNextLine()) {
                        String line = scanner.nextLine(); // Ligne courante
                        for(Column c : fileFormat) {
                            if(c.getIndex() + c.getLength() < line.length())
                                data.set(data.get() + line.substring(c.getIndex(), c.getIndex() + c.getLength()).trim() + ";");
                        }
                        // On retire le point virgule en fin de ligne et on revient à la ligne.
                        data.set(data.get().substring(0, data.get().length()-1) + "\n");
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
