/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdc.plugins.transform;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import co.cask.cdap.format.StructuredRecordStringConverter;
import co.cask.cdc.common.AvroConverter;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 *
 */
@Plugin(type = "transform")
@Name("CDCNormalizer")
public class Normalizer extends Transform<StructuredRecord, StructuredRecord> {
  private static Logger LOG = LoggerFactory.getLogger(Normalizer.class);
  private static Gson GSON = new Gson();
  private static final Schema CDC_DDL_SCHEMA = Schema.recordOf("DDLRecord",
                                                               Schema.Field.of("table_name", Schema.of(Schema.Type.STRING)),
                                                               Schema.Field.of("schemaHashId", Schema.of(Schema.Type.LONG)),
                                                               Schema.Field.of("schema", Schema.of(Schema.Type.STRING)));

  private final NormalizerConfig config;

  private URL schemaRegistryURL;

  private Table<String, Long, org.apache.avro.Schema> schemaCache;

  public Normalizer(NormalizerConfig config) {
    this.config = config;
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    // TODO Get the schema from service
    /*
      String[] serviceId = config.schemaServiceName.split(":");
      schemaRegistryURL = context.getServiceURL(serviceId[0], serviceId[1]);
      schemaCache = HashBasedTable.create();
    */
    schemaCache = HashBasedTable.create();
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    LOG.info("Input StructuredRecord is {}", GSON.toJson(input));

    byte[] message = input.get(config.getInputField());
    if (message == null) {
      throw new IllegalStateException(String.format("Input record does not contain the field '%s'.",
                                                    config.getInputField()));
    }

    String messageBody = new String(message, StandardCharsets.UTF_8);

    LOG.info("XXX Received message body is {}", messageBody);
    if (messageBody.contains("generic_wrapper") && messageBody.contains("oracle.goldengate")) {
      // Current message is the schema of generic AVRO wrapper. Can be ignored.
      /*
        {
          "type" : "record",
          "name" : "generic_wrapper",
          "namespace" : "oracle.goldengate",
          "fields" : [ {
            "name" : "table_name",
            "type" : "string"
          }, {
            "name" : "schema_fingerprint",
            "type" : "long"
          }, {
            "name" : "payload",
            "type" : "bytes"
          } ]
        }
       */
      LOG.info("Ignoring the Generic Avro Wrapper schema.");
      return;
    }

    if (messageBody.contains("\"type\" : \"record\"")) {
      // Current message is the Schema of the table
      // Create DDLRecord for it.
      LOG.info("Emitting schema for the table {}", messageBody);
      JsonObject schemaObj = GSON.fromJson(messageBody, JsonObject.class);
      String namespaceName = schemaObj.get("namespace").getAsString();
      String tableName = schemaObj.get("name").getAsString();

      org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(messageBody);
      long schemaFingerPrint = SchemaNormalization.parsingFingerprint64(avroSchema);
      LOG.info("Populating Schema cache with namespace {}, tableName {}, and fingerPrint {}", namespaceName, tableName,
               schemaFingerPrint);

      String namespacedTableName = namespaceName + "." + tableName;
      schemaCache.put(namespaceName + "." + tableName, schemaFingerPrint, avroSchema);

      StructuredRecord.Builder builder = StructuredRecord.builder(CDC_DDL_SCHEMA);
      builder.set("schemaHashId", schemaFingerPrint);
      builder.set("schema", messageBody);
      builder.set("table_name", namespacedTableName);
      // TODO Do Not emit anything for now
      emitter.emit(builder.build());
      return;
    }

    // Current message is the Wrapped Avro binary message
    org.apache.avro.Schema avroGenericWrapperSchema = getGenericWrapperSchema();
    GenericRecord genericRecord = getRecord(message, avroGenericWrapperSchema);
    // TODO Generic Avro wrapped message does not have namespace name.
    String tableName = genericRecord.get("table_name").toString();
    long schameHashId = (Long) genericRecord.get("schema_fingerprint");
    byte[] payload = genericRecord.get("payload") instanceof ByteBuffer
      ? Bytes.toBytes((ByteBuffer) genericRecord.get("payload"))
      : (byte[]) genericRecord.get("payload");
    LOG.info("Got tableName {} and fingerPrint {} in wrapped schema.", tableName, schameHashId);
    org.apache.avro.Schema avroSchema = schemaCache.get(tableName, schameHashId);

    StructuredRecord structuredRecord = AvroConverter.fromAvroRecord(getRecord(payload, avroSchema),
                                                                     AvroConverter.fromAvroSchema(avroSchema));

    LOG.info("Emitting Structured Record {}", StructuredRecordStringConverter.toJsonString(structuredRecord));
    // TODO Do Not emit anything for now
    emitter.emit(structuredRecord);
  }

  private org.apache.avro.Schema getGenericWrapperSchema() {
    String avroGenericWrapperSchema = "{\n" +
      "          \"type\" : \"record\",\n" +
      "          \"name\" : \"generic_wrapper\",\n" +
      "          \"namespace\" : \"oracle.goldengate\",\n" +
      "          \"fields\" : [ {\n" +
      "            \"name\" : \"table_name\",\n" +
      "            \"type\" : \"string\"\n" +
      "          }, {\n" +
      "            \"name\" : \"schema_fingerprint\",\n" +
      "            \"type\" : \"long\"\n" +
      "          }, {\n" +
      "            \"name\" : \"payload\",\n" +
      "            \"type\" : \"bytes\"\n" +
      "          } ]\n" +
      "        }";
    return new org.apache.avro.Schema.Parser().parse(avroGenericWrapperSchema);
  }

  private GenericRecord getRecord(byte[] message, org.apache.avro.Schema schema) throws IOException {
    GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
    return datumReader.read(null, DecoderFactory.get().binaryDecoder(message, null));
  }

  public static class NormalizerConfig extends PluginConfig {
    @Name("inputField")
    @Description("Input field containing the payload of the message. Defaults to 'message'.")
    private String inputField;

    @Name("schemaServiceName")
    @Description("Name of the schema registry service. Name should be in the form of <application_name>:<service_name>")
    private String schemaServiceName;

    public NormalizerConfig(String inputField) {
      this.inputField = inputField;
    }

    public String getInputField() {
      if (inputField == null || inputField.trim().length() == 0) {
        return "message";
      }
      return inputField;
    }
  }
}
