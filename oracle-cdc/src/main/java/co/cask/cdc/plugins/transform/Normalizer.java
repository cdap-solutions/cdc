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
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
@Plugin(type = "transform")
@Name("CDCNormalizer")
public class Normalizer extends Transform<StructuredRecord, StructuredRecord> {
  private static Logger LOG = LoggerFactory.getLogger(Normalizer.class);
  private static Gson GSON = new Gson();

  private static final Schema COLUMN_SCHEMA = Schema.recordOf("Column",
                                                              Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                                                              Schema.Field.of("type", Schema.of(Schema.Type.STRING)),
                                                              Schema.Field.of("value", Schema.of(Schema.Type.STRING)));


  private static final Schema CDC_SCHEMA = Schema.recordOf("CDC",
                                                           Schema.Field.of("table", Schema.of(Schema.Type.STRING)),
                                                           Schema.Field.of("op_type", Schema.of(Schema.Type.STRING)),
                                                           Schema.Field.of("op_ts", Schema.of(Schema.Type.STRING)),
                                                           Schema.Field.of("current_ts", Schema.of(Schema.Type.STRING)),
                                                           Schema.Field.of("before", Schema.unionOf(Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING)), Schema.of(Schema.Type.NULL))),
                                                           Schema.Field.of("after", Schema.unionOf(Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING)), Schema.of(Schema.Type.NULL))));

  // Schema.Field.of("before", Schema.unionOf(Schema.arrayOf(COLUMN_SCHEMA),
  //                                         Schema.of(Schema.Type.NULL))),
  // Schema.Field.of("after", Schema.unionOf(Schema.arrayOf(COLUMN_SCHEMA),
  //                                        Schema.of(Schema.Type.NULL))));

  private final NormalizerConfig config;

  public Normalizer(NormalizerConfig config) {
    this.config = config;
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    LOG.info("Input StructuredRecord is {}", GSON.toJson(input));
    byte[] message = input.get("message");
    if (message == null) {
      LOG.info("Message field does not exists in the input StructuredRecord.");
    } else {
      LOG.info("Message is {}", Bytes.toString(message));
      String multiMessageBody = new String(message, StandardCharsets.UTF_8);

      int startIndex = 0;
      int endIndex;
      while (true) {
        endIndex = multiMessageBody.indexOf("{\"table\":", startIndex + 5);
        String jsonMessage = multiMessageBody.substring(startIndex);
        if (endIndex != -1) {
          jsonMessage = multiMessageBody.substring(startIndex, endIndex);
        }
        // This is the message you need process.
        emitRecord(jsonMessage, emitter);
        if (endIndex == -1) {
          break;
        }
        multiMessageBody = multiMessageBody.substring(endIndex);
        startIndex = 0;
      }
    }
  }

  private void emitRecord(String message, Emitter<StructuredRecord> emitter) {
    JsonObject object = GSON.fromJson(message, JsonObject.class);
    StructuredRecord.Builder builder = StructuredRecord.builder(CDC_SCHEMA);
    builder.set("table", object.get("table").getAsString());
    builder.set("op_type", object.get("op_type").getAsString());
    builder.set("op_ts", object.get("op_ts").getAsString());
    builder.set("current_ts", object.get("current_ts").getAsString());
    LOG.info("Operation Type {}", object.get("op_type").getAsString().charAt(0));
    switch (object.get("op_type").getAsString().charAt(0)) {
      case 'I':
        builder.set("after", getTableData("after", object));
        builder.set("before", null);
        break;
      case 'U':
        builder.set("before", getTableData("before", object));
        builder.set("after", getTableData("after", object));
        break;
      case 'D':
        builder.set("before", getTableData("before", object));
        builder.set("after", null);
        break;
      default:
        throw new IllegalArgumentException("invalid operation type encountered");
    }
    emitter.emit(builder.build());
  }

  private Map<String, String> getTableData(String name, JsonObject object) {
    JsonObject after = object.getAsJsonObject(name);
    Map<String, String> values = new HashMap<>();
    for (Map.Entry<String, JsonElement> entry : after.entrySet()) {
      values.put(entry.getKey(), entry.getValue().getAsString());
    }
    return values;
  }

  public static class NormalizerConfig extends PluginConfig {
    @Name("inputField")
    @Description("Input field containing the payload of the message. Defaults to 'message'.")
    private String inputField;

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
