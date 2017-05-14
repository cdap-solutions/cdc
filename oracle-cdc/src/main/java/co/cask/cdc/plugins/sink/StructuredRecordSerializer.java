package co.cask.cdc.plugins.sink;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

/**
 *
 */
public class StructuredRecordSerializer implements JsonSerializer<StructuredRecord> {

  @Override
  public JsonElement serialize(StructuredRecord src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject obj = new JsonObject();
    for (Schema.Field field : src.getSchema().getFields()) {
      obj.add(field.getName(), context.serialize(src.get(field.getName())));
    }
    return obj;
  }
}
