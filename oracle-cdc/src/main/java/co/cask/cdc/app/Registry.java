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
package co.cask.cdc.app;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * App managing the Registry
 */
public class Registry extends AbstractApplication {
  @Override
  public void configure() {
    addService("Schema", new SchemaRegistryHandler());
    createDataset("SchemaRegistry", KeyValueTable.class, DatasetProperties.EMPTY);
  }

  public static class SchemaRegistryHandler extends AbstractHttpServiceHandler {

    @UseDataSet("SchemaRegistry")
    private KeyValueTable table;

    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryHandler.class);

    @Path("tables/{table}/schemas/{schema-id}")
    @GET
    public void getSchema(HttpServiceRequest request, HttpServiceResponder responder,
                          @PathParam("table") String tableId, @PathParam("schema-id") long schemaId) {
      String schema = Bytes.toString(table.read(getRowKey(tableId, schemaId)));
      LOG.info("Received schema for table {}, schema-id {} is {}", tableId, schemaId, schema);
      responder.sendString(schema);
    }

    @Path("tables/{table}/schemas/{schema-id}")
    @PUT
    public void putSchema(HttpServiceRequest request, HttpServiceResponder responder,
                          @PathParam("table") String tableId, @PathParam("schema-id") long schemaId) {
      String schema = Bytes.toString(request.getContent());
      LOG.info("Table {}, Schema id: {}, Schema: {}", table, schemaId, schema);
      table.write(getRowKey(tableId, schemaId), schema);
      responder.sendStatus(200);
    }

    private String getRowKey(String table, long schemaId) {
      return table + ":" + schemaId;
    }
  }
}
