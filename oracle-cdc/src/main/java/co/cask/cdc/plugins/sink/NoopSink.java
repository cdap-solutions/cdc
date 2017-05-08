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

package co.cask.cdc.plugins.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.hydrator.common.ReferenceBatchSink;
import co.cask.hydrator.common.ReferencePluginConfig;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.kudu.client.Operation;

import java.util.Map;

/**
 *
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("NoopSink")
@Description("No-op sink.")
public class NoopSink extends ReferenceBatchSink<StructuredRecord, NullWritable, Operation> {
  public NoopSink(ReferencePluginConfig config) {
    super(config);
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, Operation>> emitter) throws Exception {
    super.transform(input, emitter);
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    // no-op
    context.addOutput(Output.of("testoutput", new NullOutputFormatProvider()));
  }

  /**
   * An {@link OutputFormatProvider} which provides a {@link NullOutputFormat} which writes nothing.
   */
  public class NullOutputFormatProvider implements OutputFormatProvider {

    @Override
    public String getOutputFormatClassName() {
      return NullOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return ImmutableMap.of();
    }
  }
}
