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

package co.cask.cdap.cdc.plugins.sink;

import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.datapipeline.DataPipelineApp;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
//import co.cask.cdap.test.IntegrationTestBase;
import co.cask.cdap.test.WorkflowManager;
import co.cask.cdc.plugins.sink.CDCBigTable;
import co.cask.hydrator.common.Constants;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.hbase1_x.BigtableConnection;
import com.google.common.collect.ImmutableList;
import org.apache.kudu.client.Operation;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
//public class CDCBigTableIntegrationTest extends IntegrationTestBase {
public class CDCBigTableIntegrationTest extends HydratorTestBase {

  private static final ArtifactVersion CURRENT_VERSION = new ArtifactVersion("3.2.0");

  private static final ArtifactId BATCH_APP_ARTIFACT_ID =
    NamespaceId.DEFAULT.artifact("data-pipeline", CURRENT_VERSION.getVersion());
  private static final ArtifactSummary BATCH_ARTIFACT =
    new ArtifactSummary(BATCH_APP_ARTIFACT_ID.getArtifact(), BATCH_APP_ARTIFACT_ID.getVersion());

  @BeforeClass
  public static void setupTest() throws Exception {
    // add the artifact for etl batch app
    // its a bummer we had to have a separate dependency for this
    setupBatchArtifacts(BATCH_APP_ARTIFACT_ID, DataPipelineApp.class);

    // add artifact for batch sources and sinks
    addPluginArtifact(NamespaceId.DEFAULT.artifact("batch-plugins", "1.0.0"), BATCH_APP_ARTIFACT_ID,
                      CDCBigTable.class, BigtableSession.class, BigtableConnection.class,
                      BigtableOptions.class,
                      Operation.class);
//                      HBaseSource.class, HBaseSink.class,
//                      HBaseTableInputFormat.class, TableInputFormat.class, HBaseTableOutputFormat.class,
//                      Result.class, ImmutableBytesWritable.class,
//                      Put.class, Mutation.class);
  }

  private static final Schema DDL_RECORD_SCHEMA = Schema.recordOf("DDLRecord",
    Schema.Field.of("table", Schema.of(Schema.Type.STRING))); // tableName

  private static final Schema CHANGE_EVENT_SCHEMA = Schema.recordOf(
    "changeEvent",
    Schema.Field.of("table", Schema.of(Schema.Type.STRING)), // tableName
    Schema.Field.of("op_type", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("change", Schema.recordOf("change", Schema.Field.of("name", Schema.of(Schema.Type.STRING)))),
    Schema.Field.of("primary_keys", Schema.arrayOf(Schema.of(Schema.Type.STRING))));

  @Test
  public void test() throws Exception {
    String inputDatasetName = "input-table";
    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputDatasetName));

    Map<String, String> bigTableProps = new HashMap<>();
    // CDCBigTableConfig
//    bigTableProps.put("instance", CDCBigTable.INSTANCE_ID);
//    bigTableProps.put("project", CDCBigTable.PROJECT_ID);
//    bigTableProps.put("serviceFilePath", CDCBigTable.CREDENTIALS);
    bigTableProps.put(Constants.Reference.REFERENCE_NAME, "bigtablesink");
    ETLStage sink = new ETLStage("CDCBigTable", new ETLPlugin("CDCBigTable", SparkSink.PLUGIN_TYPE, bigTableProps, null));
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(BATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("CDCBigTableSinkTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    List<StructuredRecord> input = ImmutableList.of(
//      StructuredRecord.builder(DDL_RECORD_SCHEMA).set("table", "alitable1").build(),
      StructuredRecord.builder(DDL_RECORD_SCHEMA).set("table", "alitable2").build()
    );
    MockSource.writeInput(inputManager, input);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRuns(ProgramRunStatus.COMPLETED, 1, 5, TimeUnit.MINUTES);

//    ResultScanner resultScanner = htable.getScanner(HBASE_FAMILY_COLUMN.getBytes());
//    Result result;
//    int rowCount = 0;
//    while (resultScanner.next() != null) {
//      rowCount++;
//    }
//    resultScanner.close();
//    Assert.assertEquals(4, rowCount);
//    result = htable.get(new Get("ORCL".getBytes()));
//    Assert.assertNotNull(result);
//    Map<byte[], byte[]> orclData = result.getFamilyMap(HBASE_FAMILY_COLUMN.getBytes());
//    Assert.assertEquals(2, orclData.size());
//    Assert.assertEquals("13", Bytes.toString(orclData.get("col1".getBytes())));
//    Assert.assertEquals("212.36", Bytes.toString(orclData.get("col2".getBytes())));
  }

  /**
   com.google.common.util.concurrent.Service$Listener
   org.apache.twill.internal.ServiceListenerAdapter


   java.lang.IncompatibleClassChangeError: Implementing class
   at java.lang.ClassLoader.defineClass1(Native Method)
   at java.lang.ClassLoader.defineClass(ClassLoader.java:763)
   at java.security.SecureClassLoader.defineClass(SecureClassLoader.java:142)
   at java.net.URLClassLoader.defineClass(URLClassLoader.java:467)
   at java.net.URLClassLoader.access$100(URLClassLoader.java:73)
   at java.net.URLClassLoader$1.run(URLClassLoader.java:368)
   at java.net.URLClassLoader$1.run(URLClassLoader.java:362)
   at java.security.AccessController.doPrivileged(Native Method)
   at java.net.URLClassLoader.findClass(URLClassLoader.java:361)
   at co.cask.cdap.common.lang.InterceptableClassLoader.findClass(InterceptableClassLoader.java:46)
   at java.lang.ClassLoader.loadClass(ClassLoader.java:424)
   at java.lang.ClassLoader.loadClass(ClassLoader.java:357)
   at java.lang.ClassLoader.defineClass1(Native Method)
   at java.lang.ClassLoader.defineClass(ClassLoader.java:763)
   at java.lang.ClassLoader.defineClass(ClassLoader.java:642)
   at co.cask.cdap.common.lang.InterceptableClassLoader.findClass(InterceptableClassLoader.java:73)
   at java.lang.ClassLoader.loadClass(ClassLoader.java:424)
   at java.lang.ClassLoader.loadClass(ClassLoader.java:357)
   at java.lang.Class.getDeclaredConstructors0(Native Method)
   at java.lang.Class.privateGetDeclaredConstructors(Class.java:2671)
   at java.lang.Class.getDeclaredConstructors(Class.java:2020)
   at com.google.inject.spi.InjectionPoint.forConstructorOf(InjectionPoint.java:243)
   at com.google.inject.internal.ConstructorBindingImpl.create(ConstructorBindingImpl.java:96)
   at com.google.inject.internal.InjectorImpl.createUninitializedBinding(InjectorImpl.java:629)
   at com.google.inject.internal.UntargettedBindingProcessor$1.visit(UntargettedBindingProcessor.java:51)
   at com.google.inject.internal.UntargettedBindingProcessor$1.visit(UntargettedBindingProcessor.java:35)
   at com.google.inject.internal.UntargettedBindingImpl.acceptTargetVisitor(UntargettedBindingImpl.java:42)
   at com.google.inject.internal.UntargettedBindingProcessor.visit(UntargettedBindingProcessor.java:35)
   at com.google.inject.internal.UntargettedBindingProcessor.visit(UntargettedBindingProcessor.java:27)
   at com.google.inject.internal.BindingImpl.acceptVisitor(BindingImpl.java:93)
   at com.google.inject.internal.AbstractProcessor.process(AbstractProcessor.java:55)
   at com.google.inject.internal.InjectorShell$Builder.build(InjectorShell.java:178)
   at com.google.inject.internal.InjectorShell$Builder.build(InjectorShell.java:188)
   at com.google.inject.internal.InternalInjectorCreator.build(InternalInjectorCreator.java:103)
   at com.google.inject.Guice.createInjector(Guice.java:95)
   at com.google.inject.Guice.createInjector(Guice.java:72)
   at com.google.inject.Guice.createInjector(Guice.java:62)
   at co.cask.cdap.test.TestBase.initialize(TestBase.java:243)
   at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
   at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
   at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
   at java.lang.reflect.Method.invoke(Method.java:498)
   at org.junit.runners.model.FrameworkMethod$1.runReflectiveCall(FrameworkMethod.java:50)
   at org.junit.internal.runners.model.ReflectiveCallable.run(ReflectiveCallable.java:12)
   at org.junit.runners.model.FrameworkMethod.invokeExplosively(FrameworkMethod.java:47)
   at org.junit.internal.runners.statements.RunBefores.evaluate(RunBefores.java:24)
   at org.junit.internal.runners.statements.RunAfters.evaluate(RunAfters.java:27)
   at org.junit.rules.ExternalResource$1.evaluate(ExternalResource.java:48)
   at org.junit.rules.RunRules.evaluate(RunRules.java:20)
   at org.junit.runners.ParentRunner.run(ParentRunner.java:363)
   at co.cask.cdap.common.test.TestRunner.run(TestRunner.java:73)
   at org.junit.runner.JUnitCore.run(JUnitCore.java:137)
   at com.intellij.junit4.JUnit4IdeaTestRunner.startRunnerWithArgs(JUnit4IdeaTestRunner.java:117)
   at com.intellij.junit4.JUnit4IdeaTestRunner.startRunnerWithArgs(JUnit4IdeaTestRunner.java:42)
   at com.intellij.rt.execution.junit.JUnitStarter.prepareStreamsAndStart(JUnitStarter.java:253)
   at com.intellij.rt.execution.junit.JUnitStarter.main(JUnitStarter.java:84)



   java.lang.NullPointerException
   at co.cask.cdap.test.TestBase.finish(TestBase.java:487)
   at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
   at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
   at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
   at java.lang.reflect.Method.invoke(Method.java:498)
   at org.junit.runners.model.FrameworkMethod$1.runReflectiveCall(FrameworkMethod.java:50)
   at org.junit.internal.runners.model.ReflectiveCallable.run(ReflectiveCallable.java:12)
   at org.junit.runners.model.FrameworkMethod.invokeExplosively(FrameworkMethod.java:47)
   at org.junit.internal.runners.statements.RunAfters.evaluate(RunAfters.java:33)
   at org.junit.rules.ExternalResource$1.evaluate(ExternalResource.java:48)
   at org.junit.rules.RunRules.evaluate(RunRules.java:20)
   at org.junit.runners.ParentRunner.run(ParentRunner.java:363)
   at co.cask.cdap.common.test.TestRunner.run(TestRunner.java:73)
   at org.junit.runner.JUnitCore.run(JUnitCore.java:137)
   at com.intellij.junit4.JUnit4IdeaTestRunner.startRunnerWithArgs(JUnit4IdeaTestRunner.java:117)
   at com.intellij.junit4.JUnit4IdeaTestRunner.startRunnerWithArgs(JUnit4IdeaTestRunner.java:42)
   at com.intellij.rt.execution.junit.JUnitStarter.prepareStreamsAndStart(JUnitStarter.java:253)
   at com.intellij.rt.execution.junit.JUnitStarter.main(JUnitStarter.java:84)


   */
}
