package za.co.absa.hermes.e2eRunner

import spray.json.{DefaultJsonProtocol, RootJsonFormat}

object TestDefinitionJsonProtocol extends DefaultJsonProtocol {
  implicit val pluginDefinitionFormat: RootJsonFormat[TestDefinition] = jsonFormat6(TestDefinition.apply)
}


/**
 * A representation of a test definition together with it's actual order
 * @param definition TestDefinition instance
 * @param actualOrder Actual order of the test for running.
 */
case class TestDefinitionWithOrder(definition: TestDefinition, actualOrder: Int)

/**
 * Test definition representation.
 *
 * @param name Name of the test. This is for user's recognition
 * @param order Order of the test to be executed in. This is not an actual order, more like weight of the test.
 *              So you can specify something that must run last as 9999999 and something as first as -1.
 * @param pluginName User-friendly name of the plugin to be executed.
 * @param args Arguments to be provided into the performAction method of classes that implement the Plugin.
 * @param writeArgs Optional arguments for the write method implementations of PluginResult.
 */
case class TestDefinition(name: String,
                          order: Int,
                          pluginName: String,
                          args: Array[String],
                          dependsOn: Option[String],
                          writeArgs: Option[Array[String]]) {
  override def equals(o: Any): Boolean = o match {
    case second: TestDefinition =>
      this.name == second.name &&
        this.order == second.order &&
        this.pluginName == second.pluginName &&
        this.args.sameElements(second.args) &&
        this.dependsOn == second.dependsOn &&
        ((this.writeArgs.isDefined && second.writeArgs.isDefined &&
        this.writeArgs.get.sameElements(second.writeArgs.get)) || this.writeArgs.isEmpty && second.writeArgs.isEmpty)
    case _ => false
  }
}

