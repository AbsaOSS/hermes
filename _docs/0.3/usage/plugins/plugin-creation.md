---
layout: docs
title: Usage - Plugin Creation
version: '0.3'
categories:
    - '0.3'
    - 'usage'
    - 'plugins'
redirect_from: /docs/usage/plugins/plugin-creation
---

E2E Runner is also able to run any plugin on the classpath. The plugin needs to extend `za.co.absa.hermes.e2eRunner.Plugin` trait and the `performAction` method must to return an implementation of `za.co.absa.hermes.e2eRunner.PluginResult`.

#### Plugin implementation

```scala
import za.co.absa.hermes.e2eRunner.Plugin

class MyPlugin extends Plugin {

  /**
   * Plugin names is here to provide user-friendly name for each plugin. Used in TestDefinitions
   *
   * @return User-friendly name of the plugin
   */
  override def name: String = "MyPlugin"

  /**
   * Perform action is the core method. Executes the plugin and returns a result as a subclass of PluginResult
   *
   * @param testDefinition TestDefinition instance
   * @param actualOrder When specifying the order in the test definition json, the number might not be the
   *                    same as the execution number. This number is automatically provided by the PluginManager.
   *                    PluginResult expects this number.
   * @return Returns an implementation of PluginResult.
   */
  override def performAction(testDefinition: TestDefinition, actualOrder: Int): MyPluginResult = {
    val result = 2 + 2
    
    MyPluginResult(
      arguments = testDefinition.args,
      returnedValue = result, 
      order = actualOrder, 
      testName = testDefinition.testName, 
      passed = true, 
      additionalInfo = Map("extra" -> "info")
    )
  }
}
```

#### PluginResult implementation

```scala
import za.co.absa.hermes.e2eRunner.PluginResult

case class MyPluginResult(arguments: Array[String],
                          returnedValue: Any,
                          order: Int,
                          testName: String,
                          passed: Boolean,
                          additionalInfo: Map[String, String]) extends PluginResult {

  /**
   * This method should be used to write the plugin result in a form required.
   *
   * @param writeArgs Arguments provided from the "writeArgs" key from the test definition json
   */
  override def write(writeArgs: Array[String]): Unit = {
    import java.io._
    val pw = new PrintWriter(new File(writeArgs.head))
    pw.write(returnedValue)
    pw.close
  }

  /**
   * Logs the result of the plugin execution at the end.
   */
  override def resultLog: ResultLog = {
    if (passed) {
      InfoResultLog("We passed")
    } else {
      ErrorResultLog("We failed")
    }
  }
}
```
