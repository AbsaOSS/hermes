---
layout: docs
title: Usage - E2E Runner
version: '0.3'
categories:
    - '0.3'
    - usage
redirect_from: /docs/usage/dataset-comparison
---

#### Table Of Contents
- [Table Of Contents](#table-of-contents)
- [Properties](#properties)
- [Parameters](#parameters)
- [Test Definition JSON](#test-definition-json)

#### Properties

The only properties that might come in handy are the ones implemented atop `scribe` logging library. Check them out on their GitHub [page](https://github.com/outr/scribe)

#### Parameters

- `test-definition-path` - mandatory - path to JSON with test definitions. Format explanation bellow in [Test Definition JSON](#test-definition-json)
- `jar-path` - optional - path to additional jars to load plugins from. You can also use Spark's `--jars` instead of this.
- `fail-fast` - defaults to `false` - switch in case you want to kill the run on the first failure.

#### Test Definition JSON

Test definition JSON has a struct as a base having two keys, `vars` and `runs`. 

`vars` holds a struct used for the representation of variables inside of runs. This is so you do not have to repeat yourself with, for example, dataset names or other parameters that are valid for more than one run. Key is the variable you put in other places of the JSON surrounded by `#{` and `}#`, and the value is the value you want to use in that place.

`runs` holds an array of test definitions. Test definition is an object, having its own case class TestDefinition holding the information about the test run. That is a plugin name, input arguments, order, etc. 

**JSON Structure**

```json
{
  // Struct holding the variables. Optional
  "vars": {
    // Here `someKey` is a key that will be somewhere in runs as `#{someKey}#` and the 
    // value will be inputted there instead of it
    "someKey": "Some Random someKey stuff"
  },
  // Struct holding array of test definitions. Mandatory
  "runs" : [
    // Representation of one TestDefinition
    {
      // PluginName is a unique name of plugin loaded on start. Each plugin has its own specific name
      "pluginName" : "BashPlugin",
      // Name of the test. Also should be unique. Used by testers to differentiate runs
      "name": "Test2",
      // Order of the run. Order is used to sort runs by. If there are multiple same order elements,
      // name is used to sort them further in alphabetical order.
      "order" : 1,
      // Input arguments for the plugin. The first argument will be replaced by var `someKey`.
      // Second is a simple string
      "args" : ["#{someKey}#", "fileName"],
      // Arguments, but for the execution of write of plugin result. Optional
      "writeArgs": [],
      // If the test depends on a different test. Will automatically fail if dependee failed. Optional
      "dependsOn": "Test1"
    }
  ]
}
```