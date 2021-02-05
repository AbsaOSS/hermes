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

All properties were scraped. Only properties that might come in hadny are the ones implemented atop scribe logging library. Check them out on their GitHub [page](https://github.com/outr/scribe)
#### Parameters

- `test-definition-path` - mandatory - path to json with test definitions. Format explanation bellow in [Test Definition JSON](#test-definition-json).
- `jar-path` - optional - path to additional jars to load plugins from. You can also use spark's `--jars` instead of this.
- `fail-fast` - defaults to `false` - switch in case you want to kill the run on first failure.

#### Test Definition JSON

Test definition JSON has a struct as base having two keys, `vars` and `runs`. 

`vars` holds a struct used for the representation of variables for multiple runs. This way, you do not have to repeat yourself with, for example, dataset names or other parameters that are valid for more than one run. Key is the variable you put in other places of the JSON surrounded by `#{` and `}#` and the value is the value you want to use in that place.

`runs` holds an array of test definitions. Test definition is an object, having its own `case class TestDefinition` holding the information about a test run, i.e. a plugin name, input arguments, order, etc. 

**JSON Structure**

```json
{
  // Struct holding the variables. Optional
  "vars": {
    // Here `prefix` is a key that will be somewhere in runs as `#{prefix}#` and the 
    // value will be inputed there instead of it
    "prefix": "Some Random PreFIX stuff"
  },
  // Struct holding array of test definitons. Mandatory
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
      // Input arguments for the plugin. First argument will be replaced by var `prefix`.
      // Second is a simple string
      "args" : ["#{prefix}#", "fileName"],
      // Arguments, but for execution of write of plugin result. Optional
      "writeArgs": [],
      // If the test depends on a different test. Will automatically fail if dependee failed. Optional
      "dependsOn": "Test1"
    }
  ]
}
```
