package za.co.absa.hermes.e2eRunner

import java.io.File

import org.clapper.classutil.ScalaCompat.LazyList
import org.clapper.classutil.{ClassFinder, ClassInfo}


/**
 * PluginManager class is here to hold and retrieve plugin instances.
 *
 * @param plugins A map of plugins. Key is the human readable name and value a class identifier.
 */
class PluginDefinitions(private val plugins: Map[String, String]) {
  /**
   * Returns an instance of plugin base on the user friendly plugin name
   * @param name User friendly plugin name
   * @return Instance of the plugin corresponding to the name
   */
  @throws(classOf[PluginNotFound])
  def getPlugin(name: String): Plugin = {
    val className = if (plugins.keySet.contains(name)) {
      plugins(name)
    } else throw PluginNotFound(name)

    Class.forName(className).newInstance().asInstanceOf[Plugin]
  }

  /**
   * @return Returns a set of plugin names loaded and able to use.
   */
  def getPluginNames: Set[String] = plugins.keySet


}

object PluginDefinitions {
  /**
   * Create plugins instance from a Map
   * @param plugins  A map of plugins. Key is the human readable name and value a class identifier.
   * @return New instance of PluginManager
   */
  def apply(plugins: Map[String, String]): PluginDefinitions = new PluginDefinitions(plugins)

  /**
   * Load plugins from a Classpath
   * @param classpaths Sequence of Files where Plugins will be searched for
   * @return New instance of PluginManager
   */
  def apply(classpaths: Seq[File] = Seq(new File("."))): PluginDefinitions = {
    val plugins = getPluginsIterator(classpaths)
    val pluginMap: Map[String, String] = getPluginsMap(plugins)
    PluginDefinitions(pluginMap)
  }

  private def getPluginsMap(plugins: Iterator[ClassInfo]): Map[String, String] = {
    plugins.foldLeft(Map.empty[String, String]) {
      (acc, value) =>
        val plugin: Plugin = Class.forName(value.name).newInstance().asInstanceOf[Plugin]
        if (acc.keySet.contains(plugin.name)) throw DuplicatePluginNames(plugin.name)
        acc + (plugin.name -> value.name)
    }
  }

  private def getPluginsIterator(classpath: Seq[File]): Iterator[ClassInfo] = {
    val finder: ClassFinder = ClassFinder(classpath)
    val classes: LazyList[ClassInfo] = finder.getClasses
    val classMap = ClassFinder.classInfoMap(classes)
    val plugins: Iterator[ClassInfo] = ClassFinder.concreteSubclasses("za.co.absa.hermes.e2eRunner.Plugin", classMap)
    plugins
  }
}
