package za.co.absa.hermes.utils

trait CaseClassUtils {
  def toMap: Map[String, Any] = this.getClass.getDeclaredFields.foldLeft(Map.empty[String, Any]) { (m, f) =>
    f.setAccessible(true)
    val ret = m + (f.getName -> f.get(this))
    f.setAccessible(false)
    ret
  }

  def toStringMap: Map[String, String] = this.getClass.getDeclaredFields.foldLeft(Map.empty[String, String]) { (m, f) =>
    f.setAccessible(true)
    val ret = m + (f.getName -> f.get(this).toString)
    f.setAccessible(false)
    ret
  }
}
