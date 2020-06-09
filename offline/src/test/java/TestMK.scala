import java.net.URLDecoder
import java.util.ResourceBundle

import com.alibaba.fastjson.{JSON, JSONObject}
import com.longbei.bigdata.offline.UserBehavior.bundle

object TestMK {
  def main(args: Array[String]): Unit = {
	/*val bundle: ResourceBundle = ResourceBundle.getBundle("config")
	val str = "a b c d ef a 1 2 3 4 5 6 a aefa ea l ijl ae afe aef a fea afe"
	println(str.split(" ").mkString("\t"))
	println(str)
	val fieldsParams: String = bundle.getString("fields.params")
	val fields: Array[String] = fieldsParams.split("-")
	val commFields: Array[String] = fields(0).split(",")
	val eventFields: Array[String] = fields(1).split(",")

	for (elem <- commFields) {
	  println(elem)
	}*/

	val json: JSONObject = JSON.parseObject("{\"accu\":\"50.0\",\"area\":\"河北省邯郸市魏县礼贤南街靠近魏县图书馆\",\"brand\":\"EEBBK\",\"chl\":\"longbei4.5\",\"country\":\"中国\",\"did\":\"EEBBK_70S5C0306AB9J\",\"imei\":\"00070S5C0306AB9J\",\"ip\":\"192.168.0.102\",\"lat\":\"36.353476\",\"lng\":\"114.943343\",\"macad\":\"7C:FD:82:FB:94:1A\",\"model\":\"S5\",\"os\":\"9.0\",\"ts\":\"2020-06-08 08:36:43\",\"uid\":\"1227164771678203904\",\"unet\":\"WIFI\",\"value\":{\"cid\":\"1228363825556836352\",\"cosid\":\"20503\",\"cpid\":\"1228364593714257920\",\"optype\":\"timing\",\"stapos\":\"1341798\",\"tms\":\"136\",\"uid\":\"1227164771678203904\",\"vdsize\":\"2121481\",\"wid\":\"1591575212883\",\"wsize\":\"10000\"},\"ver\":\"5.7.0\"}")
	println(json.getJSONObject("value"))
  }

}
