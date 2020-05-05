import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

public class Test {
    public static void main(String[] args) {
//        String str = "61.151.206.18 -  [28/Apr/2020:14:32:55 +0800] \"GET /log HTTP/1.0\" 200 14 \"\" \"Mozilla/5.0 (iPhone; CPU iPhone OS 8_3 like Mac OS X) AppleWebKit/600.1.4 (KHTML; like Gecko) Mobile/12F70 MicroMessenger/6.1.5 NetType/WIFI\" \"\"\n";
        String str1 = "enc=0&appkey=yingyin&ltype=watchvideo&log={\"accu\":\"93.0\",\"area\":\"北京市海淀区上地三街靠近金融科贸大厦\",\"brand\":\"Xiaomi\",\"chl\":\"longbei4.5\",\"country\":\"中国\",\"did\":\"longbei70:3A:51:3F:99:BD\",\"imei\":\"longbei70:3A:51:3F:99:BD\",\"ip\":\"192.168.0.36\",\"lat\":\"40.036172\",\"lng\":\"116.309734\",\"macad\":\"70:3A:51:3F:99:BD\",\"model\":\"MI PAD 4\",\"os\":\"8.1.0\",\"ts\":\"2020-04-30 10:01:55\",\"uid\":\"25121\",\"unet\":\"WIFI\",\"value\":{\"cid\":\"1249971917226631168\",\"cosid\":\"30193\",\"cpid\":\"1249972163868389376\",\"optype\":\"start\",\"stapos\":\"0\",\"uid\":\"25121\",\"vdsize\":\"269560\",\"wid\":\"1588212115304\"},\"ver\":\"5.6.9\"}";
//        System.out.println(str.split(" ").length);
        System.out.println(str1.split("&")[0]);
        /*try {
            String decode = URLDecoder.decode(str1.split("\t")[9], "utf-8");
            System.out.println(decode);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }*/
    }
}
