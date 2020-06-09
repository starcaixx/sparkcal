import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.time.Instant;
import java.time.LocalDateTime;

public class Test {
    public static void main(String[] args) {
//        String str = "61.151.206.18 -  [28/Apr/2020:14:32:55 +0800] \"GET /log HTTP/1.0\" 200 14 \"\" \"Mozilla/5.0 (iPhone; CPU iPhone OS 8_3 like Mac OS X) AppleWebKit/600.1.4 (KHTML; like Gecko) Mobile/12F70 MicroMessenger/6.1.5 NetType/WIFI\" \"\"\n";
//        String str1 = "enc=0&appkey=yingyin&ltype=watchvideo&log={\"accu\":\"93.0\",\"area\":\"北京市海淀区上地三街靠近金融科贸大厦\",\"brand\":\"Xiaomi\",\"chl\":\"longbei4.5\",\"country\":\"中国\",\"did\":\"longbei70:3A:51:3F:99:BD\",\"imei\":\"longbei70:3A:51:3F:99:BD\",\"ip\":\"192.168.0.36\",\"lat\":\"40.036172\",\"lng\":\"116.309734\",\"macad\":\"70:3A:51:3F:99:BD\",\"model\":\"MI PAD 4\",\"os\":\"8.1.0\",\"ts\":\"2020-04-30 10:01:55\",\"uid\":\"25121\",\"unet\":\"WIFI\",\"value\":{\"cid\":\"1249971917226631168\",\"cosid\":\"30193\",\"cpid\":\"1249972163868389376\",\"optype\":\"start\",\"stapos\":\"0\",\"uid\":\"25121\",\"vdsize\":\"269560\",\"wid\":\"1588212115304\"},\"ver\":\"5.6.9\"}";
        String str1 = "enc=0&appkey=yingyin&ltype=watchvideo&log=%7B%22brand%22%3A%22OPPO%22%2C%22chl%22%3A%22longbei4.5%22%2C%22did%22%3A%22OPPO_5401ea0b%22%2C%22imei%22%3A%22A100004EABA501%22%2C%22ip%22%3A%22192.168.1.107%22%2C%22macad%22%3A%22C8%3AF2%3A30%3A53%3A68%3A39%22%2C%22model%22%3A%22OPPO%20R9%20Plustm%20A%22%2C%22os%22%3A%225.1.1%22%2C%22ts%22%3A%222020-06-08%2008%3A36%3A37%22%2C%22uid%22%3A%221213100716084695040%22%2C%22unet%22%3A%22WIFI%22%2C%22value%22%3A%7B%22cid%22%3A%221228363825556836352%22%2C%22cosid%22%3A%2220503%22%2C%22cpid%22%3A%221228364593714257920%22%2C%22endpos%22%3A%22948302%22%2C%22optype%22%3A%22drag%22%2C%22stapos%22%3A%221037404%22%2C%22uid%22%3A%221213100716084695040%22%2C%22vdsize%22%3A%222121481%22%2C%22wid%22%3A%221591575775939%22%2C%22wsize%22%3A%221427%22%7D%2C%22ver%22%3A%225.7.0%22%7D";
//        System.out.println(str1.split("\t").length);
//        System.out.println(str.split(" ").length);
//        System.out.println(str1.split("&")[0]);
        try {
            String decode = URLDecoder.decode(str1, "utf-8");
            System.out.println(decode);
            JSONObject jsonObject = JSON.parseObject(decode.substring(decode.indexOf("{")));
            System.out.println(jsonObject.getJSONObject("value").getString("optype"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        long ts = Instant.now().toEpochMilli();

        System.out.println(ts);
    }
}
