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
        String str1 = "124.202.181.170\t\t[02/Jun/2020:18:19:44 +0800]\t\"POST /log HTTP/1.1\"\t200\t24\t\"\"\t\"okhttp/3.11.0\"\t\"\"\tenc=0&appkey=yingyin&ltype=watchvideo&log=%7B%22accu%22%3A%22275.0%22%2C%22area%22%3A%22%E5%8C%97%E4%BA%AC%E5%B8%82%E6%B5%B7%E6%B7%80%E5%8C%BA%E4%B8%8A%E5%9C%B0%E4%B8%89%E8%A1%97%E9%9D%A0%E8%BF%91%E9%87%91%E8%9E%8D%E7%A7%91%E8%B4%B8%E5%A4%A7%E5%8E%A6%22%2C%22brand%22%3A%22HONOR%22%2C%22chl%22%3A%22longbei4.5%22%2C%22country%22%3A%22%E4%B8%AD%E5%9B%BD%22%2C%22did%22%3A%22HONOR__ANDROID_ID%3A13e6d3d2b48b45ff%22%2C%22imei%22%3A%22ANDROID_ID%3A13e6d3d2b48b45ff%22%2C%22ip%22%3A%22192.168.0.194%22%2C%22lat%22%3A%2240.036244%22%2C%22lng%22%3A%22116.309609%22%2C%22macad%22%3A%2274%3AD2%3A1D%3A9A%3ABA%3A76%22%2C%22model%22%3A%22BKL-AL20%22%2C%22os%22%3A%2210%22%2C%22ts%22%3A%222020-06-02%2018%3A19%3A46%22%2C%22uid%22%3A%221255056694036258816%22%2C%22unet%22%3A%22WIFI%22%2C%22value%22%3A%7B%22cid%22%3A%221191968374247235584%22%2C%22cosid%22%3A%221611%22%2C%22cpid%22%3A%221191968411463294976%22%2C%22endpos%22%3A%2254251%22%2C%22optype%22%3A%22drag%22%2C%22stapos%22%3A%220%22%2C%22uid%22%3A%221255056694036258816%22%2C%22vdsize%22%3A%22133295%22%2C%22wid%22%3A%221591093186195%22%2C%22wsize%22%3A%221591093186195%22%7D%2C%22ver%22%3A%225.7.0%22%7D";
        System.out.println(str1.split("\t")[9]);
//        System.out.println(str.split(" ").length);
//        System.out.println(str1.split("&")[0]);
        try {
            String decode = URLDecoder.decode(str1.split("\t")[9], "utf-8");
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
