import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

public class Test {
    public static void main(String[] args) {
        String str = "61.151.206.18 -  [28/Apr/2020:14:32:55 +0800] \"GET /log HTTP/1.0\" 200 14 \"\" \"Mozilla/5.0 (iPhone; CPU iPhone OS 8_3 like Mac OS X) AppleWebKit/600.1.4 (KHTML; like Gecko) Mobile/12F70 MicroMessenger/6.1.5 NetType/WIFI\" \"\"\n";
        String str1 = "124.202.181.170 -  [28/Apr/2020:14:32:04 +0800] \"POST /log HTTP/1.1\" 200 24 \"\" \"DragonCup/5.6.9 (iPhone; iOS 12.2; Scale/3.00)\" \"\" appkey=yingyin&enc=0&log=%7B%22tms%22%3A1%2C%22ip%22%3A%22192.168.0.104%22%2C%22province%22%3A%22%22%2C%22did%22%3A%2228371882e23f4e2a94d89d821473fb68%22%2C%22ts%22%3A1588055524.2391319%2C%22brand%22%3A%22Apple%22%2C%22imei%22%3A%2228371882e23f4e2a94d89d821473fb68%22%2C%22lng%22%3A0%2C%22wsize%22%3A0%2C%22cosid%22%3A%22%22%2C%22macad%22%3A%22%22%2C%22accu%22%3A0%2C%22os%22%3A%2212.2%22%2C%22unet%22%3A%22Wi-Fi%22%2C%22cid%22%3A%22%22%2C%22ver%22%3A%225.6.9%22%2C%22uid%22%3A%22941117597104029696%22%2C%22cpid%22%3A%22%22%2C%22model%22%3A%22iPhone%207%20Plus%22%2C%22wid%22%3A2594318685%2C%22lat%22%3A0%2C%22optype%22%3A%22pause%22%2C%22endpos%22%3A0%2C%22chl%22%3A%22AppStore%22%2C%22vdsize%22%3A0%7D&ltype=watchvideo";
        System.out.println(str.split(" ").length);
        System.out.println(str1.split(" ").length);
        try {
            String decode = URLDecoder.decode(str.split(" ")[17], "utf-8");
            System.out.println(decode);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }
}
