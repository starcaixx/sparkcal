import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.Enumeration;
import java.util.ResourceBundle;

public class Test {
    public static void main(String[] args) {
        /*ResourceBundle jdbc = ResourceBundle.getBundle("jdbc");
        Enumeration<String> keys = jdbc.getKeys();
        while (keys.hasMoreElements()) {
            String s = keys.nextElement();
            System.out.println(s);
        }*/
//        LocalDate localDate = LocalDate.now();
//        System.out.println(localDate);
//        System.out.println(localDate.toString().replace("-",""));
        String format = new SimpleDateFormat("yyyy-MM-dd").format(new Date(1554198156848l));
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(1554198156848l), ZoneId.systemDefault());
        System.out.println(localDateTime.toLocalDate());
        System.out.println(format);


        LocalDate now = LocalDate.now();
        System.out.println(now);
    }
}
