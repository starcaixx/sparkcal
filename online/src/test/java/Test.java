import java.time.LocalDate;
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
        LocalDate localDate = LocalDate.now();
        System.out.println(localDate.toString().replace("-",""));
    }
}
