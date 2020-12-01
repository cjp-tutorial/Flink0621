import java.util.HashMap;
import java.util.Map;

/**
 * TODO
 *
 * @author cjp
 * @version 1.0
 * @date 2020/11/30 16:18
 */
public class test {
    public static void main(String[] args) {
        Map<String, String> map = new HashMap<>();
        map.put(null, "a");
        String s = map.get(null);
        System.out.println(s+"---"+s.getClass());
//        System.out.println(map.containsKey(null));
    }
}
