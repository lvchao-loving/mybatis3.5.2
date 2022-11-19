import entity.User;
import org.apache.ibatis.reflection.Reflector;

import java.lang.reflect.Method;
import java.sql.Array;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 * 文件描述（必填！！！）
 * </p>
 *
 * @author lvchao
 * @since 2022/10/18 11:50
 */
public class Test {
    public static void main(String[] args) {
        Reflector reflector = new Reflector(User.class);
    }
}
