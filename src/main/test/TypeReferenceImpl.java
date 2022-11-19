import org.apache.ibatis.type.TypeReference;

/**
 * <p>
 * 文件描述（必填！！！）
 * </p>
 *
 * @author lvchao
 * @since 2022/10/23 0:21
 */
public class TypeReferenceImpl<T> extends TypeReference {
    public static void main(String[] args) {
        TypeReferenceImpl<String> typeReference = new TypeReferenceImpl<String>();

    }
}
