/**
 *
 */
package org.dbist.util;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.dbist.annotation.PrimaryKey;

public class ReflectionUtil {
    /**
     * ID Field 가져오기 실행
     * @param clazz
     * @return
     */
    public static List<Field> getIdField(Class<?> clazz) {
        List<Field> list = new ArrayList<Field>();

        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            if (field.isAnnotationPresent(PrimaryKey.class) || field.getName().equals("id")) {
                list.add(field);
            }
        }

        return list.size() > 0 ? list : null;
    }
}
