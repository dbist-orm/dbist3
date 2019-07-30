package org.dbist.util;

import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.collections.ComparatorUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.springframework.util.StringUtils;

import net.sf.common.util.ValueUtils;

public class QueryUtil {

    @SuppressWarnings("unchecked")
    private static final Comparator<String> COMPARATOR_REVERSED = ComparatorUtils.reversedComparator(ComparatorUtils.naturalComparator());

    public static String printQuery(String namedSql, Object params, boolean prettyPrint) {
        String sql = params == null ? namedSql : combine(namedSql, params);
        System.out.println(sql);
        return sql;
    }

    private static String combine(String sql, Object params) {
        if (sql == null || params == null)
            return sql;
        if (params instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, ?> map = (Map<String, ?>) params;
            Set<String> keySet = new TreeSet<String>(COMPARATOR_REVERSED);
            keySet.addAll(map.keySet());
            for (String key : keySet)
                sql = StringUtils.replace(sql, ":" + key, toParamValue(map.get(key)));
        } else if (params instanceof Object[]) {
            for (Object param : (Object[]) params)
                sql = sql.replaceFirst("?", toParamValue(param));
        }
        return sql;
    }

    private static String toParamValue(Object value) {
        if (value == null)
            return "null";
        if (value instanceof String)
            return "'" + StringEscapeUtils.escapeSql((String) value) + "'";
        if (value instanceof Date)
            return "'" + ValueUtils.toDateString((Date) value, ValueUtils.DATEPATTERN_DATETIME) + "'";
        if (value instanceof Collection) {
            StringBuffer buf = new StringBuffer();
            int i = 0;
            for (Object item : (Collection<?>) value)
                buf.append(i++ == 0 ? "" : ",").append(toParamValue(item));
            return buf.toString();
        }
        return ValueUtils.toString(value);
    }

    public static String toUnderScore(String camelCaseStr) {
        return camelCaseStr.replaceAll("(.)(\\p{Upper})", "$1_$2").toLowerCase();
    }
}
