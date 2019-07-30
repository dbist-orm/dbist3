package org.dbist.annotation;

/**
 * Master-Detail 유형 정의
 *
 * @author shortstop
 */
public class MasterDetailType {

    /**
     * 부모 - 자식 관계 - 1 : N
     */
    public static final String ONE_TO_MANY = "one-to-many";

    /**
     * 부모 - 자식 관계 - 1 : 1
     */
    public static final String ONE_TO_ONE = "one-to-one";
}
