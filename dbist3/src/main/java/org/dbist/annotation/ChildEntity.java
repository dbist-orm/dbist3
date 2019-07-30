package org.dbist.annotation;

/**
 * Master의 자식 엔티티 정의
 *
 * @author shortstop
 */
public @interface ChildEntity {
    /**
     * (Required) The class name of the child entity.
     *
     * @return The child entity class name
     */
    Class<?> entityClass();

    /**
     * (Required) The names of the columns to be included in the index.
     *
     * @return The names of the columns making up the index
     */
    String type() default MasterDetailType.ONE_TO_MANY;

    /**
     * (Required) Reference Field Name of Chile Entity Class.
     *
     * @return The reference field name
     */
    String refFields();

    /**
     * (Required) JSON으로 Export시 detail에 대한 data property 명
     *
     * @return detail data property 명
     */
    String dataProperty() default "";

    /**
     * (Required) Detail 삭제시 전략
     *
     * @return detail 삭제시 전략
     */
    String deleteStrategy() default DetailRemovalStrategy.EXCEPTION;
}
