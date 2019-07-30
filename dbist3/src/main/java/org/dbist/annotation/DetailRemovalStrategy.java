package org.dbist.annotation;

/**
 * Master Data가 삭제되는 경우 Detail Data 처리 전략
 *
 * @author shortstop
 */
public class DetailRemovalStrategy {

    /**
     * 디테일 데이터 삭제 후 디테일 콜백 없음(Hook 실행하지 않음)
     */
    public static final String DELETE = "delete";

    /**
     * 디테일 데이터 삭제 후 디테일 콜백(Hook 실행)
     */
    public static final String DESTROY = "destroy";

    /**
     * 디테일 데이터의 부모 참조 ID를 Null로 업데이트
     */
    public static final String NULLIFY = "nullify";

    /**
     * 디테일 데이터가 존재하는 경우 삭제 시도시 에러 발생
     */
    public static final String EXCEPTION = "exception";
}
