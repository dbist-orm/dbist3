package org.dbist.ddl;

import org.springframework.core.env.Environment;

/**
 * DDL Startup Service가 완료된 후 Initial Setup을 실행하는 인터페이스
 *
 * @author shortstop
 */
public interface InitialSetup {

    /**
     * initial setup
     *
     * @param env
     * @return
     */
    public boolean initialSetup(Environment env);
}
