package org.dbist.ddl;

import java.util.List;
import java.util.Map;

import org.dbist.ddl.mapper.DdlMapper;
import org.dbist.metadata.Table;
import org.dbist.metadata.TableCol;
import org.dbist.metadata.TableIdx;

public interface Ddl {

    /**
     * Tablespace 지정 ddl
     */
    void setTableSpace(String dataTBSpace, String idxTBSpace);

    /**
     * Data Tablespace를 리턴
     */
    String getDataTableSpace();

    /**
     * Index Tablespace를 리턴
     */
    String getIndexTableSpace();

    /**
     * 도메인을 리턴한다.
     */
    String getDomain();

    /**
     * table명으로 부터 Table 메타데이터를 찾아 리턴
     */
    Table getTable(String tableName);

    /**
     * Entity Class로 부터 Table 메타데이터를 찾아 리턴
     */
    Table getTable(Class<?> entityClass);

    /**
     * 로그인 한 데이터베이스 사용자가 관리하는 모든 테이블 리스트를 조회한다.
     */
    List<String> getAllTables();

    /**
     * Table의 Column 메타 정보를 찾아 리턴
     */
    List<TableCol> getTableCols(Table table);

    /**
     * 엔티티의 인덱스를 조회
     */
    List<TableIdx> getTableIndexes(Class<?> entityClass);

    /**
     * 테이블의 인덱스를 조회
     */
    List<TableIdx> getTableIndexes(String tableName);

    /**
     * 사용자 계정명을 리턴한다.
     */
    String getAccount();

    /**
     * DDL Mapper (각 데이터베이스 타입 마다 특별한 DDL 구현을 한 매퍼)를 리턴
     */
    DdlMapper getDdlMapper();

    /**
     * 테이블이 이미 존재하는지 체크
     */
    boolean isTableExist(String tableName);

    /**
     * 환경 설정에 지정되어 있는 Package 내의 Entity 목록을 추출하여, Table 생성 실행.
     */
    void createTable();

    /**
     * BasePackage 내의 Entity를 추출하여 Table 생성.
     */
    void createTable(String basePackagePath);

    /**
     * Entity 정보를 통해 Table 생성 실행.
     */
    String createTable(Class<?> entity);

    /**
     * entity class를 통해 테이블 삭제
     */
    String dropTable(Class<?> entity);

    /**
     * tableName, sequenceName, indexNames를 통해 테이블 삭제
     */
    String dropTable(String tableName, String sequenceName, List<String> indexNames);

    /**
     * 테이블을 수정
     */
    String alterTable(String tableName, Map<String, List<Map<String, Object>>> columnsMap);

    /**
     * tableName, ddlTemplate, paramMap을 받아 수행할 Script를 생성한 후 Script를 수행한다.
     */
    String executeDDL(String tableName, String ddlTemplate, Map<String, Object> paramMap);

    /**
     * basePackage 범위 내의 '@Table'을 구현한 Entity 목록 추출.
     */
    List<Class<?>> scanEntity(String basePackage);

    /**
     * SQL을 실행하여 DDL 실행.
     */
    Boolean executeBySql(String sql);

    /**
     * Procedure가 생성된 표준 SQL 가져오기 실행.
     */
    String getProcedureDef(String name);

    /**
     * Procedure 생성 템플릿 SQL.
     */
    String getCreateProcedureTemplate(String name);

    /**
     * Procedure 생성.
     */
    Boolean createProcedureBySql(String sql);

    /**
     * Procedure 수정.
     */
    Boolean alterProcedureBySql(String sql);

    /**
     * Procedure 삭제.
     */
    Boolean dropProcedureBySql(String sql);

    /**
     * Procedure Name을 이용하여 삭제.
     */
    Boolean dropProcedureByName(String name);

    Boolean dropProcedureByName(String name, String... paramTypes);

    Boolean dropProcedureByName(String name, List<String> paramTypes);
}