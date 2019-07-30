package org.dbist.ddl.mapper;

import java.util.List;
import java.util.Map;

import org.dbist.metadata.TableIdx;
import org.springframework.jdbc.core.RowMapper;

/**
 * DDLMapper Interface
 *
 * @author shortstop
 */
public interface DdlMapper {

    /**
     * Tablespace 지정 ddl
     *
     * @param useDataTBSpace
     * @param useIdxTBSpace
     */
    void setTableSpace(boolean useDataTBSpace, boolean useIdxTBSpace);

    /**
     * 현재 데이터베이스에 로그인 한 사용자가 Owner인 모든 테이블 리스트를 조회하는 쿼리
     *
     * @return
     */
    String getUserTables();

    /**
     * Table 존재 체크를 위한 쿼리
     *
     * @return
     */
    String getTableCheckSql();

    /**
     * Table의 인덱스 메타 데이터를 조회하는 쿼리
     *
     * @param domainName
     * @param tableName
     * @return
     */
    String getTableIndexSql(String domainName, String tableName);

    /**
     * 테이블 인덱스 정보를 조회를 위한 Row Mapper
     *
     * @return
     */
    RowMapper<TableIdx> getIndexMetaRowMapper();

    /**
     * 데이터베이스에서 테이블 인덱스 정보를 조회한 후 데이터를 정제한다.
     *
     * @param indexes
     * @return
     */
    List<TableIdx> refineIndexList(List<TableIdx> indexes);

    /**
     * Sequence, Table, Index Template
     *
     * @return
     */
    String getDdlTemplate();

    /**
     * Sequence 생성 Template.
     *
     * @return
     */
    String sequenceTemplate();

    /**
     * Table 생성 Template.
     *
     * @return
     */
    String tableTemplate();

    /**
     * Table 변경 Template
     *
     * @return
     */
    String alterTableTemplate();

    /**
     * Column 변경 Template
     *
     * @return
     */
    String addColumnTemplate();

    /**
     * Column 삭제 Template
     *
     * @return
     */
    String removeColumnTemplate();

    /**
     * Column 타입 변경 Template
     *
     * @return
     */
    String modifyColumnTemplate();

    /**
     * Column 명 변경 Template
     *
     * @return
     */
    String renameColumnTemplate();

    /**
     * Column Nullable 변경 Template
     *
     * @return
     */
    String modifyNullableColumnTemplate();

    /**
     * Column Default 값 변경 Template
     *
     * @return
     */
    String modifyDefaultColumnTemplate();

    /**
     * Index 생성 Template.
     *
     * @return
     */
    String indexTemplate();

    /**
     * Drop Sequence, Index, Table Template
     *
     * @return
     */
    String dropDdlTemplate();

    /**
     * Drop Sequence Template
     *
     * @return
     */
    String dropSequenceTemplate();

    /**
     * Drop Index Template
     *
     * @return
     */
    String dropIndexTemplate();

    /**
     * Drop Table Template
     *
     * @return
     */
    String dropTableTemplate();

    /**
     * Resource Column의 Type을 DB Type으로 변경.
     *
     * @return
     */
    String toDatabaseType(Map<String, Object> map);

    /**
     * Java의 타입명과 매핑되는 데이터베이스 테이블 타입을 리턴한다.
     *
     * @param javaType
     * @return
     */
    String toDatabaseType(String javaType);

    /**
     * db 컬럼 타입과 매핑되는 자바 타입을 리턴한다.
     *
     * @param dbColType
     * @return
     */
    String toJavaType(String dbColType);

    /**
     * Drop Procedure Template
     *
     * @return
     */
    String dropProcedureTemplate(String name);

    /**
     * Procedure Definition SQL
     *
     * @return
     */
    String getPprocedureDefTemplate();

    /**
     * Create Procedure Template
     *
     * @return
     */
    String getCreatePprocedureTemplate();
}
