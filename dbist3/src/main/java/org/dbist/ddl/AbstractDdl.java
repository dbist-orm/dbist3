package org.dbist.ddl;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.apache.commons.lang.ClassUtils;
import org.dbist.DbistConstants;
import org.dbist.annotation.Column;
import org.dbist.annotation.PrimaryKey;
import org.dbist.annotation.Sequence;
import org.dbist.annotation.Table;
import org.dbist.ddl.mapper.DdlMapper;
import org.dbist.ddl.mapper.DdlMapperDB2;
import org.dbist.ddl.mapper.DdlMapperH2;
import org.dbist.ddl.mapper.DdlMapperMysql;
import org.dbist.ddl.mapper.DdlMapperOracle;
import org.dbist.ddl.mapper.DdlMapperPostgresql;
import org.dbist.ddl.mapper.DdlMapperSqlserver;
import org.dbist.dml.impl.DmlJdbc;
import org.dbist.exception.DbistRuntimeException;
import org.dbist.metadata.TableCol;
import org.dbist.metadata.TableIdx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.env.Environment;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.util.StringUtils;

import net.sf.common.util.ValueUtils;

public abstract class AbstractDdl implements Ddl {

    /**
     * Logger
     */
    private static final Logger logger = LoggerFactory.getLogger(AbstractDdl.class);

    /**
     * Table Column Row Mapper
     */
    private static final RowMapper<TableCol> TABLECOLUMN_ROWMAPPER = new TableColumnRowMapper();

    private static final String MSG_QUERYNOTFOUND = "Couldn't find ${queryName} query of dbType: ${dbType}. this type maybe unsupported yet.";

    @Resource
    public Environment env;

    @Resource(name = "dml")
    public DmlJdbc dml;

    /**
     * DDL Mapper
     */
    private DdlMapper ddlMapper;
    /**
     * Database domain
     */
    private String domain;
    /**
     * 사용자 계정
     */
    private String account;

    /**
     * Table에 대한 메타데이터를 조회 후 매핑 ...
     *
     * @author shortstop
     */
    static class TableColumnRowMapper implements RowMapper<TableCol> {
        public TableCol mapRow(ResultSet rs, int rowNum) throws SQLException {
            TableCol tabColumn = new TableCol();
            tabColumn.setName(rs.getString("name"));
            tabColumn.setDataType(rs.getString("dataType"));
            String nullableStr = rs.getString("nullable");
            Boolean nullable = (nullableStr != null && !nullableStr.equals("")
                && (nullableStr.equalsIgnoreCase("yes") || nullableStr.equalsIgnoreCase("y") || nullableStr.equalsIgnoreCase("true") || nullableStr.equalsIgnoreCase("t")));
            tabColumn.setNullable(nullable);
            tabColumn.setLength(rs.getInt("length"));
            // tabColumn.setComment(rs.getString("comment"));
            return tabColumn;
        }
    }

    public DdlMapper getDdlMapper() {
        if (this.ddlMapper == null) {
            String dbType = dml.getDbType();
            if (dbType.equals(DbistConstants.POSTGRESQL)) {
                this.ddlMapper = new DdlMapperPostgresql();
            } else if (dbType.equals(DbistConstants.MYSQL)) {
                this.ddlMapper = new DdlMapperMysql();
            } else if (dbType.equals(DbistConstants.ORACLE)) {
                this.ddlMapper = new DdlMapperOracle();
            } else if (dbType.equals(DbistConstants.DB2)) {
                this.ddlMapper = new DdlMapperDB2();
            } else if (dbType.equals(DbistConstants.SQLSERVER)) {
                this.ddlMapper = new DdlMapperSqlserver();
            } else if (dbType.equals(DbistConstants.H2)) {
                this.ddlMapper = new DdlMapperH2();
            }
        }

        return this.ddlMapper;
    }

    public void setDdlMapper(DdlMapper ddlMapper) {
        this.ddlMapper = ddlMapper;
    }

    public String getAccount() {
        if (env != null || this.account == null) {
            this.account = env.getProperty("spring.datasource.username");
        }

        return this.account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public String getDomain() {
        if (this.domain == null) {
            this.domain = this.dml.getDomain();
        }

        return this.domain;
    }

    public DmlJdbc getDml() {
        return dml;
    }

    public void setDml(DmlJdbc dml) {
        this.dml = dml;
    }

    @Override
    public org.dbist.metadata.Table getTable(String tableName) {
        return this.dml.getTable(tableName);
    }

    @Override
    public org.dbist.metadata.Table getTable(Class<?> entityClass) {
        return this.dml.getTable(entityClass);
    }

    @Override
    public List<TableIdx> getTableIndexes(Class<?> entityClass) {
        List<TableIdx> indexList = new ArrayList<TableIdx>();
        Annotation tableAnn = AnnotationUtils.findAnnotation(entityClass, Table.class);
        if (tableAnn == null) {
            return indexList;
        }

        Map<String, Object> tableInfo = AnnotationUtils.getAnnotationAttributes(tableAnn);
        org.dbist.annotation.Index[] indexes = (org.dbist.annotation.Index[]) tableInfo.get("indexes");
        String tableName = (String) tableInfo.get("name");

        for (org.dbist.annotation.Index idx : indexes) {
            String columns = idx.columnList().replace(" ", "");
            indexList.add(new TableIdx(tableName, idx.name(), idx.unique(), columns));
        }

        return indexList;
    }

    @Override
    public List<TableIdx> getTableIndexes(String tableName) {
        DdlMapper ddlMap = this.getDdlMapper();
        String indexSearchSql = ddlMap.getTableIndexSql(this.getDomain(), tableName);
        RowMapper<TableIdx> indexRowMapper = ddlMap.getIndexMetaRowMapper();
        List<TableIdx> indexes = this.dml.getJdbcOperations().query(indexSearchSql, indexRowMapper);

        for (TableIdx tableIdx : indexes) {
            tableIdx.setIndexFields(tableIdx.getIndexFields().toLowerCase());
            tableIdx.setIndexName(tableIdx.getIndexName().toLowerCase());
        }

        indexes = ddlMap.refineIndexList(indexes);
        return indexes;
    }

    @Override
    public List<TableCol> getTableCols(org.dbist.metadata.Table table) {
        String sql = "view".equals(table.getType()) ? this.dml.getQueryMapper().getQueryViewColumns() : this.dml.getQueryMapper().getQueryColumns();
        if (sql == null)
            throw new IllegalArgumentException(ValueUtils.populate(MSG_QUERYNOTFOUND, ValueUtils.toMap("queryName: table columns", "dbType:" + this.dml.getDbType())));

        String domainName = table.getDomain();
        int dotIndex = domainName.indexOf('.');
        domainName = dotIndex < 0 ? domainName : domainName.substring(0, dotIndex);
        sql = StringUtils.replace(sql, "${domain}", domainName);
        return this.dml.getJdbcOperations().query(sql, new Object[]{table.getName()}, TABLECOLUMN_ROWMAPPER);
    }

    /**
     * DB에 Table이 존재하는지 확인.
     *
     * @param tableName
     * @return
     */
    public boolean isTableExist(String tableName) {
        String checkQuery = this.tableCheckSql();

        Map<String, Object> params = new HashMap<String, Object>();
        params.put("domain", this.getDomain());
        params.put("tableName", tableName);
        params.put("owner", this.getAccount());

        try {
            String script = dml.getPreprocessor().process(checkQuery, params);
            int count = this.dml.selectSizeBySql(script, null);
            return count > 0;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> getAllTables() {
        String query = this.getDdlMapper().getUserTables();
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("domain", this.getDomain());
        params.put("owner", this.getAccount());

        try {
            String script = dml.getPreprocessor().process(query, params);
            List<String> list = this.dml.selectListBySql(script, new HashMap<String, Object>(), String.class, 0, 0);
            return list;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Table이 존재하는지 check를 위한 쿼리 - 에러가 발생하면 안 되고 테이블 존재 개수를 리턴해야 함
     *
     * @return
     */
    public abstract String tableCheckSql();

    /**
     * ddlTemplate, paramMap으로 ddl script를 생성하고 그 script를 실행
     *
     * @param tableName
     * @param ddlTemplate
     * @param paramMap
     * @return
     */
    public String executeDDL(String tableName, String ddlTemplate, Map<String, Object> paramMap) {
        try {
            String script = dml.getPreprocessor().process(ddlTemplate, paramMap);
            String[] sqlArr = StringUtils.tokenizeToStringArray(script, ";");

            for (String sql : sqlArr) {
                logger.info(sql);
                dml.getJdbcOperations().execute(sql);
            }
        } catch (Exception e) {
            String message = e.getCause().getMessage();
            logger.error(message + "[" + tableName + "]");
            return new StringBuilder(tableName).append(" : ").append(message).toString();
        }

        return null;
    }

    @Override
    public List<Class<?>> scanEntity(String basePackage) {
        List<Class<?>> classList = new ArrayList<>();

        ClassPathScanningCandidateComponentProvider scanner = new ClassPathScanningCandidateComponentProvider(false);
        scanner.addIncludeFilter(new AnnotationTypeFilter(Table.class));

        String[] basePackages = StringUtils.tokenizeToStringArray(basePackage, ",");
        if (ValueUtils.isEmpty(basePackages)) {
            throw new DbistRuntimeException("Base Package is Empty.");
        }

        for (String path : basePackages) {
            for (BeanDefinition bd : scanner.findCandidateComponents(path)) {
                try {
                    Class<?> clazz = ClassUtils.getClass(bd.getBeanClassName());
                    Annotation tableAnn = AnnotationUtils.findAnnotation(clazz, Table.class);

                    if (tableAnn == null) {
                        continue;
                    }

                    Map<String, Object> tableAnnInfoMap = AnnotationUtils.getAnnotationAttributes(tableAnn);
                    if (!(boolean) tableAnnInfoMap.get("isRef")) {
                        classList.add(clazz);
                    }
                } catch (Exception e) {
                    logger.debug(e.getMessage() + "[" + bd.getBeanClassName() + "]", e);
                }
            }
        }
        return classList;
    }

    /**
     * Entity와 Mapping되어있는 Table Name 가져오기 실행.
     *
     * @param entity
     * @return
     */
    protected Map<String, Object> getTableInfoMap(Class<?> entity) {
        Annotation tableAnn = AnnotationUtils.findAnnotation(entity, Table.class);
        if (tableAnn == null) {
            return null;
        }

        return AnnotationUtils.getAnnotationAttributes(tableAnn);
    }

    /**
     * Field의 '@Colum'에 대한 속정 정보 추출.
     *
     * @param clazz
     * @return
     */
    protected List<Map<String, Object>> getFieldAnnAtrInfoMap(Class<?> clazz) {
        List<Map<String, Object>> fieldAnnAttrInfoMapList = new ArrayList<>();
        List<Field> fields = this.declaredFieldList(clazz);

        for (Field field : fields) {
            Map<String, Object> fieldAnnAttrInfoMap = new HashMap<String, Object>();
            Map<String, Object> colAnnInfoMap = new HashMap<String, Object>();
            Map<String, Object> pkAnnInfoMap = new HashMap<String, Object>();

            Annotation columnAnn = field.getAnnotation(Column.class);
            if (columnAnn != null) {
                colAnnInfoMap = AnnotationUtils.getAnnotationAttributes(columnAnn);
                fieldAnnAttrInfoMap.putAll(colAnnInfoMap);
            }

            // PrimaryKey에 따른, Default Setting 적용.
            Annotation pkAnn = field.getAnnotation(PrimaryKey.class);
            if (pkAnn != null) {
                String colName = (String) colAnnInfoMap.get("name");
                String fieldName = (colName == null) ? field.getName() : colName;

                pkAnnInfoMap.put("primaryKey", fieldName);
                pkAnnInfoMap.put("name", fieldName);
                pkAnnInfoMap.put("nullable", false);
                fieldAnnAttrInfoMap.putAll(pkAnnInfoMap);
            }

            Annotation sequenceAnn = field.getAnnotation(Sequence.class);
            if (sequenceAnn != null) {
                Map<String, Object> seqMap = AnnotationUtils.getAnnotationAttributes(sequenceAnn);
                fieldAnnAttrInfoMap.put("sequenceName", seqMap.get("name"));
            }

            if (fieldAnnAttrInfoMap.size() > 1) {
                fieldAnnAttrInfoMap.put("fieldType", field.getType().getSimpleName());
                fieldAnnAttrInfoMapList.add(fieldAnnAttrInfoMap);
            }
        }

        return fieldAnnAttrInfoMapList;
    }

    /**
     * Class에 정의되어 있는 Field 목록 추출.(부모 객체의 Field 포함)
     *
     * @param clazz
     * @return
     */
    protected List<Field> declaredFieldList(Class<?> clazz) {
        List<Field> filedList = new ArrayList<Field>();
        Class<?> targetClass = clazz;

        do {
            Field[] fields = targetClass.getDeclaredFields();
            for (Field field : fields) {
                filedList.add(field);
            }

            targetClass = targetClass.getSuperclass();
        } while (targetClass != null && targetClass instanceof Object);

        return filedList;
    }
}