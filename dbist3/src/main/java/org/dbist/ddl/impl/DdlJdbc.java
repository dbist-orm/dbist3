package org.dbist.ddl.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import org.dbist.annotation.Index;
import org.dbist.ddl.AbstractDdl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import net.sf.common.util.ValueUtils;

public class DdlJdbc extends AbstractDdl {

    /**
     * Logger
     */
    private static final Logger logger = LoggerFactory.getLogger(DdlJdbc.class);

    /**
     * 데이터 테이블 스페이스 명
     */
    private String dataTBSpace;
    /**
     * 인덱스 테이블 스페이스 명
     */
    private String idxTBSpace;

    @Override
    public void setTableSpace(String dataTBSpace, String idxTBSpace) {
        // 데이터 테이블 스페이스 사용 여부
        boolean useDataTBSpace;

        // 인덱스 테이블 스페이스 사용 여부
        boolean useIdxTBSpace;

        if (StringUtils.isEmpty(dataTBSpace)) {
            useDataTBSpace = false;
        } else {
            useDataTBSpace = true;
            this.dataTBSpace = dataTBSpace;
        }

        if (StringUtils.isEmpty(idxTBSpace)) {
            useIdxTBSpace = false;
        } else {
            useIdxTBSpace = true;
            this.idxTBSpace = idxTBSpace;
        }

        this.getDdlMapper().setTableSpace(useDataTBSpace, useIdxTBSpace);
    }

    @Override
    public String getDataTableSpace() {
        return this.dataTBSpace;
    }

    @Override
    public String getIndexTableSpace() {
        return this.idxTBSpace;
    }

    @Override
    public void createTable() {
        String path = env.getProperty("dbist.base.entity.path");
        this.createTable(path);
    }

    @Override
    public void createTable(String basePackagePath) {
        if (ValueUtils.isEmpty(basePackagePath)) {
            return;
        }

        StringBuilder summary = new StringBuilder();
        summary.append("\n==================== DDL Start ============================\n");

        // @Table 구현체 Entity Scan
        List<Class<?>> classList = scanEntity(basePackagePath);
        for (Class<?> clazz : classList) {
            String result = createTable(clazz);
            if (result != null) {
                summary.append(result);
            }
        }

        summary.append("\n==================== DDL End ==============================\n");
        logger.debug(summary.toString());
    }

    @Override
    public String createTable(Class<?> entity) {
        Map<String, Object> tableInfoMap = getTableInfoMap(entity);

        boolean ignore = (boolean) tableInfoMap.get("ignoreDdl");
        if (ignore) {
            return "Skip create table - entity [" + entity.getName() + "]!";
        }

        String tableName = (String) tableInfoMap.get("name");
        if (tableName == null) {
            return "Table Name is empty";
        }

        if (this.isTableExist(tableName)) {
            return "Table [" + tableName + "] is already exist!";
        }

        String sequenceName = null;
        StringJoiner primaryFields = new StringJoiner(",");
        List<String> uniqueIndexFields = new ArrayList<>();
        List<Map<String, String>> columnAttrMapList = new ArrayList<>();

        List<Map<String, Object>> fieldInfoMap = this.getFieldAnnAtrInfoMap(entity);

        for (Map<String, Object> attribute : fieldInfoMap) {
            String colName = (String) attribute.get("name");
            String seqName = (String) attribute.get("sequenceName");
            boolean nullable = ValueUtils.toBoolean(attribute.get("nullable"), true);

            Map<String, String> colInfoMap = new HashMap<>();

            // Set Sequence
            if (!ValueUtils.isEmpty(seqName)) {
                colInfoMap.put("sequenceAble", "true");
                sequenceName = seqName;
            }

            // Set Primary Key
            String primaryKey = (String) attribute.get("primaryKey");
            if (primaryKey != null) {
                colInfoMap.put("primaryKey", primaryKey);
                primaryFields.add(primaryKey);
            }

            // @Field의 Unique Index 정보 추출
            if (ValueUtils.toBoolean(attribute.get("unique"), false)) {
                uniqueIndexFields.add(colName);
            }

            // Set Column Info
            colInfoMap.put("name", colName);
            colInfoMap.put("nullable", nullable ? "" : "NOT NULL");
            colInfoMap.put("col_type", getDdlMapper().toDatabaseType(attribute));

            columnAttrMapList.add(colInfoMap);
        }

        Object indexInfo = tableInfoMap.get("indexes");
        List<String> uniqueIndexList = new ArrayList<>();
        List<Map<String, Object>> indexList = new ArrayList<>();

        // @Table의 Unique Index정보 추출
        if (!ValueUtils.isEmpty(indexInfo)) {
            Index[] indexes = (Index[]) indexInfo;
            for (Index idx : indexes) {
                Map<String, Object> indexMap = new HashMap<>();
                boolean isUnique = idx.unique();
                String columnList = idx.columnList();

                indexMap.put("unique", isUnique);
                indexMap.put("name", idx.name());
                indexMap.put("columnList", columnList);
                indexList.add(indexMap);

                if (isUnique) {
                    uniqueIndexList.add(columnList);
                }
            }
        }

        // 이미 추가된 Index는 대상에서 제외
        uniqueIndexFields.removeAll(uniqueIndexList);
        // Filed에 정의되어 있는 Unique정보를 Index에 추가
        for (String fieldName : uniqueIndexFields) {
            Map<String, Object> uniqueIndexMap = new HashMap<>();
            uniqueIndexMap.put("unique", true);
            uniqueIndexMap.put("name", "ix_" + tableName + "_on_" + fieldName);
            uniqueIndexMap.put("columnList", fieldName);
            indexList.add(uniqueIndexMap);
        }

        // Velocity PramMap
        Map<String, Object> paramMap = new HashMap<>();
        paramMap.put("account", this.getAccount());
        paramMap.put("tableName", tableName);
        paramMap.put("columns", columnAttrMapList);
        paramMap.put("primaryKeys", primaryFields.toString());
        paramMap.put("indexes", indexList);

        paramMap.put("dataTableSpaceName", this.dataTBSpace);
        paramMap.put("idxTableSpaceName", this.idxTBSpace);

        if (!ValueUtils.isEmpty(sequenceName)) {
            paramMap.put("sequenceName", sequenceName);
        }

        // Template 가져오기
        String template = getDdlMapper().getDdlTemplate();
        String resultStr = this.executeDDL(tableName, template, paramMap);

        if (resultStr == null) {
            logger.info("Table [" + tableName + "] is created.");
            return null;
        } else {
            return resultStr;
        }
    }

    @Override
    public String dropTable(Class<?> entity) {
        Map<String, Object> tableInfoMap = getTableInfoMap(entity);
        String tableName = (String) tableInfoMap.get("name");

        if (tableName == null) {
            return "Table Name is empty";
        }

        if (!this.isTableExist(tableName)) {
            return "Table [" + tableName + "] is not exist!";
        }

        String sequenceName = tableName + "_id_seq";
        return this.dropTable(tableName, sequenceName, null);
    }

    @Override
    public String dropTable(String tableName, String sequenceName, List<String> indexNames) {
        if (!this.isTableExist(tableName)) {
            return "Table [" + tableName + "] is not exist!";
        }

        String dropTableTemplate = this.getDdlMapper().dropTableTemplate();

        Map<String, Object> paramMap = new HashMap<>();
        paramMap.put("domain", this.dml.getDomain());
        paramMap.put("tableName", tableName);
        paramMap.put("sequenceName", sequenceName);

        {
            String result = this.executeDDL(tableName, dropTableTemplate, paramMap);
            if (result != null) {
                return result;
            }
        }

        if (sequenceName != null && !sequenceName.equalsIgnoreCase("")) {
            String dropSequenceTemplate = this.getDdlMapper().dropSequenceTemplate();
            paramMap.put("sequenceName", sequenceName);

            String result = this.executeDDL(tableName, dropSequenceTemplate, paramMap);
            if (result != null) {
                return result;
            }
        }

        if (indexNames != null && !indexNames.isEmpty()) {
            String dropIndexTemplate = this.getDdlMapper().dropIndexTemplate();
            paramMap.put("indexes", indexNames);

            String result = this.executeDDL(tableName, dropIndexTemplate, paramMap);
            if (result != null) {
                return result;
            }
        }

        return null;
    }

    @Override
    public String alterTable(String tableName, Map<String, List<Map<String, Object>>> columnsMap) {
        if (!this.isTableExist(tableName)) {
            return "Table [" + tableName + "] is not exist!";
        }

        Map<String, Object> paramMap = new HashMap<>();
        paramMap.put("domain", this.dml.getDomain());
        paramMap.put("tableName", tableName);

        List<Map<String, Object>> addColumns = columnsMap.get("addColumns");
        if (addColumns != null && !addColumns.isEmpty()) {
            String addColumnTemplate = getDdlMapper().addColumnTemplate();
            paramMap.put("addColumns", addColumns);
            String result = this.executeDDL(tableName, addColumnTemplate, paramMap);

            if (result != null) {
                return result;
            }
        }

        List<Map<String, Object>> removeColumns = columnsMap.get("removeColumns");
        if (removeColumns != null && !removeColumns.isEmpty()) {
            String removeColumnTemplate = getDdlMapper().removeColumnTemplate();
            paramMap.put("removeColumns", removeColumns);
            String result = this.executeDDL(tableName, removeColumnTemplate, paramMap);

            if (result != null) {
                return result;
            }
        }

        List<Map<String, Object>> modifyColumns = columnsMap.get("modifyColumns");
        if (modifyColumns != null && !modifyColumns.isEmpty()) {
            String modifyColumnTemplate = getDdlMapper().modifyColumnTemplate();
            paramMap.put("modifyColumns", modifyColumns);
            String result = this.executeDDL(tableName, modifyColumnTemplate, paramMap);

            if (result != null) {
                return result;
            }
        }

        List<Map<String, Object>> nullableColumns = columnsMap.get("nullableColumns");
        if (nullableColumns != null && !nullableColumns.isEmpty()) {
            String nullableColumnTemplate = getDdlMapper().modifyNullableColumnTemplate();
            paramMap.put("nullableColumns", nullableColumns);
            String result = this.executeDDL(tableName, nullableColumnTemplate, paramMap);

            if (result != null) {
                return result;
            }
        }

        return null;
    }

    @Override
    public String tableCheckSql() {
        return this.getDdlMapper().getTableCheckSql();
    }

    @Override
    public Boolean executeBySql(String sql) {
        ValueUtils.assertNotEmpty("sql", sql);
        dml.getJdbcOperations().execute(sql.trim());
        return true;
    }

    @Override
    public String getProcedureDef(String name) {
        String sql = getDdlMapper().getPprocedureDefTemplate();

        Map<String, Object> paramMap = new HashMap<>();
        paramMap.put("domain", Arrays.asList(StringUtils.tokenizeToStringArray(this.dml.getDomain().toUpperCase(), ",")));
        paramMap.put("name", name);

        try {
            List<String> values = dml.selectListBySql(sql, paramMap, String.class, 0, 0);
            if (values == null || values.isEmpty())
                return null;

            String procedureDef;
            if (values.size() == 1) {
                procedureDef = values.get(0);
            } else {
                StringBuilder appender = new StringBuilder();
                for (String value : values) {
                    if (!value.isEmpty())
                        appender.append(value);
                }

                procedureDef = appender.toString();
            }

            return procedureDef;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getCreateProcedureTemplate(String name) {
        Map<String, Object> params = new HashMap<>();
        params.put("name", name);

        String sql = getDdlMapper().getCreatePprocedureTemplate();
        try {
            return dml.getPreprocessor().process(sql, params);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Boolean createProcedureBySql(String sql) {
        return this.executeBySql(sql);
    }

    @Override
    public Boolean alterProcedureBySql(String sql) {
        return this.executeBySql(sql);
    }

    @Override
    public Boolean dropProcedureBySql(String sql) {
        return this.executeBySql(sql);
    }

    @Override
    public Boolean dropProcedureByName(String name) {
        return executeBySql(this.getDdlMapper().dropProcedureTemplate(name));
    }

    @Override
    public Boolean dropProcedureByName(String name, String... paramTypes) {
        return this.dropProcedureByName(name, Arrays.asList(paramTypes));
    }

    @Override
    public Boolean dropProcedureByName(String name, List<String> paramTypes) {
        String sql = this.getDdlMapper().dropProcedureTemplate(name);

        StringBuilder paramSql = new StringBuilder();
        for (String type : paramTypes) {
            paramSql.append(type).append(",");
        }

        Map<String, Object> paramMap = new HashMap<>();
        if (paramSql.length() > 0) {
            paramMap.put("paramTypes", paramSql.substring(0, paramSql.lastIndexOf(",")));
        }

        try {
            String script = dml.getPreprocessor().process(sql, paramMap);
            return executeBySql(script);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}