package org.dbist.ddl.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import org.dbist.DbistConstants;
import org.dbist.metadata.TableIdx;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.util.StringUtils;

import net.sf.common.util.ValueUtils;

/**
 * Oracle용 DDL Mapper
 *
 * @author shortstop
 */
public class DdlMapperOracle extends DdlMapperAbstract implements DdlMapper {
    /**
     * Table Index RowMapper
     */
    private static final RowMapper<TableIdx> TABLE_INDEX_ROWMAPPER = new TableIndexRowMapper();

    boolean useDataTBSpace = false;
    boolean useIdxTBSpace = false;

    @Override
    public void setTableSpace(boolean useDataTBSpace, boolean useIdxTBSpace) {
        this.useDataTBSpace = useDataTBSpace;
        this.useIdxTBSpace = useIdxTBSpace;
    }

    @Override
    public String getDdlTemplate() {
        StringBuilder template = new StringBuilder();

        // Create Sequence Template
        template.append(this.sequenceTemplate());
        template.append("\n\n");

        // Create Table Template
        template.append(this.tableTemplate());
        template.append("\n\n");

        // Create Index Template
        template.append(this.indexTemplate());
        return template.toString();
    }

    @Override
    public String sequenceTemplate() {
        StringJoiner joiner = new StringJoiner("\n");
        joiner.add("#if($sequenceName && $sequenceName != '')");
        joiner.add("CREATE SEQUENCE $sequenceName");
        joiner.add("	START WITH 1");
        joiner.add("	INCREMENT BY 1");
        joiner.add("	MINVALUE 1");
        joiner.add("	MAXVALUE 9999999999999999999999999999");
        joiner.add("	CACHE 20;");
        joiner.add("#end");
        return joiner.toString();
    }

    @Override
    public String tableTemplate() {
        StringJoiner joiner = new StringJoiner("\n");
        joiner.add("CREATE TABLE $tableName");
        joiner.add("(");
        joiner.add("#foreach( $column in $columns )");
        joiner.add("$column.name $column.col_type $column.nullable,");
        joiner.add("#end");
        joiner.add("CONSTRAINT ${tableName}_pkey PRIMARY KEY ($primaryKeys)");

        if (useIdxTBSpace) {
            joiner.add("using index tablespace $idxTableSpaceName");
        }

        joiner.add(useDataTBSpace ? ") tablespace $dataTableSpaceName ;" : ");");
        return joiner.toString();
    }

    @Override
    public String alterTableTemplate() {
        StringJoiner joiner = new StringJoiner("\n");
        joiner.add(this.removeColumnTemplate());
        joiner.add(this.addColumnTemplate());
        joiner.add(this.modifyColumnTemplate());
        joiner.add(this.modifyNullableColumnTemplate());
        //joiner.add(this.renameColumnTemplate());
        //joiner.add(this.modifyDefaultColumnTemplate());
        return joiner.toString();
    }

    @Override
    public String indexTemplate() {
        StringJoiner joiner = new StringJoiner("\n");
        joiner.add("#foreach( $index in $indexes )");
        joiner.add("#if($index.unique)");
        joiner.add("CREATE UNIQUE INDEX $index.name");
        joiner.add("ON $tableName");
        joiner.add("($index.columnList)");

        if (useIdxTBSpace)
            joiner.add(" tablespace $idxTableSpaceName");

        joiner.add(";");
        joiner.add("#else");
        joiner.add("CREATE INDEX $index.name");
        joiner.add("ON $tableName");
        joiner.add("($index.columnList) ");

        if (useIdxTBSpace)
            joiner.add(" tablespace $idxTableSpaceName");

        joiner.add(";");
        joiner.add("#end");
        joiner.add("\n");
        joiner.add("#end");

        return joiner.toString();
    }

    @Override
    public String toDatabaseType(Map<String, Object> map) {
        Integer length = ValueUtils.toInteger(map.get("length"));
        String requiredType = String.valueOf(map.get("type"));
        String fieldType = (requiredType == null || requiredType.equalsIgnoreCase("null") || requiredType.equalsIgnoreCase("empty")) ? (String) map.get("fieldType") : requiredType;
        return this.setColumnSize(fieldType, length);
    }

    private String setColumnSize(String fieldType, Integer length) {
        String type = this.toDatabaseType(fieldType);
        String columnSize = null;

        if (length != null && length > 0 && length != 255) {
            columnSize = length.toString();
        } else {
//            if (type.equalsIgnoreCase(DbistConstants.COLUMN_TYPE_VARCHAR2)) {
//                columnSize = "255";
//            } else if (fieldType.equalsIgnoreCase(DbistConstants.FIELD_TYPE_BOOLEAN)) {
//                columnSize = "1";
//            } else if (fieldType.equalsIgnoreCase(DbistConstants.FIELD_TYPE_DATETIME)) {
//                columnSize = "6";
//            } else if (fieldType.equalsIgnoreCase(DbistConstants.FIELD_TYPE_TIMESTAMP)) {
//                columnSize = "6";
//            } else if (fieldType.equalsIgnoreCase(DbistConstants.FIELD_TYPE_DECIMAL)) {
//                columnSize = "15"
//            } else if (fieldType.equalsIgnoreCase(DbistConstants.FIELD_TYPE_DOUBLE)) {
//                columnSize = "6";
//            } else if (fieldType.equalsIgnoreCase(DbistConstants.FIELD_TYPE_FLOAT)) {
//                columnSize = this.getFloatFieldSize();
//            } else if (fieldType.equalsIgnoreCase(DbistConstants.FIELD_TYPE_INT) || fieldType.equalsIgnoreCase(DbistConstants.FIELD_TYPE_INTEGER)) {
//                columnSize = this.getIntegerFieldSize();
//            } else if (fieldType.equalsIgnoreCase(DbistConstants.FIELD_TYPE_LONG)) {
//                columnSize = this.getLongFieldSize();
//            }
        }

        /*
         * Column Size 설정.
         */
        if (columnSize == null) {
            return type;
        }

        String[] columnSizeArr = StringUtils.tokenizeToStringArray(columnSize, ",");
        Integer precision = Integer.parseInt(columnSizeArr[0]);
        Integer scale = Integer.parseInt(columnSizeArr.length > 1 ? columnSizeArr[1] : "3");

        // Number Type의 Max size 초과 시
        if (this.isNumberType(fieldType)) {
            int maxNumberSize = DbistConstants.COLUMN_NUMBER_MAX_SIZE;
            columnSize = precision > maxNumberSize ? Integer.toString(maxNumberSize) : Integer.toString(precision);
        }

        // 소수점 타입이 아닌 경우
        if (!this.isRealType(fieldType)) {
            return type + String.format("(%s)", columnSize);
        }

        // 소수점 자릿수가 존재하지 않거나, 1보다 작을 경우
        if (scale == null || scale < 1) {
            return type + String.format("(%s)", columnSize);
        }

        return type + String.format("(%s)", columnSize + "," + scale);
    }

    @Override
    public String toDatabaseType(String fieldType) {
        if (fieldType == null || fieldType.equals(""))
            return DbistConstants.COLUMN_TYPE_VARCHAR2;
        else if (fieldType.equalsIgnoreCase(DbistConstants.FIELD_TYPE_TEXT))
            return DbistConstants.COLUMN_TYPE_CLOB;
        else if (fieldType.equalsIgnoreCase(DbistConstants.FIELD_TYPE_DATE))
            return DbistConstants.COLUMN_TYPE_DATE;
        else if (fieldType.equalsIgnoreCase(DbistConstants.FIELD_TYPE_DATETIME) || fieldType.equalsIgnoreCase(DbistConstants.FIELD_TYPE_TIMESTAMP))
            return DbistConstants.COLUMN_TYPE_TIMESTAMP;

        List<String> numberTypeList = new ArrayList<>();
        numberTypeList.add(DbistConstants.FIELD_TYPE_BOOLEAN.toUpperCase());
        numberTypeList.add(DbistConstants.FIELD_TYPE_INT.toUpperCase());
        numberTypeList.add(DbistConstants.FIELD_TYPE_INTEGER.toUpperCase());
        numberTypeList.add(DbistConstants.FIELD_TYPE_LONG.toUpperCase());
        numberTypeList.add(DbistConstants.FIELD_TYPE_FLOAT.toUpperCase());
        numberTypeList.add(DbistConstants.FIELD_TYPE_DOUBLE.toUpperCase());
        numberTypeList.add(DbistConstants.FIELD_TYPE_DECIMAL.toUpperCase());

        if (numberTypeList.contains(fieldType.toUpperCase())) {
            return DbistConstants.COLUMN_TYPE_NUMBER;
        }

        return DbistConstants.COLUMN_TYPE_VARCHAR2;
    }

    @Override
    public String dropDdlTemplate() {
        StringBuilder template = new StringBuilder();

        // Drop Sequence Template
        template.append(this.dropSequenceTemplate());
        template.append("\n\n");

        // Drop Table Template
        template.append(this.dropTableTemplate());
        return template.toString();
    }

    @Override
    public String dropSequenceTemplate() {
        StringJoiner joiner = new StringJoiner("\n");
        joiner.add("DROP SEQUENCE $sequenceName");
        return joiner.toString();
    }

    @Override
    public String dropIndexTemplate() {
        StringJoiner joiner = new StringJoiner("\n");
        joiner.add("#foreach( $index in $indexes )");
        joiner.add("	DROP INDEX $index\n");
        joiner.add("#end");
        return joiner.toString();
    }

    @Override
    public String dropTableTemplate() {
        StringJoiner joiner = new StringJoiner("\n");
        joiner.add("DROP TABLE $tableName CASCADE CONSTRAINT");
        return joiner.toString();
    }

    @Override
    public String getTableCheckSql() {
        //return "select count(*) from all_tables where owner=upper('$owner') and table_name=upper('$tableName')";
        return "select 1 from user_tables where table_name=upper('$tableName')";
    }

    @Override
    public String getUserTables() {
        return "SELECT table_name FROM user_tables ORDER BY table_name";
    }

    @Override
    public String addColumnTemplate() {
        StringJoiner joiner = new StringJoiner("\n");
        joiner.add("#foreach( $column in $addColumns )");
        joiner.add("	ALTER TABLE $tableName ADD $column.name $column.type;\n");

        joiner.add("	#if($column.nullable && $column.nullable == 'NO')");
        joiner.add("		ALTER TABLE $tableName MODIFY $column.name NOT NULL;\n");
        joiner.add("	#end");

        //joiner.add("	#ifnotnull($column.defaultValue)");
        //joiner.add("		ALTER TABLE $tableName MODIFY $column.name DEFAULT '$column.defaultValue';\n");
        //joiner.add("	#end");

        joiner.add("#end");
        return joiner.toString();
    }

    @Override
    public String removeColumnTemplate() {
        StringJoiner joiner = new StringJoiner("\n");
        joiner.add("#foreach( $column in $removeColumns )");
        joiner.add("	ALTER TABLE $tableName DROP COLUMN $column.name CASCADE CONSTRAINTS;\n");
        joiner.add("#end");
        return joiner.toString();
    }

    @Override
    public String modifyColumnTemplate() {
        StringJoiner joiner = new StringJoiner("\n");
        joiner.add("#foreach( $column in $modifyColumns )");
        joiner.add("	ALTER TABLE $tableName MODIFY ($column.name $column.type);\n");
        joiner.add("#end");
        return joiner.toString();
    }

    @Override
    public String renameColumnTemplate() {
        StringJoiner joiner = new StringJoiner("\n");
        joiner.add("#foreach( $column in $renameColumns )");
        joiner.add("	ALTER TABLE $tableName RENAME COLUMN $column.nameFrom TO $column.nameTo;\n");
        joiner.add("#end");
        return joiner.toString();
    }

    @Override
    public String modifyNullableColumnTemplate() {
        StringJoiner joiner = new StringJoiner("\n");
        joiner.add("#foreach( $column in $nullableColumns )");
        joiner.add("	#if($column.nullable && $column.nullable == 'YES')");
        joiner.add("		ALTER TABLE $tableName MODIFY $column.name NULL;\n");
        joiner.add("	#end");
        joiner.add("	#if($column.nullable && $column.nullable == 'NO')");
        joiner.add("		ALTER TABLE $tableName MODIFY $column.name NOT NULL;\n");
        joiner.add("	#end");
        joiner.add("#end");
        return joiner.toString();
    }

    @Override
    public String modifyDefaultColumnTemplate() {
        StringJoiner joiner = new StringJoiner("\n");
        joiner.add("#foreach( $column in $defaultColumns )");
        joiner.add("	#if(!$column.defaultValue)");
        joiner.add("		ALTER TABLE $tableName MODIFY $column.name DEFAULT NULL;\n");
        joiner.add("	#else");
        joiner.add("		ALTER TABLE $tableName MODIFY $column.name DEFAULT '$column.defaultValue';\n");
        joiner.add("	#end");
        joiner.add("#end");
        return joiner.toString();
    }

    @Override
    public String getTableIndexSql(String domainName, String tableName) {
        StringBuilder sql = new StringBuilder();
        sql.append("select")
            .append("	ui.table_name, ui.index_name, ui.uniqueness, uic.column_name, uic.descend ")
            .append("from")
            .append("	user_indexes ui")
            .append("	inner join")
            .append("	user_ind_columns uic")
            .append("	on ui.index_name = uic.index_name ")
            .append("where")
            .append("	ui.table_name = '").append(tableName.toUpperCase()).append("' ")
            .append("order by")
            .append("	ui.index_name asc, uic.column_position asc ");
        return sql.toString();
    }

    @Override
    public String dropProcedureTemplate(String name) {
        return "DROP PROCEDURE " + name;
    }

    @Override
    public String getPprocedureDefTemplate() {
        StringJoiner sql = new StringJoiner("\n");
        sql.add("select text from all_source");
        sql.add("where upper(owner) in (:domain)");
        sql.add("and upper(type) = 'PROCEDURE'");
        sql.add("and upper(name) = upper(:name)");
        sql.add("order by name, line");
        return sql.toString();
    }

    @Override
    public String getCreatePprocedureTemplate() {
        return "create or replace procedure ${name}()";
    }

    @Override
    public RowMapper<TableIdx> getIndexMetaRowMapper() {
        return TABLE_INDEX_ROWMAPPER;
    }

    /**
     * Table Index RowMapper
     *
     * @author shortstop
     */
    static class TableIndexRowMapper implements RowMapper<TableIdx> {
        public TableIdx mapRow(ResultSet rs, int rowNum) throws SQLException {
            TableIdx tableIndex = new TableIdx();
            tableIndex.setTableName(rs.getString("table_name"));
            tableIndex.setIndexName(rs.getString("index_name"));
            tableIndex.setIndexFields(rs.getString("column_name"));
            String uniqueness = rs.getString("uniqueness");
            if (uniqueness != null) {
                boolean uniq = uniqueness.equalsIgnoreCase("UNIQUE");
                tableIndex.setUnique(uniq);
            }

            return tableIndex;
        }
    }

    @Override
    public List<TableIdx> refineIndexList(List<TableIdx> indexes) {
        List<TableIdx> newIndexes = new ArrayList<>();

        for (TableIdx currentIdx : indexes) {
            if (currentIdx.getIndexName().toUpperCase().endsWith("_PKEY")) {
                continue;
            }

            TableIdx foundIdx = this.findIndex(newIndexes, currentIdx);
            if (foundIdx == null) {
                foundIdx = currentIdx;
                newIndexes.add(foundIdx);
            }

            String prevIndexFields = foundIdx.getIndexFields();
            if (!prevIndexFields.equalsIgnoreCase(currentIdx.getIndexFields())) {
                foundIdx.setIndexFields(prevIndexFields + "," + currentIdx.getIndexFields());
            }
        }

        return newIndexes;
    }

    private TableIdx findIndex(List<TableIdx> newIndexes, TableIdx currentIdx) {
        for (TableIdx idx : newIndexes) {
            if (idx.getIndexName().equalsIgnoreCase(currentIdx.getIndexName())) {
                return idx;
            }
        }

        return null;
    }

    @Override
    public String toJavaType(String dbColType) {
        String type = "string";

        if (dbColType == null || dbColType.equals(""))
            type = "string";
        else if (dbColType.equalsIgnoreCase("CLOB"))
            type = "text";
        else if (dbColType.equalsIgnoreCase("NUMBER"))
            type = "integer";
        else if (dbColType.equalsIgnoreCase("DATE"))
            type = "date";
        else if (dbColType.startsWith("TIMESTAMP"))
            type = "datetime";

        return type;
    }
}