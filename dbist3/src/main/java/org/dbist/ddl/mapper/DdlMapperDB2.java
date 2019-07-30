package org.dbist.ddl.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import org.dbist.metadata.TableIdx;
import org.springframework.jdbc.core.RowMapper;

import net.sf.common.util.ValueUtils;

/**
 * Oracle용 DDL Mapper
 *
 * @author shortstop
 */
public class DdlMapperDB2 extends DdlMapperAbstract implements DdlMapper {

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
        joiner.add("	NO MAXVALUE");
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

        if (useIdxTBSpace) joiner.add("using index tablespace $idxTableSpaceName");

        if (useDataTBSpace) joiner.add(") tablespace $dataTableSpaceName ;");
        else joiner.add(");");


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
        if (useIdxTBSpace) joiner.add(" tablespace $idxTableSpaceName");
        joiner.add(";");
        joiner.add("#else");
        joiner.add("CREATE INDEX $index.name");
        joiner.add("ON $tableName");
        joiner.add("($index.columnList) ");
        if (useIdxTBSpace) joiner.add(" tablespace $idxTableSpaceName");
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
        String type = this.toDatabaseType(fieldType);

        if (fieldType.equalsIgnoreCase("decimal")) {
            type += "(15,3)";
        } else if (fieldType.equalsIgnoreCase("boolean")) {
            // type += "(1)";
        }

        if (type.equalsIgnoreCase("VARCHAR")) {
            int size = (length == null || length < 1) ? 255 : length;
            type += "(" + size + ")";
        }

        return type;
    }

    @Override
    public String toDatabaseType(String fieldType) {
        String type = "VARCHAR";

        if (fieldType == null || fieldType.equals(""))
            type = "VARCHAR";
        else if (fieldType.equalsIgnoreCase("text"))
            type = "CLOB";
        else if (fieldType.equalsIgnoreCase("boolean"))
            type = "INTEGER";
        else if (fieldType.equalsIgnoreCase("integer") || fieldType.equalsIgnoreCase("int"))
            type = "INTEGER";
        else if (fieldType.equalsIgnoreCase("long"))
            type = "NUMERIC";
        else if (fieldType.equalsIgnoreCase("double") || fieldType.equalsIgnoreCase("float"))
            type = "DOUBLE";
        else if (fieldType.equalsIgnoreCase("decimal"))
            type = "DECIMAL";
        else if (fieldType.equalsIgnoreCase("date"))
            type = "DATE";
        else if (fieldType.equalsIgnoreCase("datetime") || fieldType.equalsIgnoreCase("timestamp"))
            type = "TIMESTAMP(6)";

        return type;
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
        StringBuffer sql = new StringBuffer();
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
                Boolean uniq = uniqueness.equalsIgnoreCase("UNIQUE");
                tableIndex.setUnique(uniq);
            }

            return tableIndex;
        }
    }

    @Override
    public List<TableIdx> refineIndexList(List<TableIdx> indexes) {
        List<TableIdx> newIndexes = new ArrayList<TableIdx>();

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
        else if (dbColType.equalsIgnoreCase("INTEGER") || dbColType.equalsIgnoreCase("INT"))
            type = "integer";
        else if (dbColType.equalsIgnoreCase("BOOLEAN"))
            type = "integer";
        else if (dbColType.equalsIgnoreCase("NUMERIC"))
            type = "long";
        else if (dbColType.equalsIgnoreCase("DOUBLE"))
            type = "double";
        else if (dbColType.equalsIgnoreCase("DECIMAL"))
            type = "decimal";
        else if (dbColType.equalsIgnoreCase("DATE"))
            type = "date";
        else if (dbColType.startsWith("TIMESTAMP"))
            type = "datetime";

        return type;
    }

    @Override
    public String dropProcedureTemplate(String name) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getPprocedureDefTemplate() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getCreatePprocedureTemplate() {
        // TODO Auto-generated method stub
        return null;
    }
}