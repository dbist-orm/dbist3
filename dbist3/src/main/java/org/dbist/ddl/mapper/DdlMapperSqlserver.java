package org.dbist.ddl.mapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import org.apache.commons.lang.StringUtils;
import org.dbist.metadata.TableIdx;
import org.springframework.jdbc.core.RowMapper;

import net.sf.common.util.ValueUtils;

/**
 * Sqlserver용 DDL Mapper
 *
 * @author shortstop
 */
public class DdlMapperSqlserver extends DdlMapperAbstract implements DdlMapper {

    /**
     * Table Index RowMapper
     */
    private static final RowMapper<TableIdx> TABLE_INDEX_ROWMAPPER = new TableIndexRowMapper();

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
        joiner.add("	MAXVALUE 9223372036854775807");
        joiner.add("#end");
        return joiner.toString();
    }

    @Override
    public String tableTemplate() {
        StringJoiner joiner = new StringJoiner("\n");
        joiner.add("CREATE TABLE $tableName");
        joiner.add("(");
        joiner.add("#foreach( $column in $columns )");
        joiner.add("#if($column.sequenceAble)");
        joiner.add("$column.name $column.col_type $column.nullable DEFAULT NEXT VALUE FOR $sequenceName,"); // Id bigint NOT NULL DEFAULT NEXT VALUE FOR {SEQUENCE_NAME},
        joiner.add("#else");
        joiner.add("$column.name $column.col_type $column.nullable,");
        joiner.add("#end");
        joiner.add("#end");
        joiner.add("PRIMARY KEY ($primaryKeys)");
        joiner.add(");");
        return joiner.toString();
    }

    @Override
    public String alterTableTemplate() {
        StringJoiner joiner = new StringJoiner("\n");
        joiner.add(this.removeColumnTemplate());
        joiner.add(this.addColumnTemplate());
        joiner.add(this.modifyColumnTemplate());
        joiner.add(this.modifyNullableColumnTemplate());
        // joiner.add(this.renameColumnTemplate());
        // joiner.add(this.modifyDefaultColumnTemplate());
        return joiner.toString();
    }

    @Override
    public String indexTemplate() {
        StringJoiner joiner = new StringJoiner("\n");
        joiner.add("#foreach( $index in $indexes )");
        joiner.add("#if($index.unique)");
        joiner.add("CREATE UNIQUE INDEX $index.name");
        joiner.add("ON $tableName");
        joiner.add("($index.columnList);");
        joiner.add("#else");
        joiner.add("CREATE INDEX $index.name");
        joiner.add("ON $tableName");
        joiner.add("($index.columnList);");
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

        if (type.equalsIgnoreCase("nvarchar")) {
            int size = (length == null || length < 1) ? 255 : length;
            type += "(" + size + ")";
        }

        return type;
    }

    @Override
    public String toDatabaseType(String fieldType) {
        String type = "nvarchar";

        if (fieldType == null || fieldType.equals(""))
            type = "nvarchar";
        else if (fieldType.equalsIgnoreCase("text"))
            type = "ntext";
        else if (fieldType.equalsIgnoreCase("boolean"))
            type = "bit";
        else if (fieldType.equalsIgnoreCase("integer") || fieldType.equalsIgnoreCase("int"))
            type = "int";
        else if (fieldType.equalsIgnoreCase("long"))
            type = "bigint";
        else if (fieldType.equalsIgnoreCase("double"))
            type = "float";
        else if (fieldType.equalsIgnoreCase("float"))
            type = "real";
        else if (fieldType.equalsIgnoreCase("decimal"))
            type = "decimal";
        else if (fieldType.equalsIgnoreCase("date"))
            type = "date";
        else if (fieldType.equalsIgnoreCase("datetime") || fieldType.equalsIgnoreCase("timestamp"))
            type = "datetime";

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
        joiner.add("DROP INDEX $index ON $tableName");
        joiner.add("\n");
        joiner.add("#end");
        return joiner.toString();
    }

    @Override
    public String dropTableTemplate() {
        StringJoiner joiner = new StringJoiner("\n");
        joiner.add("DROP TABLE $tableName;");
        return joiner.toString();
    }

    @Override
    public String getTableCheckSql() {
        return "SELECT 1 AS A FROM information_schema.tables WHERE table_schema = '$domain' AND table_name = '$tableName'";
    }

    @Override
    public String getUserTables() {
        return "SELECT table_name FROM information_schema.tables WHERE table_schema = '$domain'";
    }

    @Override
    public String addColumnTemplate() {
        StringJoiner joiner = new StringJoiner("\n");
        joiner.add("#foreach( $column in $addColumns )");
        joiner.add("	ALTER TABLE $tableName ADD $column.name $column.type;\n");

        joiner.add("	#if($column.nullable && $column.nullable == 'NO')");
        joiner.add("		ALTER TABLE $tableName ALTER COLUMN $column.name NOT NULL;\n");
        joiner.add("	#end");

        // joiner.add(" #ifnotnull($column.defaultValue)");
        // joiner.add(" ALTER TABLE public.$tableName ALTER COLUMN $column.name SET DEFAULT $column.defaultValue;\n");
        // joiner.add(" #end");

        joiner.add("#end");
        return joiner.toString();
    }

    @Override
    public String removeColumnTemplate() {
        StringJoiner joiner = new StringJoiner("\n");
        joiner.add("#foreach( $column in $removeColumns )");
        joiner.add("	ALTER TABLE $tableName DROP COLUMN $column.name CASCADE;\n");
        joiner.add("#end");
        return joiner.toString();
    }

    @Override
    public String modifyColumnTemplate() {
        StringJoiner joiner = new StringJoiner("\n");
        joiner.add("#foreach( $column in $modifyColumns )");
        joiner.add("	ALTER TABLE $tableName ALTER COLUMN $column.name $column.type;\n");
        joiner.add("#end");
        return joiner.toString();
    }

    @Override
    public String renameColumnTemplate() {
        StringJoiner joiner = new StringJoiner("\n");
        joiner.add("#foreach( $column in $renameColumns )");
        // joiner.add(" ALTER TABLE $tableName RENAME COLUMN $column.nameFrom TO $column.nameTo;\n");
        joiner.add("	EXEC SP_RENAME '$tableName.[$column.nameFrom]', '$column.nameTo', 'COLUMN';\n");
        joiner.add("#end");
        return joiner.toString();
    }

    @Override
    public String modifyNullableColumnTemplate() {
        StringJoiner joiner = new StringJoiner("\n");
        joiner.add("#foreach( $column in $nullableColumns )");
        joiner.add("	#if($column.nullable && $column.nullable == 'YES')");
        joiner.add("		ALTER TABLE $tableName ALTER COLUMN $column.name $column.type NULL;\n");
        joiner.add("	#end");
        joiner.add("	#if($column.nullable && $column.nullable == 'NO')");
        joiner.add("		ALTER TABLE $tableName ALTER COLUMN $column.name $column.type NOT NULL;\n");
        joiner.add("	#end");
        joiner.add("#end");
        return joiner.toString();
    }

    @Override
    public String modifyDefaultColumnTemplate() {
        StringJoiner joiner = new StringJoiner("\n");
        joiner.add("#foreach( $column in $defaultColumns )");
        joiner.add("#if(!$column.defaultValue)");
        joiner.add("	ALTER TABLE $tableName ALTER COLUMN $column.name DROP DEFAULT;\n");
        joiner.add("#else");
        joiner.add("	ALTER TABLE $tableName ALTER COLUMN $column.name SET DEFAULT $column.defaultValue;\n");
        joiner.add("#end");
        joiner.add("#end");
        return joiner.toString();
    }

    @Override
    public String getTableIndexSql(String domainName, String tableName) {
        return "EXEC SP_HELPINDEX " + tableName;
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
            tableIndex.setIndexName(rs.getString("index_name"));
            String idxFields = rs.getString("index_keys");
            idxFields = StringUtils.remove(idxFields, " ");
            tableIndex.setIndexFields(idxFields);
            String idxDesc = rs.getString("index_description");
            boolean unique = StringUtils.contains(idxDesc, ", unique");
            tableIndex.setUnique(unique);

            if (StringUtils.contains(idxDesc, ", primary key")) {
                tableIndex.setIndexName("__pk__" + tableIndex.getIndexName());
            }

            return tableIndex;
        }
    }

    @Override
    public List<TableIdx> refineIndexList(List<TableIdx> indexes) {
        List<TableIdx> newIndexes = new ArrayList<TableIdx>();

        for (TableIdx currentIdx : indexes) {
            if (currentIdx.getIndexName().toLowerCase().startsWith("__pk__")) {
                continue;
            }

            newIndexes.add(currentIdx);
        }

        return newIndexes;
    }

    @Override
    public String toJavaType(String dbColType) {
        String type = "string";

        if (dbColType == null || dbColType.equals(""))
            type = "string";
        else if (dbColType.equalsIgnoreCase("text"))
            type = "text";
        else if (dbColType.equalsIgnoreCase("bit"))
            type = "boolean";
        else if (dbColType.equalsIgnoreCase("integer"))
            type = "integer";
        else if (dbColType.equalsIgnoreCase("bigint"))
            type = "long";
        else if (dbColType.equalsIgnoreCase("float"))
            type = "double";
        else if (dbColType.equalsIgnoreCase("real"))
            type = "float";
        else if (dbColType.equalsIgnoreCase("decimal"))
            type = "decimal";
        else if (dbColType.equalsIgnoreCase("date"))
            type = "date";
        else if (dbColType.startsWith("timestamp"))
            type = "datetime";

        return type;
    }

    @Override
    public void setTableSpace(boolean useDataTBSpace, boolean useIdxTBSpace) {
        // TODO Auto-generated method stub

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