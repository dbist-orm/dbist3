package org.dbist.metadata;

/**
 * Table Index 메타 데이터
 *
 * @author shortstop
 */
public class TableIdx {
    /**
     * 테이블 명
     */
    private String tableName;
    /**
     * 테이블 인덱스 명
     */
    private String indexName;
    /**
     * 테이블 Unique Index 여부
     */
    private boolean unique;
    /**
     * 테이블 인덱스 필드 리스트 : ','로 구분되는 필드 리스트
     */
    private String indexFields;

    /**
     * Table Index
     */
    public TableIdx() {
    }

    /**
     * Table Index
     *
     * @param tableName
     * @param indexName
     * @param unique
     * @param indexFields
     */
    public TableIdx(String tableName, String indexName, boolean unique, String indexFields) {
        this.tableName = tableName;
        this.indexName = indexName;
        this.unique = unique;
        this.indexFields = indexFields;
    }

    /**
     * @return the tableName
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * @param tableName the tableName to set
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * @return the indexName
     */
    public String getIndexName() {
        return indexName;
    }

    /**
     * @param indexName the indexName to set
     */
    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    /**
     * @return the unique
     */
    public boolean getUnique() {
        return unique;
    }

    /**
     * @param unique the unique to set
     */
    public void setUnique(boolean unique) {
        this.unique = unique;
    }

    /**
     * @return the indexFields
     */
    public String getIndexFields() {
        return indexFields;
    }

    /**
     * @param indexFields the indexFields to set
     */
    public void setIndexFields(String indexFields) {
        this.indexFields = indexFields;
    }
}
