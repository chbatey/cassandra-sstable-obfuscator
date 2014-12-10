package info.batey.cassandra.sstable.obfuscation.config;

public class SchemaConfig {
    private String newKeySpaceName;
    private String originalKeyspaceName;
    private String tableName;
    private String schema;
    private String insertStatement;
    private int sstableGeneration;

    public String getNewKeySpaceName() {
        return newKeySpaceName;
    }

    public String getOriginalKeyspaceName() {
        return originalKeyspaceName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getSchema() {
        return schema;
    }

    public String getInsertStatement() {
        return insertStatement;
    }

    public int getSstableGeneration() {
        return sstableGeneration;
    }
}
