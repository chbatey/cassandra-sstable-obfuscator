package info.batey.cassandra.sstable.obfuscation.config;

import java.util.Map;

public class Configuration {

    private SchemaConfig schema;

    private String outputDirectory;
    private String inputDirectory;

    private Map<String, String> columnsToObfuscate;

    public String getOutputDirectory() {
        return outputDirectory;
    }

    public String getInputDirectory() {
        return inputDirectory;
    }

    public Map<String, String> getColumnsToObfuscate() {
        return columnsToObfuscate;
    }

    public SchemaConfig getSchema() {
        return schema;
    }
}
