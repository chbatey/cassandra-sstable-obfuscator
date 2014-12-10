package info.batey.cassandra.sstable.obfuscation;

import info.batey.cassandra.sstable.obfuscation.config.SchemaConfig;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;

public class CqlSSTableWriterFactory {
    public CQLSSTableWriter createWriter(String outputDirectory,
                                        SchemaConfig schemaConfig) {
        String outputDirectoryForFiles = String.format("%s/%s/%s",
                outputDirectory,
                schemaConfig.getNewKeySpaceName(),
                schemaConfig.getTableName());

        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                .inDirectory(outputDirectoryForFiles)
                .forTable(schemaConfig.getSchema())
                .using(schemaConfig.getInsertStatement()).build();

        return writer;
    }
}
