package info.batey.cassandra.sstable.obfuscation;

import info.batey.cassandra.sstable.obfuscation.config.SchemaConfig;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class CqlSSTableWriterFactory {

    private final static Logger LOGGER = LoggerFactory.getLogger(CqlSSTableWriterFactory.class);

    public CQLSSTableWriter createWriter(String outputDirectory,
                                        SchemaConfig schemaConfig) {

        String outputDirectoryForFiles = String.format("%s/%s/%s",
                outputDirectory,
                schemaConfig.getNewKeySpaceName(),
                schemaConfig.getTableName());
        try {
            LOGGER.info("Creating directory {}", outputDirectoryForFiles);
            Files.createDirectories(Paths.get(outputDirectoryForFiles));
        } catch (IOException e) {
            throw new RuntimeException("Can't create output directory " + outputDirectory);
        }

        return CQLSSTableWriter.builder()
                .inDirectory(outputDirectoryForFiles)
                .forTable(schemaConfig.getSchema())
                .using(schemaConfig.getInsertStatement()).build();
    }
}
