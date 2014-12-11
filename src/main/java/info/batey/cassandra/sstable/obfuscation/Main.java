package info.batey.cassandra.sstable.obfuscation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import info.batey.cassandra.sstable.obfuscation.config.Configuration;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;

import java.io.File;

public class Main {
    public static void main(String[] args) throws Exception {
        new Main().runObfuscation("config.yml");
        System.exit(0);
    }

    public void runObfuscation(String configFile) throws java.io.IOException, InvalidRequestException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        Configuration configuration = mapper.readValue(new File(configFile), Configuration.class);

        CqlSSTableWriterFactory cqlSSTableWriterFactory = new CqlSSTableWriterFactory();
        SSTableReaderFactory ssTableReaderFactory = new SSTableReaderFactory();

        CQLSSTableWriter writer = cqlSSTableWriterFactory.createWriter(configuration.getOutputDirectory(), configuration.getSchema());
        SSTableReaderFactory.CqlTableSSTableReader reader = ssTableReaderFactory.sstableReader(configuration.getInputDirectory(), configuration.getSchema());

        SSTableObfuscator ssTableMapper = new SSTableObfuscator(configuration.getColumnsToObfuscate());
        ssTableMapper.mapSSTable(reader, writer);

    }
}
