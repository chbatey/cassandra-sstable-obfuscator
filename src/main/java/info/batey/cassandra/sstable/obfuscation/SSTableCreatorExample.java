package info.batey.cassandra.sstable.obfuscation;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;

import java.io.IOException;

public class SSTableCreatorExample {
    public static void main(String[] args) throws InvalidRequestException, IOException {
        String schema = "CREATE TABLE sstables.simple_key_value2 (\n" +
                "  key text,\n" +
                "  value text,\n" +
                "  PRIMARY KEY (key)\n" +
                ")";

        String insertStatement = "INSERT INTO sstables.simple_key_value2 (key, value ) VALUES ( ?, ? )";

        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                .inDirectory("/Users/chbatey/dev/cassandra-examples/SSTableOffuscation/output/sstables/simple_key_value2")
                .forTable(schema)
                .using(insertStatement).build();

        writer.addRow("chris_obs", "chris_value");
        writer.addRow("chris_obs2", "chris_value2");

        writer.close();

        System.exit(0);
    }
}
