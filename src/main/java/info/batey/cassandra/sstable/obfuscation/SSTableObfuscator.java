package info.batey.cassandra.sstable.obfuscation;

import info.batey.cassandra.sstable.obfuscation.obfuscation.ObfuscationStrategy;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.BufferCell;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.composites.CompoundSparseCellName;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableScanner;
import org.apache.cassandra.serializers.UTF8Serializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SSTableObfuscator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SSTableObfuscator.class);

    private Map<String, String> columnToObfuscate;

    public SSTableObfuscator(Map<String, String> columnToObfuscate) {
        this.columnToObfuscate = columnToObfuscate;
    }

    /**
     * Streams the new SS table to disk.
     */
    public void mapSSTable(SSTableReaderFactory.CqlTableSSTableReader reader, CQLSSTableWriter writer) throws InvalidRequestException, IOException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        SSTableScanner scanner = reader.getScanner();
        int rowCount = 0;

        // loop through the entire ss table
        while (scanner.hasNext()) {
            SSTableIdentityIterator row = (SSTableIdentityIterator) scanner.next();
            DecoratedKey key = row.getKey();
            String keyValue = new String(ByteBufferUtil.getArray(key.getKey()));
            LOGGER.debug("Key: " + key + " row " + rowCount++ + " key value " + keyValue);
            List<Object> cqlCols = new ArrayList<>();
            cqlCols.add(keyValue);

            // loop through a single storage row
            while (row.hasNext()) {

                BufferCell col = (BufferCell) row.next();
                CompoundSparseCellName name = (CompoundSparseCellName) col.name();
                ColumnIdentifier cqlColumnName = name.cql3ColumnName(reader.getCfMetaData());
                LOGGER.trace("Cqlcol name: |{}|", cqlColumnName);
                ByteBuffer value = col.value();
                String valueAsString = UTF8Serializer.instance.deserialize(value);
                String obfuscationStrategy = columnToObfuscate.get(cqlColumnName.toString());

                if (cqlColumnName.toString().equals("")) {
                    LOGGER.debug("New CQL row");
                    // todo save the last cql row
                    if (cqlCols.size() > 1) {
                        writer.addRow(cqlCols.toArray());

                    }
                    cqlCols = new ArrayList<>();
                    cqlCols.add(keyValue);

                    // add the clustering columns
                    int numberOfClusteringColumns = name.clusteringSize();
                    for (int i = 0; i < numberOfClusteringColumns; i++) {
                        String clusteringColumnValue = UTF8Serializer.instance.deserialize(name.get(i));
                        cqlCols.add(clusteringColumnValue);
                    }
                } else if (obfuscationStrategy != null) {
                    Class<?> obfuscationClass = Class.forName(obfuscationStrategy);
                    ObfuscationStrategy obfuscationStrategyInstance = (ObfuscationStrategy) obfuscationClass.newInstance();
                    Object obfuscatedValue = obfuscationStrategyInstance.obfuscate(valueAsString);
                    LOGGER.debug("Value to obfuscate: {} to: {}", valueAsString, obfuscatedValue);
                    cqlCols.add(obfuscatedValue);
                }  else {
                    LOGGER.debug("Value not obfuscating: {}");
                    cqlCols.add(valueAsString);
                }

            }
            // save the last cql row
            System.out.println("Adding row: " + cqlCols);
            writer.addRow(cqlCols.toArray());
        }

        writer.close();
    }
}
