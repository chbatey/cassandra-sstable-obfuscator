package info.batey.cassandra.sstable.obfuscation;

import info.batey.cassandra.sstable.obfuscation.obfuscation.ObfuscationStrategy;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableScanner;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SSTableObfuscator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SSTableObfuscator.class);

    private Map<String, String> columnToObfuscate;
    private CellExtractor cellExtractor;

    public SSTableObfuscator(Map<String, String> columnToObfuscate, CellExtractor cellExtractor) {
        this.columnToObfuscate = columnToObfuscate;
        this.cellExtractor = cellExtractor;
    }

    /**
     * Streams the new SS table to disk.
     */
    public void mapSSTable(CqlTableSSTableReader reader, CQLSSTableWriter writer) throws InvalidRequestException, IOException, ClassNotFoundException, IllegalAccessException, InstantiationException {
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

                OnDiskAtom next = row.next();
                CFMetaData cfMetaData = reader.getCfMetaData();
                CellExtractor.Cell cell = cellExtractor.extractCellFromRow(next, cfMetaData);
                String obfuscationStrategy = columnToObfuscate.get(cell.getCqlColumnName());

                if (cell.getCqlColumnName().equals("")) {
                    LOGGER.debug("New CQL row");
                    // todo save the last cql row
                    if (cqlCols.size() > 1) {
                        writer.addRow(cqlCols.toArray());

                    }
                    cqlCols = new ArrayList<>();
                    cqlCols.add(keyValue);

                    // add the clustering columns
                    List<CellExtractor.ClusteringColumn> clusteringColumns = cell.getClusteringColumns();
                    for (CellExtractor.ClusteringColumn clusteringColumn : clusteringColumns) {
                        cqlCols.add(clusteringColumn.getValue());
                    }
                } else if (obfuscationStrategy != null) {
                    Class<?> obfuscationClass = Class.forName(obfuscationStrategy);
                    ObfuscationStrategy obfuscationStrategyInstance = (ObfuscationStrategy) obfuscationClass.newInstance();
                    Object obfuscatedValue = obfuscationStrategyInstance.obfuscate(cell.getValue());
                    LOGGER.debug("Value to obfuscate: {} to: {}", cell.getValue(), obfuscatedValue);
                    cqlCols.add(obfuscatedValue);
                }  else {
                    LOGGER.debug("Value not obfuscating: {}");
                    cqlCols.add(cell.getValue());
                }

            }
            // save the last cql row
            LOGGER.debug("Adding row: {}", cqlCols);
            writer.addRow(cqlCols.toArray());
        }

        writer.close();
    }
}
