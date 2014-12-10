package info.batey.cassandra.sstable.obfuscation;

import info.batey.cassandra.sstable.obfuscation.config.SchemaConfig;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.CompoundSparseCellNameType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableScanner;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class SSTableReaderFactory {
    public CqlTableSSTableReader sstableReader(String inputSSTableDirectory, SchemaConfig schemaConfig) throws IOException {
        File directory = new File(inputSSTableDirectory);
        Descriptor descriptor = new Descriptor(directory,
                schemaConfig.getOriginalKeyspaceName(),
                schemaConfig.getTableName(),
                schemaConfig.getSstableGeneration(),
                Descriptor.Type.FINAL);

        ArrayList<AbstractType<?>> types = new ArrayList<AbstractType<?>>();
        // the clustering columns
//        types.add(UTF8Type.instance);
//        types.add(TimeUUIDType.instance);

        CellNameType cellNameType = new CompoundSparseCellNameType(types);
        CFMetaData metadata = new CFMetaData(
                descriptor.ksname,
                descriptor.cfname,
                ColumnFamilyType.Standard,
                cellNameType);


        return new CqlTableSSTableReader(SSTableReader.open(descriptor, metadata), metadata);
    }

    public static class CqlTableSSTableReader {
        private final SSTableReader ssTableReader;
        private final CFMetaData cfMetaData;

        public CqlTableSSTableReader(SSTableReader ssTableReader, CFMetaData cfMetaData) {
            this.ssTableReader = ssTableReader;
            this.cfMetaData = cfMetaData;
        }
        public CFMetaData getCfMetaData() {
            return cfMetaData;
        }

        public SSTableScanner getScanner() {
            return ssTableReader.getScanner();
        }
    }

}
