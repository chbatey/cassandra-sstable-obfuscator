package info.batey.cassandra.sstable.obfuscation;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableScanner;

public class CqlTableSSTableReader {
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
