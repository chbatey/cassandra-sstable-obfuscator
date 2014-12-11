package info.batey.cassandra.sstable.obfuscation;

import com.google.common.collect.Lists;
import info.batey.cassandra.sstable.obfuscation.obfuscation.CellExtractor;
import info.batey.cassandra.sstable.obfuscation.obfuscation.SSTableObfuscator;
import info.batey.cassandra.sstable.obfuscation.reader.CqlTableSSTableReader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.composites.CompoundSparseCellNameType;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableScanner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class SSTableObfuscatorTest {

    private SSTableObfuscator underTest;

    @Mock
    private CqlTableSSTableReader reader;

    @Mock
    private CQLSSTableWriter writer;

    @Mock
    private SSTableScanner sstableScanner;

    @Mock
    private SSTableIdentityIterator row;

    @Mock
    private DecoratedKey rowKey;

    @Mock
    private CellExtractor cellExtractor;

    private CFMetaData cfMetaData = new CFMetaData("ks", "cf", ColumnFamilyType.Standard, new CompoundSparseCellNameType(Collections.emptyList()));

    @Mock
    private OnDiskAtom cell;


    @Before
    public void setUp() throws Exception {
        given(reader.getScanner()).willReturn(sstableScanner);
        given(sstableScanner.next()).willReturn(row);
        given(row.getKey()).willReturn(rowKey);
        given(row.next()).willReturn(cell);
        given(reader.getCfMetaData()).willReturn(cfMetaData);
        // default to one row and one column
        given(sstableScanner.hasNext()).willReturn(true, false);
        given(row.hasNext()).willReturn(true, false);
    }

    @Test
    public void testAddsClusteringColumns() throws Exception {
        //given
        Map<String, String> obfuscation = new HashMap<>();
        underTest = new SSTableObfuscator(obfuscation, cellExtractor);
        mockRowKeyValue(rowKey, "RowKeyOne");
        String clusteringKeyValue = "cc 1 value";
        CellExtractor.ClusteringColumn clusteringKey = new CellExtractor.ClusteringColumn("clustering key", clusteringKeyValue);
        CellExtractor.Cell myCell = new CellExtractor.Cell("", "", Lists.newArrayList(clusteringKey));
        given(cellExtractor.extractCellFromRow(cell, cfMetaData)).willReturn(myCell);

        //when
        underTest.mapSSTable(reader, writer);

        //then
        verify(writer).addRow("RowKeyOne", clusteringKeyValue);
    }

    @Test
    public void obfuscateNonPrimaryKeyColumn() throws Exception {
        //given
        Map<String, String> obfuscation = new HashMap<>();
        obfuscation.put("non-primary-key-col", "info.batey.cassandra.sstable.obfuscation.obfuscation.StupidObfuscationStrategy");
        underTest = new SSTableObfuscator(obfuscation, cellExtractor);
        mockRowKeyValue(rowKey, "RowKeyOne");
        CellExtractor.Cell myCell = new CellExtractor.Cell("non-primary-key-col", "col value", Collections.emptyList());
        given(cellExtractor.extractCellFromRow(cell, cfMetaData)).willReturn(myCell);

        //when
        underTest.mapSSTable(reader, writer);

        //then
        verify(writer).addRow("RowKeyOne", "col value obfuscated");
    }

    @Test
    public void obfuscateClusteringKey() throws Exception {
        //given
        Map<String, String> obfuscation = new HashMap<>();
        obfuscation.put("clustering key", "info.batey.cassandra.sstable.obfuscation.obfuscation.StupidObfuscationStrategy");
        underTest = new SSTableObfuscator(obfuscation, cellExtractor);
        mockRowKeyValue(rowKey, "RowKeyOne");
        String clusteringKeyValue = "cc 1 value";
        CellExtractor.ClusteringColumn clusteringKey = new CellExtractor.ClusteringColumn("clustering key", clusteringKeyValue);
        CellExtractor.Cell myCell = new CellExtractor.Cell("", "", Lists.newArrayList(clusteringKey));
        given(cellExtractor.extractCellFromRow(cell, cfMetaData)).willReturn(myCell);

        //when
        underTest.mapSSTable(reader, writer);

        //then
        verify(writer).addRow("RowKeyOne", clusteringKeyValue + " obfuscated");
    }

    private void mockRowKeyValue(DecoratedKey rowKey, String value) {
        ByteBuffer bytes = ByteBuffer.wrap(value.getBytes());
        given(rowKey.getKey()).willReturn(bytes);
    }


}