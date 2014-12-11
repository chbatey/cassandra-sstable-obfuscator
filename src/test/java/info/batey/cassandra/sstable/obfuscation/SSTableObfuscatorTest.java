package info.batey.cassandra.sstable.obfuscation;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.composites.CellNameType;
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
    }

    @Test
    public void obfuscateNonPrimaryKeyColumn() throws Exception {
        //given
        Map<String, String> obfuscation = new HashMap<>();
        obfuscation.put("non-primary-key-col", "info.batey.cassandra.sstable.obfuscation.obfuscation.StupidObfuscationStrategy");
        underTest = new SSTableObfuscator(obfuscation, cellExtractor);
        given(sstableScanner.hasNext()).willReturn(true, false);
        mockRowKeyValue(rowKey, "RowKeyOne");
        given(row.hasNext()).willReturn(true, false);
        CellExtractor.Cell myCell = new CellExtractor.Cell("col name", "col value", Collections.emptyList());
        given(cellExtractor.extractCellFromRow(cell, cfMetaData)).willReturn(myCell);

        //when
        underTest.mapSSTable(reader, writer);

        //then
        verify(writer).addRow("RowKeyOne", "col value");

    }

    private void mockRowKeyValue(DecoratedKey rowKey, String value) {
        ByteBuffer bytes = ByteBuffer.wrap(value.getBytes());
        given(rowKey.getKey()).willReturn(bytes);
    }


}