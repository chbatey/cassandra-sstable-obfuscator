package info.batey.cassandra.sstable.obfuscation;

import com.google.common.collect.Lists;
import info.batey.cassandra.sstable.obfuscation.obfuscation.CellExtractor;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.BufferCell;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.composites.CompoundSparseCellName;
import org.apache.cassandra.db.composites.CompoundSparseCellNameType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.nio.ByteBuffer;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;

@RunWith(MockitoJUnitRunner.class)
public class CellExtractorTest {

    private CellExtractor underTest;

//    private CFMetaData cfMetaDataNoClusteringColumns = new CFMetaData("ks", "cf", ColumnFamilyType.Standard, new CompoundSparseCellNameType(Collections.emptyList()));
    private CFMetaData cfMetaDataNoClusteringColumns = CFMetaData.compile("CREATE TABLE ks.simple_key_value (key text, value text, PRIMARY KEY (key))", "ks");
    private CFMetaData cfMetaDataClusteringColumns = CFMetaData.compile("create TABLE ks.text_and_clustering(key text, cc1 text , cc2 text, value3 text, primary KEY (key, cc1, cc2) )", "ks");

    @Mock
    private BufferCell row;

    @Mock
    private CompoundSparseCellName cellName;

    @Mock
    private ColumnIdentifier columnName;

    @Before
    public void setUp() throws Exception {
        underTest = new CellExtractor();
    }

    @Test
    public void testCellExtractWithoutClusteringColumns() throws Exception {
        //given
        String rowValue = "row value";
        ByteBuffer rowValueBytes = ByteBuffer.wrap(rowValue.getBytes());
        given(row.name()).willReturn(cellName);
        given(row.value()).willReturn(rowValueBytes);
        given(cellName.cql3ColumnName(cfMetaDataNoClusteringColumns)).willReturn(columnName);

        //when
        CellExtractor.Cell cell = underTest.extractCellFromRow(row, cfMetaDataNoClusteringColumns);

        //then
        assertEquals(rowValue, cell.getValue());
        assertEquals(columnName.toString(), cell.getCqlColumnName());
        assertEquals(Lists.<CellExtractor.ClusteringColumn>newArrayList(), cell.getClusteringColumns());
    }

    @Test
    public void testCellExtractWithClusteringColumns() throws Exception {
        //given
        String rowValue = "row value";
        given(row.name()).willReturn(cellName);
        given(row.value()).willReturn(ByteBuffer.wrap(rowValue.getBytes()));
        given(cellName.cql3ColumnName(cfMetaDataClusteringColumns)).willReturn(columnName);
        given(cellName.clusteringSize()).willReturn(2);

        CellExtractor.ClusteringColumn clusteringColumnOne = new CellExtractor.ClusteringColumn("cc1", "cc 1 value" );
        given(cellName.get(0)).willReturn(ByteBuffer.wrap(clusteringColumnOne.getValue().toString().getBytes()));

        CellExtractor.ClusteringColumn clusteringColumnTwo = new CellExtractor.ClusteringColumn("cc2", "cc 2 value");
        given(cellName.get(1)).willReturn(ByteBuffer.wrap(clusteringColumnTwo.getValue().toString().getBytes()));

        //when
        CellExtractor.Cell cell = underTest.extractCellFromRow(row, cfMetaDataClusteringColumns);

        //then
        assertEquals(rowValue, cell.getValue());
        assertEquals(columnName.toString(), cell.getCqlColumnName());
        assertEquals(Lists.newArrayList(clusteringColumnOne, clusteringColumnTwo), cell.getClusteringColumns());
    }
}