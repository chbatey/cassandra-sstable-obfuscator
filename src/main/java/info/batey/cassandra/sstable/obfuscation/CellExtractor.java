package info.batey.cassandra.sstable.obfuscation;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.BufferCell;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.composites.CompoundSparseCellName;
import org.apache.cassandra.serializers.UTF8Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class CellExtractor {

    private static final Logger LOGGER = LoggerFactory.getLogger(CellExtractor.class);

    public Cell extractCellFromRow(OnDiskAtom row, CFMetaData cfMetaData) {
        BufferCell column = (BufferCell) row;
        CompoundSparseCellName name = (CompoundSparseCellName) column.name();
        ColumnIdentifier cqlColumnName = name.cql3ColumnName(cfMetaData);
        LOGGER.trace("CQL column name: |{}|", cqlColumnName);
        ByteBuffer value = column.value();
        String valueAsString = UTF8Serializer.instance.deserialize(value);


        int numberOfClusteringColumns = name.clusteringSize();
        List<ClusteringColumn> clusteringColumns = new ArrayList<>();
        for (int i = 0; i < numberOfClusteringColumns; i++) {
            String clusteringColumnValue = UTF8Serializer.instance.deserialize(name.get(i));
            clusteringColumns.add(new ClusteringColumn(null, clusteringColumnValue));
        }

        return new Cell(cqlColumnName.toString(), valueAsString, clusteringColumns);
    }

    public static class Cell {
        private final String cqlColumnName;
        private final Object value;
        private final List<ClusteringColumn> clusteringColumns;

        public Cell(String cqlColumnName, Object value, List<ClusteringColumn> clusteringColumns) {
            this.cqlColumnName = cqlColumnName;
            this.value = value;
            this.clusteringColumns = clusteringColumns;
        }

        public String getCqlColumnName() {
            return cqlColumnName;
        }

        public Object getValue() {
            return value;
        }

        public List<ClusteringColumn> getClusteringColumns() {
            return clusteringColumns;
        }

        @Override
        public String toString() {
            return "Cell{" +
                    "cqlColumnName='" + cqlColumnName + '\'' +
                    ", value=" + value +
                    ", clusteringColumns=" + clusteringColumns +
                    '}';
        }
    }

    public static class ClusteringColumn {
        private final String name;
        private final Object value;

        public ClusteringColumn(String name, Object value) {
            this.name = name;
            this.value = value;
        }

        public String getName() {
            return name;
        }

        public Object getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "ClusteringColumn{" +
                    "name='" + name + '\'' +
                    ", value=" + value +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ClusteringColumn that = (ClusteringColumn) o;

            if (name != null ? !name.equals(that.name) : that.name != null) return false;
            if (value != null ? !value.equals(that.value) : that.value != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + (value != null ? value.hashCode() : 0);
            return result;
        }
    }

}
