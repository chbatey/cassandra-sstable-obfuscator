package info.batey.cassandra.sstable.obfuscation;

import info.batey.cassandra.sstable.obfuscation.config.SchemaConfig;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.CompoundSparseCellNameType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableScanner;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.Pair;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SSTableReaderFactory {
    public CqlTableSSTableReader sstableReader(String inputSSTableDirectory, SchemaConfig schemaConfig) throws IOException {
        File directory = new File(inputSSTableDirectory);
        Descriptor descriptor = new Descriptor(directory,
                schemaConfig.getOriginalKeyspaceName(),
                schemaConfig.getTableName(),
                schemaConfig.getSstableGeneration(),
                Descriptor.Type.FINAL);

        CFMetaData cfMetaData;
        try {
            Pair<CreateTableStatement, List<ColumnSpecification>> schemaObject = getStatement(schemaConfig.getSchema(), CreateTableStatement.class, "CREATE TABLE");
            for (ColumnSpecification columnSpecification : schemaObject.right) {
                System.out.println(columnSpecification);
            }
            cfMetaData = schemaObject.left.getCFMetaData().rebuild();
            System.out.println(cfMetaData);
        } catch (RequestValidationException e) {
            throw new RuntimeException(e);
        }

        ArrayList<AbstractType<?>> types = new ArrayList<AbstractType<?>>();
        List<ColumnDefinition> columnDefinitions = cfMetaData.clusteringColumns();
        for (ColumnDefinition columnDefinition : columnDefinitions) {
            types.add(columnDefinition.type);
        }
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

    private static <T extends CQLStatement> Pair<T, List<ColumnSpecification>> getStatement(String query, Class<T> klass, String type) {
        try {
            ClientState state = ClientState.forInternalCalls();
            ParsedStatement.Prepared prepared = QueryProcessor.getStatement(query, state);
            CQLStatement stmt = prepared.statement;
            stmt.validate(state);

            if (!stmt.getClass().equals(klass))
                throw new IllegalArgumentException("Invalid query, must be a " + type + " statement");

            return Pair.create(klass.cast(stmt), prepared.boundNames);
        } catch (RequestValidationException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
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
