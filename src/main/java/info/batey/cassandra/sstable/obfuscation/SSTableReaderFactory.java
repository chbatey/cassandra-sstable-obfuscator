package info.batey.cassandra.sstable.obfuscation;

import info.batey.cassandra.sstable.obfuscation.config.SchemaConfig;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class SSTableReaderFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(SSTableReaderFactory.class);

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
                LOGGER.debug("Column spec: {}", columnSpecification);
            }
            cfMetaData = schemaObject.left.getCFMetaData().rebuild();
            LOGGER.debug("CF meta data: {}", cfMetaData);
        } catch (RequestValidationException e) {
            throw new RuntimeException(e);
        }

        return new CqlTableSSTableReader(SSTableReader.open(descriptor, cfMetaData), cfMetaData);
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

}
