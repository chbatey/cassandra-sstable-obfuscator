package info.batey.cassandra.sstable.obfuscation;

import junit.framework.TestCase;

public class MainIntegrationTest extends TestCase {

    public static void main(String[] args) throws Exception {
        new Main().runObfuscation("src/test/resources/config-many-text-cols.yml");
    }
}