package info.batey.cassandra.sstable.obfuscation;

import junit.framework.TestCase;

public class MainIntegrationTest extends TestCase {

    public static void main(String[] args) throws Exception {
        Main main = new Main();
//        main.runObfuscation("src/test/resources/config-many-text-cols.yml");
//        main.runObfuscation("src/test/resources/config-key-value.yml");
        main.runObfuscation("src/test/resources/config-clustering-key.yml");
        System.exit(0);
    }
}