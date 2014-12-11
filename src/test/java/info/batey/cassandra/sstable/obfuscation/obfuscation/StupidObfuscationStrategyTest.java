package info.batey.cassandra.sstable.obfuscation.obfuscation;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class StupidObfuscationStrategyTest {
    @Test
    public void addsObfuscated() throws Exception {
        StupidObfuscationStrategy obfuscator = new StupidObfuscationStrategy();

        Object obfuscatedValue = obfuscator.obfuscate("hello");

        assertEquals("hello obfuscated", obfuscatedValue);
    }
}