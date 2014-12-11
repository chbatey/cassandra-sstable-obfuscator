package info.batey.cassandra.sstable.obfuscation.obfuscation;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MD5ObfuscatorTest {
    @Test
    public void mdsAndBase64Encodes() throws Exception {
        MD5Obfuscator obfuscator = new MD5Obfuscator();

        Object obfuscatedValue = obfuscator.obfuscate("hello");

        assertEquals("XUFAKrxLKna5cZ2REBfFkg==", obfuscatedValue);
    }
}