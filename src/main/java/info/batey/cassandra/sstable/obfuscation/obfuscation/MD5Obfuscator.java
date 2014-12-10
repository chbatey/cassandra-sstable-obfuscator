package info.batey.cassandra.sstable.obfuscation.obfuscation;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public class MD5Obfuscator implements ObfuscationStrategy {

    private final MessageDigest md;

    public MD5Obfuscator() throws NoSuchAlgorithmException {
        md = MessageDigest.getInstance("MD5");
    }

    @Override
    public Object obfuscate(Object rawValue) {
        byte[] md5 = md.digest(rawValue.toString().getBytes(Charset.forName("UTF-8")));
        return new String(Base64.getEncoder().encode(md5));
    }
}
