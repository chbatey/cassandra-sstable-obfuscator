package info.batey.cassandra.sstable.obfuscation.obfuscation;

public class StupidObfuscationStrategy implements ObfuscationStrategy {
    @Override
    public Object obfuscate(Object rawValue) {
        return rawValue.toString() + " obfuscated";
    }
}
