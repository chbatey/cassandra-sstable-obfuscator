package info.batey.cassandra.sstable.obfuscation.obfuscation;

public interface ObfuscationStrategy {
    Object obfuscate(Object rawValue);
}
