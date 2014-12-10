# SSTable Obfuscator

A tool for taking Raw SS Tables (not snappy compressed) and obfuscating text fields.

**Very limited functionality so far**

* Only works for tables without clustering columns
* All fields must be text

Example configuration:

```
schema:
  newKeySpaceName: sstables_obfuscated
  originalKeyspaceName: sstables
  tableName: simple_key_value
  schema: CREATE TABLE sstables_obfuscated.simple_key_value (key text, value text, PRIMARY KEY (key))
  insertStatement: INSERT INTO sstables_obfuscated.simple_key_value (key, value ) VALUES ( ?, ? )
  sstableGeneration: 7

outputDirectory: /Users/chbatey/dev/cassandra-examples/SSTableOffuscation/output/
inputDirectory: /Users/chbatey/.ccm/local/node1/data/sstables/simple_key_value-47f78e70805111e4a6bc2de2b6ea4379/snapshots/1418209941152

columnsToObfuscate:
  value: info.batey.cassandra.sstable.obfuscation.obfuscation.MD5Obfuscator

```

Possible values for the Obfuscation class are:

* info.batey.cassandra.sstable.obfuscation.obfuscation.MD5Obfuscator
* info.batey.cassandra.sstable.obfuscation.obfuscation.StupidObfuscationStrategy

You can implement your own by implementing the ObfuscationStrategy

Once you have generated your SS tables you can import them to a new cluster using SS table loader.