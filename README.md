# SSTable Obfuscator

A tool for taking Raw SS Tables (not snappy compressed) and obfuscating text fields.

Works for Cassandra 2.1 SS tables. I will add support for other versions if requested.

**Very limited functionality so far**

* Can't obfuscate primary key
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

## To use it

* Clone the repo
* Modify the config.yml

Run the main class: info.batey.cassandra.sstable.obfuscation.Main

The config.yml must be in the directory where you run it from.
