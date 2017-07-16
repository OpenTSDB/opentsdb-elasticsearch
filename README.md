OTSDBElasticSearch
==================

Search plugin for OpenTSDB

Quick notes for getting up and running (Temporarily till I get a build setup):

* Download the HttpComponents client version 4.2 or later
* Download https://github.com/mcaprari/httpclient-failover and compile
* Compile OpenTSDB 2.0 (the "next" branch from Github) to get the jar
* Compile the ES plugin however you like, be sure to include the manifest in the .jar. The included POM.xml *should* work
* Create a plugins directory for your TSD
* Add "tsd.core.plugin_path = <directory>" to your TSD config
* Add "tsd.search.enable = true" to your TSD config
* Add "tsd.search.plugin = net.opentsdb.search.ElasticSearch" 
* Add "tsd.search.elasticsearch.host = <host>" The HTTP protocol, host and port for an ES host or VIP in the format ``http[s]://<host>[:port]``.
* Add a mapping for each JSON file in the ./scripts folder via:
  (NOTE: It's important to do this BEFORE starting a TSD that would index data as you can't modify the mappings for documents that have already been indexed [afaik])

```  
  curl -X PUT -d @scripts/opentsdb_index.json http://<eshost>/opentsdb/
  curl -X PUT -d @scripts/tsmeta_mapping.json http://<eshost>/opentsdb/tsmeta/_mapping
  curl -X PUT -d @scripts/uidmeta_mapping.json http://<eshost>/opentsdb/uidmeta/_mapping
  curl -X PUT -d @scripts/annotation_mapping.json http://<eshost>/opentsdb/annotation/_mapping
```

* Optionally add "tsd.core.meta.enable_tracking = true" to your TSD config if it's processing incoming data
* Turn up the TSD OR...
* ... if you have existing data, run the "uid metasync" utility from OpenTSDB
