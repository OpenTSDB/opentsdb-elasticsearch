OTSDBElasticSearch
==================

Search plugin for OpenTSDB

Quick notes for getting up and running (Temporarily till I get a build setup):

* Download the HttpComponents client version 4.2 or later
* Download https://github.com/mcaprari/httpclient-failover and compile
* Compile OpenTSDB 2.0 to get the jar
* Compile the ES plugin however you like, be sure to include the manifest in the .jar
* Create a plugins directory for your TSD
* Add "tsd.core.plugin_path = <directory>" to your TSD config
* Add "tsd.search.enable = true" to your TSD config
* Add "tsd.search.plugin = net.opentsdb.search.ElasticSearch" 
* Add "tsd.search.elasticsearch.hosts = <host>" to the config where host is single host or a semicoln delimited list of hosts.
