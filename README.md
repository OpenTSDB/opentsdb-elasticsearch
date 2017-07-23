       ___                 _____ ____  ____  ____
      / _ \ _ __   ___ _ _|_   _/ ___||  _ \| __ )
     | | | | '_ \ / _ \ '_ \| | \___ \| | | |  _ \
     | |_| | |_) |  __/ | | | |  ___) | |_| | |_) |
      \___/| .__/ \___|_| |_|_| |____/|____/|____/
           |_|    The modern time series database.


[![Build Status](https://travis-ci.org/OpenTSDB/opentsdb-elasticsearch.svg?branch=master)](https://travis-ci.org/OpenTSDB/opentsdb-elasticsearch) [![Coverage Status](https://coveralls.io/repos/github/OpenTSDB/opentsdb-elasticsearch/badge.svg?branch=master)](https://coveralls.io/github/OpenTSDB/opentsdb-elasticsearch?branch=master)
 
Search plugin for OpenTSDB

##Installation

* Compile the plugin via ``mvn package``.
* Create a plugins directory for your TSD
* Copy the plugin from the ``target`` directory into your TSD's plugin's directory.
* Add the following configs to your ``opentsdb.conf`` file.
    * Add ``tsd.core.plugin_path = <directory>`` pointing to a valid directory for your plugins.
    * Add ``tsd.search.enable = true``
    * Add ``tsd.search.plugin = net.opentsdb.search.ElasticSearch`` 
    * Add ``tsd.search.elasticsearch.host = <host>`` The HTTP protocol, host and port for an ES host or VIP in the format ``http[s]://<host>[:port]``.
* Add a mapping for each JSON file in the ``./schemas`` sub folder of your choice via:
  (NOTE: It's important to do this BEFORE starting a TSD that would index data as you can't modify the mappings for documents that have already been indexed [afaik])

```  
  curl -X PUT -d @schemas/simple/opentsdb_index.json http://<eshost>/opentsdb/
  curl -X PUT -d @schemas/simple/tsmeta_mapping.json http://<eshost>/opentsdb/tsmeta/_mapping
  curl -X PUT -d @schemas/simple/uidmeta_mapping.json http://<eshost>/opentsdb/uidmeta/_mapping
  curl -X PUT -d @schemas/simple/annotation_mapping.json http://<eshost>/opentsdb/annotation/_mapping
```

* Optionally add ``tsd.core.meta.enable_tracking = true`` to your TSD config if it's processing incoming data
* Turn up the TSD OR...
* ... if you have existing data, run the ``uid metasync`` utility from OpenTSDB

## Schemas

TODO - doc em
