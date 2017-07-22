// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.search.schemas.tsmeta;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


public class ESNamespace {
  private static final Logger LOG = LoggerFactory.getLogger(ESNamespace.class);

  static  Set<String> namespace = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
 

  public static synchronized void addNamespaceDoc(String namespace, BulkRequestBuilder bulkRequest) {
    Map<String, Object> namespaceDocMap= new HashMap<String, Object>();
    namespaceDocMap.put("namespace.raw", namespace);
    namespaceDocMap.put("namespace", namespace.toLowerCase());

    //namespaceDocMap.put("timestamp", System.currentTimeMillis());
    String uid = Long.toString(UID.generateUID(namespace));
    
    IndexRequest namtRequest = new IndexRequest("all_namespace", "namt", uid).source(namespaceDocMap);
    LOG.info("Adding namespace " + namespace + " to bulk request");
    bulkRequest.add(namtRequest);
  }


}
