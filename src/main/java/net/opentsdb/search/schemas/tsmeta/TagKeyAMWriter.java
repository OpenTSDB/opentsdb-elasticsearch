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

import java.util.HashMap;
import javax.xml.bind.DatatypeConverter;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TagKeyAMWriter {
  private static final Logger LOG = LoggerFactory.getLogger(TagKeyAMWriter.class);

  public  void addNATDoc(MetaEvent metaEvent, BulkRequestBuilder bulkRequest) {
    long uidByte = UID.generateUID(UID.generateNATKeysfromEvent(metaEvent));
    if (! TagKeyAMCache.tagkeyCache.contains(uidByte) || metaEvent.isRefresh()) {

      HashMap<String, Object> natMap = new HashMap<String, Object>();
      
      natMap.put("application.raw", metaEvent.getApplication());
      natMap.put("application.lowercase", metaEvent.getApplication().toLowerCase());

      natMap.put("AM_nested", metaEvent.getAMNested());

      natMap.put("tags", metaEvent.getTagsKeysNested());

      natMap.put("timestamp", System.currentTimeMillis());
      natMap.put("firstSeenTime", metaEvent.getFirstSeenTime()/1000);
      natMap.put("lastSeenTime", metaEvent.getLastSeenTime()/1000);

      TagKeyAMCache.tagkeyCache.add(uidByte);
      String uid = Long.toString(uidByte);
     
      IndexRequest namtRequest = new IndexRequest(metaEvent.getNamespace().toLowerCase()+"_tagkeys", "namt", uid).source(natMap);
      UpdateRequest namtUpdateReq = new UpdateRequest(metaEvent.getNamespace().toLowerCase()+"_tagkeys", "namt", uid).doc(natMap).upsert(namtRequest).retryOnConflict(20);
      bulkRequest.add(namtUpdateReq);
      
    }
  }

  public  void addNAMDoc(MetaEvent metaEvent, BulkRequestBuilder bulkRequest) {
    for (int i = 0; i< metaEvent.getMetric().length; i++) {
      long uidByte = UID.generateUID(UID.generateNAMfromEvent(metaEvent, metaEvent.getMetric()[i]));

      if (! TagKeyAMCache.amCache.contains(uidByte) || metaEvent.isRefresh()) {
        HashMap<String, Object> natMap = new HashMap<String, Object>();

        natMap.put("application.raw", metaEvent.getApplication());
        natMap.put("application.lowercase", metaEvent.getApplication().toLowerCase());

        natMap.put("AM_nested", metaEvent.getAMNested(metaEvent.getMetric()[i]));

        natMap.put("timestamp", System.currentTimeMillis());
        natMap.put("firstSeenTime", metaEvent.getFirstSeenTime()/1000);
        natMap.put("lastSeenTime", metaEvent.getLastSeenTime()/1000);


        TagKeyAMCache.amCache.add(uidByte);
        String uid = Long.toString(uidByte);
        
        IndexRequest namtRequest = new IndexRequest(metaEvent.getNamespace().toLowerCase()+"_am", "namt", uid).source(natMap);
        UpdateRequest namtUpdateReq = new UpdateRequest(metaEvent.getNamespace().toLowerCase()+"_am", "namt", uid).doc(natMap)
            .upsert(namtRequest).retryOnConflict(20);
        bulkRequest.add(namtUpdateReq);
      }
    }
  }
}