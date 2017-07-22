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
import java.util.Map;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import net.opentsdb.search.ESPluginConfig;

@PowerMockIgnore({ "javax.management.*", "javax.xml.*", "ch.qos.*",
  "org.slf4j.*", "com.sum.*", "org.xml.*" })
@RunWith(PowerMockRunner.class)
//@PrepareForTest ({BulkRequestBuilder.class, UpdateRequest.class, TagKeyAMWriter.class })
public class EsWriterTest {

  BulkRequestBuilder bulkRequest;
  ESPluginConfig metaConf;

//  @BeforeMethod
//  public void mocks() throws Exception {
//    bulkRequest =  PowerMockito.mock(BulkRequestBuilder.class);
//    PowerMockito.whenNew(BulkRequestBuilder.class).withAnyArguments().thenReturn(bulkRequest);
//    metaConf = PowerMockito.mock(MetaSystemConfig.class);
//    PowerMockito.whenNew(MetaSystemConfig.class).withAnyArguments().thenReturn(metaConf);
//    System.getProperties().put("isLocalMode", true);
//    TagKeyAMCache.amCache.clear();
//    TagKeyAMCache.tagkeyCache.clear();
//  }
//
//  @Test
//  public void TestAddNATDoc() {
//    try {
//      String[] metric = {"metric1", "metric2"};
//      Map<String, String> tags = new HashMap<String, String>();
//      tags.put("tagk1", "tagv1");
//      MetaEvent metaEvent = new MetaEvent("somenamespace", "someapp", metric, 1200000000L, 1200000000L, tags, 0, false);
//      long uidByte = UID.generateUID(UID.generateNATKeysfromEvent(metaEvent));
//      String uid = Long.toString(uidByte);
//      HashMap<String, Object> natMap = new HashMap<>();
//
//      natMap.put("application.raw", metaEvent.getApplication());
//      natMap.put("application.lowercase", metaEvent.getApplication().toLowerCase());
//
//      natMap.put("AM_nested", metaEvent.getAMNested());
//
//      natMap.put("tags", metaEvent.getTagsKeysNested());
//
//      natMap.put("timestamp", System.currentTimeMillis());
//      natMap.put("firstSeenTime", metaEvent.getFirstSeenTime()/1000);
//      natMap.put("lastSeenTime", metaEvent.getLastSeenTime()/1000);
//
//      TagKeyAMWriter writer = new TagKeyAMWriter();
//      IndexRequest namtRequest = new IndexRequest(metaEvent.getNamespace().toLowerCase()+"_tagkeys", "namt", uid).source(natMap);
//      UpdateRequest namtUpdateReq = new UpdateRequest(metaEvent.getNamespace().toLowerCase()+"_tagkeys", "namt", uid).doc(natMap).upsert(namtRequest).retryOnConflict(20);
//
//      writer.addNATDoc(metaEvent, bulkRequest);
//      writer.addNATDoc(metaEvent, bulkRequest);
//
//      Mockito.verify(bulkRequest, Mockito.times(1)).add(Mockito.any(UpdateRequest.class));
//      
//      
//    } catch (Exception e) {
//      // TODO Auto-generated catch block
//      e.printStackTrace();
//    }}
//  
//  
//  
//  @Test
//  public void TestAddNAMDoc() {
//    try {
//      String[] metric = {"metric1", "metric2"};
//      Map<String, String> tags = new HashMap<String, String>();
//      tags.put("tagk1", "tagv1");
//      MetaEvent metaEvent = new MetaEvent("somenamespace", "someapp", metric, 1200000000L, 1200000000L, tags, 0, false);
//      long uidByte = UID.generateUID(UID.generateNATKeysfromEvent(metaEvent));
//      String uid = Long.toString(uidByte);
//      HashMap<String, Object> natMap = new HashMap<>();
//
//      natMap.put("application.raw", metaEvent.getApplication());
//      natMap.put("application.lowercase", metaEvent.getApplication().toLowerCase());
//
//      natMap.put("AM_nested", metaEvent.getAMNested());
//
//      natMap.put("tags", metaEvent.getTagsKeysNested());
//
//      natMap.put("timestamp", System.currentTimeMillis());
//      natMap.put("firstSeenTime", metaEvent.getFirstSeenTime()/1000);
//      natMap.put("lastSeenTime", metaEvent.getLastSeenTime()/1000);
//
//      TagKeyAMWriter writer = new TagKeyAMWriter();
//      IndexRequest namtRequest = new IndexRequest(metaEvent.getNamespace().toLowerCase()+"_tagkeys", "namt", uid).source(natMap);
//      UpdateRequest namtUpdateReq = new UpdateRequest(metaEvent.getNamespace().toLowerCase()+"_tagkeys", "namt", uid).doc(natMap).upsert(namtRequest).retryOnConflict(20);
//
//      writer.addNAMDoc(metaEvent, bulkRequest);
//
//      Mockito.verify(bulkRequest, Mockito.times(metric.length)).add(Mockito.any(UpdateRequest.class));
//    } catch (Exception e) {
//      // TODO Auto-generated catch block
//      e.printStackTrace();
//    }}
//  
//  @Test
//  public void TestNamespaceDoc() {
//    String namespace = "somenamespace";
//    ESNamespace.addNamespaceDoc(namespace, bulkRequest);
//    Map<String, Object> namespaceDocMap= new HashMap<String, Object>();
//    namespaceDocMap.put("namespace.raw", namespace);
//    namespaceDocMap.put("namespace", namespace.toLowerCase());
//
//    //namespaceDocMap.put("timestamp", System.currentTimeMillis());
//    String uid = Long.toString(UID.generateUID(namespace));
//    Mockito.verify(bulkRequest, Mockito.times(1)).add(Mockito.any(IndexRequest.class));
//    
//  }
//  
//  @Test
//  public void TestAddNamtDoc() {
//    ElasticsearchWriter esWriter = new ElasticsearchWriter(0, 0, metaConf, 100);
//    String[] metric = {"metric1", "metric2"};
//    Map<String, String> tags = new HashMap<String, String>();
//    tags.put("tagk1", "tagv1");
//    MetaEvent metaEvent = new MetaEvent("somenamespace", "someapp", metric, 1200000000L, 1200000000L, tags, 0, false);
//    
//    esWriter.addNamtDoc(metaEvent, bulkRequest);
//    Mockito.verify(bulkRequest, Mockito.times(metric.length + 1 + 1)).add(Mockito.any(UpdateRequest.class));
//    Mockito.verify(bulkRequest, Mockito.times(1)).add(Mockito.any(IndexRequest.class));
//
//  }
  
  @Test
  public void todo() throws Exception {
    
  }

}
