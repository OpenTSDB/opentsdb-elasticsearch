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
package net.opentsdb.search;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.search.schemas.annotation.AnnotationSchema;
import net.opentsdb.search.schemas.annotation.DefaultAnnotationSchema;
import net.opentsdb.search.schemas.tsmeta.DefaultTSMetaSchema;
import net.opentsdb.search.schemas.tsmeta.TSMetaSchema;
import net.opentsdb.search.schemas.uidmeta.DefaultUIDMetaSchema;
import net.opentsdb.search.schemas.uidmeta.UIDMetaSchema;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.Config;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ TSDB.class, ElasticSearch.class, HttpAsyncClients.class,
  PoolingNHttpClientConnectionManager.class })
public class TestElasticSearch {

  private TSDB tsdb;
  private Config config;
  private PoolingNHttpClientConnectionManager connection_manager;
  private HttpAsyncClientBuilder client_builder;
  private CloseableHttpAsyncClient client;
  private TSMetaSchema ts_meta_schema;
  private UIDMetaSchema uid_meta_schema;
  private AnnotationSchema annotation_schema;
  
  @Before
  public void before() throws Exception {
    tsdb = PowerMockito.mock(TSDB.class);
    config = new Config(false);
    connection_manager = mock(PoolingNHttpClientConnectionManager.class);
    client_builder = mock(HttpAsyncClientBuilder.class);
    client = mock(CloseableHttpAsyncClient.class);
    ts_meta_schema = mock(TSMetaSchema.class);
    uid_meta_schema = mock(UIDMetaSchema.class);
    annotation_schema = mock(AnnotationSchema.class);
    
    config.overrideConfig("tsd.search.elasticsearch.host", "localhost:9200");
    
    when(tsdb.getConfig()).thenReturn(config);
    
    PowerMockito.mockStatic(HttpAsyncClients.class);
    when(HttpAsyncClients.custom()).thenReturn(client_builder);
    
    PowerMockito.whenNew(PoolingNHttpClientConnectionManager.class)
      .withAnyArguments().thenReturn(connection_manager);
    when(client_builder.build()).thenReturn(client);
  }
  
  @Test
  public void initialize() throws Exception {
    final ElasticSearch plugin = new ElasticSearch();
    plugin.initialize(tsdb);
    
    verify(client, times(1)).start();
    assertEquals("http://localhost:9200", plugin.host());
    verify(connection_manager, times(1)).setMaxTotal(
        plugin.config().getInt("tsd.search.elasticsearch.pool.max_total"));
    verify(connection_manager, times(1)).setDefaultMaxPerRoute(
        plugin.config().getInt("tsd.search.elasticsearch.pool.max_per_route"));
    assertTrue(plugin.tsMetaSchema() instanceof DefaultTSMetaSchema);
    assertTrue(plugin.uidMetaSchema() instanceof DefaultUIDMetaSchema);
    assertTrue(plugin.annotationSchema() instanceof DefaultAnnotationSchema);
  }
  
  @Test
  public void initializeDisabledIndices() throws Exception {
    config.overrideConfig("tsd.search.elasticsearch.schema.tsmeta", null);
    config.overrideConfig("tsd.search.elasticsearch.schema.uidmeta", null);
    config.overrideConfig("tsd.search.elasticsearch.schema.annotation", null);
    final ElasticSearch plugin = new ElasticSearch();
    plugin.initialize(tsdb);
    
    verify(client, times(1)).start();
    assertEquals("http://localhost:9200", plugin.host());
    verify(connection_manager, times(1)).setMaxTotal(
        plugin.config().getInt("tsd.search.elasticsearch.pool.max_total"));
    verify(connection_manager, times(1)).setDefaultMaxPerRoute(
        plugin.config().getInt("tsd.search.elasticsearch.pool.max_per_route"));
    assertNull(plugin.tsMetaSchema());
    assertNull(plugin.uidMetaSchema());
    assertNull(plugin.annotationSchema());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void initializeNullHost() throws Exception {
    config.overrideConfig("tsd.search.elasticsearch.host", null);
    final ElasticSearch plugin = new ElasticSearch();
    plugin.initialize(tsdb);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void initializeEmptyHost() throws Exception {
    config.overrideConfig("tsd.search.elasticsearch.host", "");
    final ElasticSearch plugin = new ElasticSearch();
    plugin.initialize(tsdb);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void initializeMisingIndex() throws Exception {
    config.overrideConfig("tsd.search.elasticsearch.index", "");
    final ElasticSearch plugin = new ElasticSearch();
    plugin.initialize(tsdb);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void initializeMisingTSmega() throws Exception {
    config.overrideConfig("tsd.search.elasticsearch.tsmeta_type", "");
    final ElasticSearch plugin = new ElasticSearch();
    plugin.initialize(tsdb);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void initializeMisingUIDMeta() throws Exception {
    config.overrideConfig("tsd.search.elasticsearch.uidmeta_type", "");
    final ElasticSearch plugin = new ElasticSearch();
    plugin.initialize(tsdb);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void initializeNoSuchClass() throws Exception {
    config.overrideConfig("tsd.search.elasticsearch.schema.tsmeta", 
        "net.opentsdb.search.TestElasticSearch$NoSuchClass");
    final ElasticSearch plugin = new ElasticSearch();
    plugin.initialize(tsdb);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void initializeBadClassCtor() throws Exception {
    config.overrideConfig("tsd.search.elasticsearch.schema.tsmeta", 
        "net.opentsdb.search.TestElasticSearch$BadSchema");
    final ElasticSearch plugin = new ElasticSearch();
    plugin.initialize(tsdb);
  }

  @Test
  public void indexTSMeta() throws Exception {
    final TSMeta meta = new TSMeta("010101");
    meta.setDisplayName("Testing");
    ElasticSearch plugin = new ElasticSearch();
    Whitebox.setInternalState(plugin, "ts_meta_schema", ts_meta_schema);
    
    Deferred<Object> deferred = plugin.indexTSMeta(meta);
    verify(ts_meta_schema, times(1)).index(meta);
    assertNull(deferred);
    
    Whitebox.setInternalState(plugin, "ts_meta_schema", (TSMetaSchema) null);
    deferred = plugin.indexTSMeta(meta);
    verify(ts_meta_schema, times(1)).index(meta);
    assertFalse((Boolean) deferred.join());
  }
  
  @Test
  public void deleteTSMeta() throws Exception {
    ElasticSearch plugin = new ElasticSearch();
    Whitebox.setInternalState(plugin, "ts_meta_schema", ts_meta_schema);
    
    Deferred<Object> deferred = plugin.deleteTSMeta("010101");
    verify(ts_meta_schema, times(1)).delete("010101");
    assertNull(deferred);
    
    Whitebox.setInternalState(plugin, "ts_meta_schema", (TSMetaSchema) null);
    deferred = plugin.deleteTSMeta("010101");
    verify(ts_meta_schema, times(1)).delete("010101");
    assertFalse((Boolean) deferred.join());
  }
  
  @Test
  public void indexUIDMeta() throws Exception {
    final UIDMeta meta = new UIDMeta(UniqueIdType.METRIC, "01");
    meta.setDisplayName("Testing");
    ElasticSearch plugin = new ElasticSearch();
    Whitebox.setInternalState(plugin, "uid_meta_schema", uid_meta_schema);
    
    Deferred<Object> deferred = plugin.indexUIDMeta(meta);
    verify(uid_meta_schema, times(1)).index(meta);
    assertNull(deferred);
    
    Whitebox.setInternalState(plugin, "uid_meta_schema", (UIDMetaSchema) null);
    deferred = plugin.indexUIDMeta(meta);
    verify(uid_meta_schema, times(1)).index(meta);
    assertFalse((Boolean) deferred.join());
  }
  
  @Test
  public void deleteUIDMeta() throws Exception {
    final UIDMeta meta = new UIDMeta(UniqueIdType.METRIC, "01");
    meta.setDisplayName("Testing");
    ElasticSearch plugin = new ElasticSearch();
    Whitebox.setInternalState(plugin, "uid_meta_schema", uid_meta_schema);
    
    Deferred<Object> deferred = plugin.deleteUIDMeta(meta);
    verify(uid_meta_schema, times(1)).delete(meta);
    assertNull(deferred);
    
    Whitebox.setInternalState(plugin, "uid_meta_schema", (UIDMetaSchema) null);
    deferred = plugin.deleteUIDMeta(meta);
    verify(uid_meta_schema, times(1)).delete(meta);
    assertFalse((Boolean) deferred.join());
  }
  
  @Test
  public void indexAnnotation() throws Exception {
    final Annotation note = new Annotation();
    note.setTSUID("01010101");
    ElasticSearch plugin = new ElasticSearch();
    Whitebox.setInternalState(plugin, "annotation_schema", annotation_schema);
    
    Deferred<Object> deferred = plugin.indexAnnotation(note);
    verify(annotation_schema, times(1)).index(note);
    assertNull(deferred);
    
    Whitebox.setInternalState(plugin, "annotation_schema", (AnnotationSchema) null);
    deferred = plugin.indexAnnotation(note);
    verify(annotation_schema, times(1)).index(note);
    assertFalse((Boolean) deferred.join());
  }
  
  @Test
  public void deleteAnnotation() throws Exception {
    final Annotation note = new Annotation();
    note.setTSUID("01010101");
    ElasticSearch plugin = new ElasticSearch();
    Whitebox.setInternalState(plugin, "annotation_schema", annotation_schema);
    
    Deferred<Object> deferred = plugin.deleteAnnotation(note);
    verify(annotation_schema, times(1)).delete(note);
    assertNull(deferred);
    
    Whitebox.setInternalState(plugin, "annotation_schema", (AnnotationSchema) null);
    deferred = plugin.deleteAnnotation(note);
    verify(annotation_schema, times(1)).delete(note);
    assertFalse((Boolean) deferred.join());
  }
  
  @Test
  public void shutdown() throws Exception {
    final ElasticSearch plugin = new ElasticSearch();
    plugin.initialize(tsdb);
    
    assertNull(plugin.shutdown().join());
    verify(client, times(1)).close();
    
    doThrow(new IOException("Boo!")).when(client).close();
    Deferred<Object> deferred = plugin.shutdown();
    try {
      deferred.join();
      fail("Expected IOException");
    } catch (IOException e) { }
  }
  
  public static class BadSchema extends TSMetaSchema {

    public BadSchema(final ElasticSearch es, final long ignored) {
      super(es);
    }
    
  }

  /**
   * Helper to mock out a response.
   * @param status_code The status code to return.
   * @param json A non-null JSON string to return. Can be empty.
   * @return A mocked response.
   * @throws Exception If something goes pear shaped.
   */
  public static HttpResponse mockResponse(final int status_code, 
                                          final String json) 
        throws Exception {
    final HttpResponse response = mock(HttpResponse.class);
    final StatusLine status_line = mock(StatusLine.class);
    when(status_line.getStatusCode()).thenReturn(status_code);
    when(response.getStatusLine()).thenReturn(status_line);
    when(response.getEntity()).thenReturn(new StringEntity(json));
    return response;
  }
}
