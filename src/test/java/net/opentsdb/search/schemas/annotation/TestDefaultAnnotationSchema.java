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
package net.opentsdb.search.schemas.annotation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CancellationException;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.TimeoutException;

import net.opentsdb.meta.Annotation;
import net.opentsdb.search.ESPluginConfig;
import net.opentsdb.search.ElasticSearch;
import net.opentsdb.search.TestElasticSearch;
import net.opentsdb.utils.Config;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ ElasticSearch.class, HttpAsyncClients.class })
public class TestDefaultAnnotationSchema {
  private static final String HOST = "localhost:9092";
  
  private ESPluginConfig config;
  private CloseableHttpAsyncClient client;
  private ElasticSearch es;
  private FutureCallback<HttpResponse> cb;
  private HttpUriRequest request;
  private Annotation note;
  private String index;
  private String doc_type;
  
  @SuppressWarnings("unchecked")
  @Before
  public void before() throws Exception {
    config = new ESPluginConfig(new Config(false));
    client = mock(CloseableHttpAsyncClient.class);
    es = mock(ElasticSearch.class);
    note = new Annotation();
    note.setTSUID("010101");
    note.setDescription("Unit testing Dragonstone!");
    note.setStartTime(1483228800);
    
    index = config.getString("tsd.search.elasticsearch.index");
    doc_type = config.getString("tsd.search.elasticsearch.annotation_type");
    
    when(es.httpClient()).thenReturn(client);
    when(es.host()).thenReturn(HOST);
    when(es.index()).thenReturn(index);
    when(es.config()).thenReturn(config);
    when(client.execute(any(HttpUriRequest.class), 
        any(FutureCallback.class)))
      .thenAnswer(new Answer<Void>() {
        @Override
        public Void answer(InvocationOnMock invocation) throws Throwable {
          request = (HttpUriRequest) invocation.getArguments()[0];
          cb = (FutureCallback<HttpResponse>) invocation.getArguments()[1];
          return null;
        }
      });
  }
  
  @Test
  public void ctor() {
    DefaultAnnotationSchema schema = new DefaultAnnotationSchema(es);
    assertEquals(doc_type, schema.docType());
    
    config.overrideConfig("tsd.search.elasticsearch.annotation_type", null);
    try {
      new DefaultAnnotationSchema(es);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void index() throws Exception {
    final DefaultAnnotationSchema schema = new DefaultAnnotationSchema(es);
    Deferred<Object> deferred = schema.index(note);
    try {
      deferred.join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    assertEquals(HOST + "/" + index + "/" + doc_type + "/1483228800010101", 
        request.getURI().toString());
    
    // good
    cb.completed(TestElasticSearch.mockResponse(204, ""));
    assertTrue((Boolean) deferred.join());
    assertCounters(schema, 1, 0, 0);
    
    // good with async
    when(es.asyncReplication()).thenReturn(true);
    deferred = schema.index(note);
    assertEquals(HOST + "/" + index + "/" + doc_type 
        + "/1483228800010101?replication=async", request.getURI().toString());
    final String payload = EntityUtils.toString(((HttpPost) request)
        .getEntity());
    assertTrue(payload.contains("\"description\":\"Unit testing Dragonstone!\""));
    assertTrue(payload.contains("\"tsuid\":\"010101\""));
    assertTrue(payload.contains("\"startTime\":1483228800"));
    
    // global
    note = new Annotation();
    note.setDescription("Unit testing Dragonstone!");
    note.setStartTime(1483228800);
    deferred = schema.index(note);
    assertEquals(HOST + "/" + index + "/" + doc_type 
        + "/1483228800?replication=async", request.getURI().toString());
    
    // bad
    deferred = schema.index(note);
    cb.completed(TestElasticSearch.mockResponse(500, "WTF?"));
    try {
      deferred.join();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    assertCounters(schema, 1, 0, 1);
    
    // cancelled
    deferred = schema.index(note);
    cb.cancelled();
    try {
      deferred.join();
      fail("Expected CancellationException");
    } catch (CancellationException e) { }
    assertCounters(schema, 1, 0, 2);
    
    // oopsies
    deferred = schema.index(note);
    cb.failed(new IllegalArgumentException("Boo!"));
    try {
      deferred.join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    assertCounters(schema, 1, 0, 3);
    
    deferred = schema.index(null);
    try {
      deferred.join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void delete() throws Exception {
    final DefaultAnnotationSchema schema = new DefaultAnnotationSchema(es);
    Deferred<Object> deferred = schema.delete(note);
    try {
      deferred.join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException e) { }
    assertEquals(HOST + "/" + index + "/" + doc_type + "/1483228800010101", 
        request.getURI().toString());
    
    // good
    cb.completed(TestElasticSearch.mockResponse(204, ""));
    assertTrue((Boolean) deferred.join());
    assertCounters(schema, 0, 1, 0);
    
    // good with async
    when(es.asyncReplication()).thenReturn(true);
    deferred = schema.delete(note);
    assertEquals(HOST + "/" + index + "/" + doc_type + 
        "/1483228800010101?replication=async",request.getURI().toString());
    
    // bad
    deferred = schema.delete(note);
    cb.completed(TestElasticSearch.mockResponse(500, "WTF?"));
    try {
      deferred.join();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
    assertCounters(schema, 0, 1, 1);
    
    // cancelled
    deferred = schema.delete(note);
    cb.cancelled();
    try {
      deferred.join();
      fail("Expected CancellationException");
    } catch (CancellationException e) { }
    assertCounters(schema, 0, 1, 2);
    
    // oopsies
    deferred = schema.delete(note);
    cb.failed(new IllegalArgumentException("Boo!"));
    try {
      deferred.join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    assertCounters(schema, 0, 1, 3);
    
    deferred = schema.delete(null);
    try {
      deferred.join();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  void assertCounters(final AnnotationSchema schema, 
                      final long added,
                      final long deleted,
                      final long errors) {
    assertEquals(added, schema.added());
    assertEquals(deleted, schema.deleted());
    assertEquals(errors, schema.errors());
  }
}
