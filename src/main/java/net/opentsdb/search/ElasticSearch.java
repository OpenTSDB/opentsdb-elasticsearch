// This file is part of OpenTSDB.
// Copyright (C) 2013-2017  The OpenTSDB Authors.
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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.search.SearchQuery.SearchType;
import net.opentsdb.search.schemas.annotation.AnnotationSchema;
import net.opentsdb.search.schemas.tsmeta.TSMetaSchema;
import net.opentsdb.search.schemas.uidmeta.UIDMetaSchema;
import net.opentsdb.utils.JSON;

import org.hbase.async.Counter;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

public final class ElasticSearch extends SearchPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(ElasticSearch.class);
  
  private CloseableHttpAsyncClient http_client;
  private String index = "opentsdb";
  private boolean async_replication;
  private ESPluginConfig config = null;
  private String host;
  private TSMetaSchema ts_meta_schema;
  private UIDMetaSchema uid_meta_schema;
  private AnnotationSchema annotation_schema;
  
  final Counter queries_executed = new Counter();

  /**
   * Default constructor
   */
  public ElasticSearch() {

  }

  /**
   * Initializes the search plugin, setting up the HTTP client pool and config
   * options.
   * @param tsdb The TSDB to which we belong
   * @throws IllegalArgumentException if a config value is invalid
   * @throws NumberFormatException if a config value is invalid
   */
  @Override
  public void initialize(final TSDB tsdb) {
    config = new ESPluginConfig(tsdb.getConfig());
    
    host = config.getString("tsd.search.elasticsearch.host");
    if (Strings.isNullOrEmpty(host)) {
      throw new IllegalArgumentException(
          "Missing config 'tsd.search.elasticsearch.host'");
    }
    if (!host.toLowerCase().startsWith("http")) {
      host = "http://" + host;
    }

    // set index/types
    index = config.getString("tsd.search.elasticsearch.index");
    if (Strings.isNullOrEmpty(index)) {
      throw new IllegalArgumentException("Missing config "
          + "'tsd.search.elasticsearch.index'");
    }
    
    // TODO - configs for all these params
    final RequestConfig requestConfig = RequestConfig.custom()
        .setConnectTimeout(500)
        .setConnectionRequestTimeout(1000)
        .setSocketTimeout(5000)
        .build();

    final ConnectionConfig connectionConfig = 
        ConnectionConfig.custom().setBufferSize(8 * 1024)
          .setFragmentSizeHint(8 * 1024).build();
    
    IOReactorConfig.Builder ioReactorConfigBuilder = IOReactorConfig.custom();
    ioReactorConfigBuilder.setConnectTimeout(1000);
    ioReactorConfigBuilder.setInterestOpQueued(false);
    ioReactorConfigBuilder.setSelectInterval(100);
    ioReactorConfigBuilder.setShutdownGracePeriod(500L);
    ioReactorConfigBuilder.setSoKeepAlive(true);
    ioReactorConfigBuilder.setSoLinger(-1);
    ioReactorConfigBuilder.setSoReuseAddress(false);
    ioReactorConfigBuilder.setSoTimeout(1000);
    ioReactorConfigBuilder.setTcpNoDelay(false);

    try {
      final ConnectingIOReactor reactor = 
          new DefaultConnectingIOReactor(ioReactorConfigBuilder.build());
      final PoolingNHttpClientConnectionManager connManager = 
          new PoolingNHttpClientConnectionManager(reactor);
      connManager.setMaxTotal(
          config.getInt("tsd.search.elasticsearch.pool.max_total"));
      connManager.setDefaultMaxPerRoute(
          config.getInt("tsd.search.elasticsearch.pool.max_per_route"));
      http_client = HttpAsyncClients.custom().setDefaultRequestConfig(requestConfig)
          .setDefaultConnectionConfig(connectionConfig)
          .setConnectionManager(connManager)
          .build();

      http_client.start();
    } catch (IOReactorException e) {
      throw new RuntimeException("Unable to create http client", e);
    }
    
    String schema = config.getString("tsd.search.elasticsearch.schema.tsmeta");
    if (Strings.isNullOrEmpty(schema)) {
      LOG.info("Disabling TSMeta schema.");
      ts_meta_schema = null;
    } else {
      try {
        final Class<?> clazz = Class.forName(schema);
        final Constructor<?> ctor = clazz.getConstructor(ElasticSearch.class);
        ts_meta_schema = (TSMetaSchema) ctor.newInstance(this);
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("No TSMeta schema class found "
            + "for: " + schema);
      } catch (NoSuchMethodException e) {
        throw new IllegalArgumentException("TSMeta schema did not implement "
            + "the proper constructor. " + schema);
      } catch (SecurityException e) {
        throw new IllegalArgumentException("Unexpected security exception "
            + "loading TSMeta schema: " + schema, e);
      } catch (InstantiationException e) {
        throw new IllegalArgumentException("Unexpected exception instantiating "
            + "TSMeta schema: " + schema, e);
      } catch (IllegalAccessException e) {
        throw new IllegalArgumentException("Unexpected exception instantiating "
            + "TSMeta schema: " + schema, e);
      } catch (IllegalArgumentException e) {
        throw e;
      } catch (InvocationTargetException e) {
        throw new IllegalArgumentException("Unexpected exception instantiating "
            + "TSMeta schema: " + schema, e);
      }
    }
    
    schema = config.getString("tsd.search.elasticsearch.schema.uidmeta");
    if (Strings.isNullOrEmpty(schema)) {
      LOG.info("Disabling UIDMeta schema.");
      uid_meta_schema = null;
    } else {
      try {
        final Class<?> clazz = Class.forName(schema);
        final Constructor<?> ctor = clazz.getConstructor(ElasticSearch.class);
        uid_meta_schema = (UIDMetaSchema) ctor.newInstance(this);
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("No UIDMeta schema class found "
            + "for: " + schema);
      } catch (NoSuchMethodException e) {
        throw new IllegalArgumentException("UIDMeta schema did not implement "
            + "the proper constructor. " + schema);
      } catch (SecurityException e) {
        throw new IllegalArgumentException("Unexpected security exception "
            + "loading UIDMeta schema: " + schema, e);
      } catch (InstantiationException e) {
        throw new IllegalArgumentException("Unexpected exception instantiating "
            + "UIDMeta schema: " + schema, e);
      } catch (IllegalAccessException e) {
        throw new IllegalArgumentException("Unexpected exception instantiating "
            + "UIDMeta schema: " + schema, e);
      } catch (IllegalArgumentException e) {
        throw e;
      } catch (InvocationTargetException e) {
        throw new IllegalArgumentException("Unexpected exception instantiating "
            + "UIDMeta schema: " + schema, e);
      }
    }
    
    schema = config.getString("tsd.search.elasticsearch.schema.annotation");
    if (Strings.isNullOrEmpty(schema)) {
      LOG.info("Disabling Annotation schema.");
      annotation_schema = null;
    } else {
      try {
        final Class<?> clazz = Class.forName(schema);
        final Constructor<?> ctor = clazz.getConstructor(ElasticSearch.class);
        annotation_schema = (AnnotationSchema) ctor.newInstance(this);
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("No Annotation schema class found "
            + "for: " + schema);
      } catch (NoSuchMethodException e) {
        throw new IllegalArgumentException("Annotation schema did not implement "
            + "the proper constructor. " + schema);
      } catch (SecurityException e) {
        throw new IllegalArgumentException("Unexpected security exception "
            + "loading Annotation schema: " + schema, e);
      } catch (InstantiationException e) {
        throw new IllegalArgumentException("Unexpected exception instantiating "
            + "Annotation schema: " + schema, e);
      } catch (IllegalAccessException e) {
        throw new IllegalArgumentException("Unexpected exception instantiating "
            + "Annotation schema: " + schema, e);
      } catch (IllegalArgumentException e) {
        throw e;
      } catch (InvocationTargetException e) {
        throw new IllegalArgumentException("Unexpected exception instantiating "
            + "Annotation schema: " + schema, e);
      }
    }
  }
  
  @Override
  public Deferred<Object> indexTSMeta(final TSMeta meta) {
    if (ts_meta_schema != null) {
      return ts_meta_schema.index(meta);
    }
    return Deferred.<Object>fromResult(false);
  }
  
  public Deferred<Object> deleteTSMeta(final String tsuid) {
    if (ts_meta_schema != null) {
      return ts_meta_schema.delete(tsuid);
    }
    return Deferred.<Object>fromResult(false);
  }
  
  @Override
  public Deferred<Object> indexUIDMeta(final UIDMeta meta) {
    if (uid_meta_schema != null) {
      return uid_meta_schema.index(meta);
    }
    return Deferred.<Object>fromResult(false);
  }

  @Override
  public Deferred<Object> deleteUIDMeta(final UIDMeta meta) {
    if (uid_meta_schema != null) {
      return uid_meta_schema.delete(meta);
    }
    return Deferred.<Object>fromResult(false);
  }
  
  @Override
  public Deferred<Object> indexAnnotation(final Annotation note) {
    if (annotation_schema != null) {
      return annotation_schema.index(note);
    }
    return Deferred.<Object>fromResult(false);
  }

  @Override
  public Deferred<Object> deleteAnnotation(final Annotation note) {
    if (annotation_schema != null) {
      return annotation_schema.delete(note);
    }
    return Deferred.<Object>fromResult(false);
  }
  
  @Override
  public Deferred<SearchQuery> executeQuery(final SearchQuery query) {
    final Deferred<SearchQuery> result = new Deferred<SearchQuery>();

    final StringBuilder uri = new StringBuilder(host);
    uri.append("/").append(index).append("/");
    switch(query.getType()) {
      case TSMETA:
      case TSMETA_SUMMARY:
      case TSUIDS:
        uri.append(ts_meta_schema.docType());
        break;
      case UIDMETA:
        uri.append(uid_meta_schema.docType());
        break;
      case ANNOTATION:
        uri.append(annotation_schema.docType());
        break;
      default:
        return Deferred.fromError(new IllegalArgumentException(
            "Unhandled query type: " + query.getType()));
    }
    uri.append("/_search");

    // setup the query body
    HashMap<String, Object> body = new HashMap<String, Object>(3);
    body.put("size", query.getLimit());
    body.put("from", query.getStartIndex());

    HashMap<String, Object> qs = new HashMap<String, Object>(1);
    body.put("query", qs);
    HashMap<String, String> query_string = new HashMap<String, String>(1);
    query_string.put("query", query.getQuery());
    qs.put("query_string", query_string);

    final HttpPost post = new HttpPost(uri.toString());
    post.setEntity(new ByteArrayEntity(JSON.serializeToBytes(body)));

    http_client.execute(post, new SearchCB(query, result));
    queries_executed.increment();
    return result;
  }

  /**
   * Gracefully closes connections
   */
  @Override
  public Deferred<Object> shutdown() {
    try {
      if (http_client != null) {
        http_client.close();
      }
      return Deferred.fromResult(null);
    } catch (IOException e) {
      return Deferred.fromError(e);
    }
  }

  /** @return the version of this plugin */
  public String version() {
    return "2.0.0";
  }
  
  @Override
  public void collectStats(final StatsCollector collector) {
    if (ts_meta_schema != null) {
      collector.record("search.tsmeta_added", ts_meta_schema.added());
      collector.record("search.tsmeta_deleted", ts_meta_schema.deleted());
      collector.record("search.tsmeta_errors", ts_meta_schema.errors());
    }
    
    if (uid_meta_schema != null) {
      collector.record("search.uid_added", uid_meta_schema.added());
      collector.record("search.uid_deleted", uid_meta_schema.deleted());
      collector.record("search.uid_errors", uid_meta_schema.errors());
    }
    
    if (annotation_schema != null) {
      collector.record("search.annotation_added", annotation_schema.added());
      collector.record("search.annotation_deleted", annotation_schema.deleted());
      collector.record("search.annotation_errors", annotation_schema.errors());
    }
    
    collector.record("search.queries_executed", queries_executed.get());
  }
  
  public class SearchCB implements FutureCallback<HttpResponse> {

    final SearchQuery query;
    final Deferred<SearchQuery> result;

    public SearchCB(final SearchQuery query, final Deferred<SearchQuery> result) {
      this.query = query;
      this.result = result;
    }

    @Override
    public void cancelled() {
      result.callback(null);
    }

    @Override
    public void completed(final HttpResponse content) {
      try {
        JsonParser jp = JSON.parseToStream(EntityUtils.toString(content.getEntity()));
        if (jp == null) {
          LOG.warn("Query response was null or empty");
          result.callback(null);
          return;
        }
  
        try {
          JsonToken next = jp.nextToken();
          if (next != JsonToken.START_OBJECT) {
            LOG.error("Error: root should be object: quiting.");
            result.callback(null);
            return;
          }
  
          final List<Object> objects = new ArrayList<Object>();
  
          // loop through the JSON structure
          String parent = "";
          String last = "";
  
          while (jp.nextToken() != null) {
            String fieldName = jp.getCurrentName();
            if (fieldName != null)
              last = fieldName;
  
            if (jp.getCurrentToken() == JsonToken.START_ARRAY ||
                jp.getCurrentToken() == JsonToken.START_OBJECT)
              parent = last;
  
            if (fieldName != null && fieldName.equals("_source")) {
              if (jp.nextToken() == JsonToken.START_OBJECT) {
                // parse depending on type
                switch (query.getType()) {
                  case TSMETA:
                  case TSMETA_SUMMARY:
                  case TSUIDS:
                    final TSMeta meta = jp.readValueAs(TSMeta.class);
                    if (query.getType() == SearchType.TSMETA) {
                      objects.add(meta);
                    } else if (query.getType() == SearchType.TSUIDS) {
                      objects.add(meta.getTSUID());
                    } else {
                      final HashMap<String, Object> map =
                        new HashMap<String, Object>(3);
                      map.put("tsuid", meta.getTSUID());
                      map.put("metric", meta.getMetric().getName());
                      final HashMap<String, String> tags =
                        new HashMap<String, String>(meta.getTags().size() / 2);
                      int idx = 0;
                      String name = "";
                      for (final UIDMeta uid : meta.getTags()) {
                        if (idx % 2 == 0) {
                          name = uid.getName();
                        } else {
                          tags.put(name, uid.getName());
                        }
                        idx++;
                      }
                      map.put("tags", tags);
                      objects.add(map);
                    }
                    break;
                  case UIDMETA:
                    final UIDMeta uid = jp.readValueAs(UIDMeta.class);
                    objects.add(uid);
                    break;
                  case ANNOTATION:
                    final Annotation note = jp.readValueAs(Annotation.class);
                    objects.add(note);
                    break;
                }
              }else
                LOG.warn("Invalid _source value from ES, should have been a START_OBJECT");
            } else if (fieldName != null && jp.getCurrentToken() != JsonToken.FIELD_NAME &&
                parent.equals("hits") && fieldName.equals("total")){
              LOG.trace("Total hits: [" + jp.getValueAsInt() + "]");
              query.setTotalResults(jp.getValueAsInt());
            } else if (fieldName != null && jp.getCurrentToken() != JsonToken.FIELD_NAME &&
                fieldName.equals("took")){
              LOG.trace("Time taken: [" + jp.getValueAsInt() + "]");
              query.setTime(jp.getValueAsInt());
            }
  
            query.setResults(objects);
          }
  
          result.callback(query);
  
        } catch (JsonParseException e) {
          LOG.error("Query failed", e);
          throw new RuntimeException(e);
        } catch (IOException e) {
          LOG.error("Query failed", e);
          throw new RuntimeException(e);
        }
      } catch (ParseException e1) {
        LOG.error("Query failed", e1);
        throw new RuntimeException(e1);
      } catch (IOException e1) {
        LOG.error("Query failed", e1);
        throw new RuntimeException(e1);
      }
    }

    @Override
    public void failed(final Exception e) {
      LOG.error("Query failed", e);
      throw new RuntimeException(e);
    }
  }

  public String host() {
    return host;
  }

  public String index() {
    return index;
  }
  
  public CloseableHttpAsyncClient httpClient() {
    return http_client;
  }
  
  public boolean asyncReplication() {
    return async_replication;
  }
  
  public ESPluginConfig config() {
    return config;
  }
  
  public TSMetaSchema tsMetaSchema() {
    return ts_meta_schema;
  }
  
  public UIDMetaSchema uidMetaSchema() {
    return uid_meta_schema;
  }
  
  public AnnotationSchema annotationSchema() {
    return annotation_schema;
  }
}
