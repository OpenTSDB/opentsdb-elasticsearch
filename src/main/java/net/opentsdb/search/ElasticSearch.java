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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
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
import net.opentsdb.utils.JSON;

import org.hbase.async.Counter;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.stumbleupon.async.Deferred;

public final class ElasticSearch extends SearchPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(ElasticSearch.class);
  
  private ImmutableList<HttpHost> hosts;
  private CloseableHttpAsyncClient httpClient;
  private String index = "opentsdb";
  private String tsmeta_type = "tsmeta";
  private String uidmeta_type = "uidmeta";
  private String annotation_type = "annotation";
  private ESPluginConfig config = null;

  private final Counter tsmetaAdded = new Counter();
  private final Counter tsmetaDeleted = new Counter();
  private final Counter uidAdded = new Counter();
  private final Counter uidDeleted = new Counter();
  private final Counter annotationAdded = new Counter();
  private final Counter annotationDeleted = new Counter();
  private final Counter queriesExecuted = new Counter();

  /**
   * Default constructor
   */
  public ElasticSearch() {

  }

  /**
   * Initializes the search plugin, setting up the HTTP client pool and config
   * options.
   * @param tsdb The TSDB to which we belong
   * @return null if successful, otherwise it throws an exception
   * @throws IllegalArgumentException if a config value is invalid
   * @throws NumberFormatException if a config value is invalid
   */
  @Override
  public void initialize(final TSDB tsdb) {
    config = new ESPluginConfig(tsdb.getConfig());
    setConfiguration();
    
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
      PoolingNHttpClientConnectionManager connManager = 
          new PoolingNHttpClientConnectionManager(reactor);
      connManager.setMaxTotal(
          config.getInt("tsd.search.elasticsearch.pool.max_total"));
      connManager.setDefaultMaxPerRoute(
          config.getInt("tsd.search.elasticsearch.pool.max_per_route"));
      httpClient = HttpAsyncClients.custom().setDefaultRequestConfig(requestConfig)
          .setDefaultConnectionConfig(connectionConfig)
          .setConnectionManager(connManager)
          .build();

      httpClient.start();
    } catch (IOReactorException e) {
      throw new RuntimeException("Unable to create http client", e);
    }
  }

  /**
   * Queues the given TSMeta object for indexing
   * @param meta The meta data object to index
   * @return null
   */
  @Override
  public Deferred<Object> indexTSMeta(final TSMeta meta) {
    tsmetaAdded.increment();
    final StringBuilder uri = new StringBuilder("http://");
    uri.append(hosts.get(0).toHostString());
    uri.append("/").append(index).append("/").append(tsmeta_type).append("/");
    uri.append(meta.getTSUID()).append("?replication=async");
    
    final HttpPost post = new HttpPost(uri.toString());
    post.setEntity(new ByteArrayEntity(TSMetaAugment.serializeToBytes(meta)));
    
    final Deferred<Object> result = new Deferred<Object>();
    httpClient.execute(post, new AsyncCB(result));
    return result;
  }

  /**
   * Queues the given TSMeta object for deletion
   * @param meta The meta data object to delete
   * @return null
   */
  public Deferred<Object> deleteTSMeta(final String tsuid) {
    tsmetaDeleted.increment();
    final StringBuilder uri = new StringBuilder("http://");
    uri.append(hosts.get(0).toHostString());
    uri.append("/").append(index).append("/").append(tsmeta_type).append("/");
    uri.append(tsuid).append("?replication=async");
    
    final HttpGet delete = new HttpGet(uri.toString());
    
    final Deferred<Object> result = new Deferred<Object>();
    httpClient.execute(delete, new AsyncCB(result));
    return result;
  }

  /**
   * Queues the given UIDMeta object for indexing
   * @param meta The meta data object to index
   * @return null
   */
  @Override
  public Deferred<Object> indexUIDMeta(final UIDMeta meta) {
    uidAdded.increment();
    final StringBuilder uri = new StringBuilder("http://");
    uri.append(hosts.get(0).toHostString());
    uri.append("/").append(index).append("/").append(uidmeta_type).append("/");
    uri.append(meta.getType().toString()).append(meta.getUID());
    uri.append("?replication=async");
    
    final HttpPost post = new HttpPost(uri.toString());
    post.setEntity(new ByteArrayEntity(JSON.serializeToBytes(meta)));
    
    final Deferred<Object> result = new Deferred<Object>();
    httpClient.execute(post, new AsyncCB(result));
    return result;
  }

  /**
   * Queues the given UIDMeta object for deletion
   * @param meta The meta data object to delete
   * @return null
   */
  public Deferred<Object> deleteUIDMeta(final UIDMeta meta) {
    uidDeleted.increment();
    final StringBuilder uri = new StringBuilder("http://");
    uri.append(hosts.get(0).toHostString());
    uri.append("/").append(index).append("/").append(tsmeta_type).append("/");
    uri.append(meta.getType().toString()).append(meta.getUID());
    uri.append("?replication=async");
    
    final HttpGet delete = new HttpGet(uri.toString());
    
    final Deferred<Object> result = new Deferred<Object>();
    httpClient.execute(delete, new AsyncCB(result));
    return result;
  }

  /**
   * Indexes an annotation object
   * <b>Note:</b> Unique Document ID = TSUID and Start Time
   * @param note The annotation to index
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}
   * (think of it as {@code Deferred<Void>}).
   */
  public Deferred<Object> indexAnnotation(final Annotation note) {
    final StringBuilder uri = new StringBuilder("http://");
    uri.append(hosts.get(0).toHostString());
    uri.append("/").append(index).append("/").append(annotation_type).append("/");
    uri.append(note.getStartTime());
    if (note != null ) {
      uri.append(note.getTSUID());
      annotationAdded.increment();
    }
    uri.append("?replication=async");
    
    final HttpPost post = new HttpPost(uri.toString());
    post.setEntity(new ByteArrayEntity(JSON.serializeToBytes(note)));
    
    final Deferred<Object> result = new Deferred<Object>();
    httpClient.execute(post, new AsyncCB(result));
    return result;
  }

  /**
   * Called to remove an annotation object from the index
   * <b>Note:</b> Unique Document ID = TSUID and Start Time
   * @param note The annotation to remove
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}
   * (think of it as {@code Deferred<Void>}).
   */
  public Deferred<Object> deleteAnnotation(final Annotation note) {
    final StringBuilder uri = new StringBuilder("http://");
    uri.append(hosts.get(0).toHostString());
    uri.append("/").append(index).append("/").append(annotation_type).append("/");
    uri.append(note.getStartTime());
    if (note != null ) {
      uri.append(note.getTSUID());
      annotationDeleted.increment();
    }
    
    final HttpGet delete = new HttpGet(uri.toString());
    
    final Deferred<Object> result = new Deferred<Object>();
    httpClient.execute(delete, new AsyncCB(result));
    return result;
  }

  public Deferred<SearchQuery> executeQuery(final SearchQuery query) {
    final Deferred<SearchQuery> result = new Deferred<SearchQuery>();

    final StringBuilder uri = new StringBuilder("http://");
    uri.append(hosts.get(0).toHostString());
    uri.append("/").append(index).append("/");
    switch(query.getType()) {
      case TSMETA:
      case TSMETA_SUMMARY:
      case TSUIDS:
        uri.append(tsmeta_type);
        break;
      case UIDMETA:
        uri.append(uidmeta_type);
        break;
      case ANNOTATION:
        uri.append(annotation_type);
        break;
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

    httpClient.execute(post, new SearchCB(query, result));
    queriesExecuted.increment();
    return result;
  }

  /**
   * Gracefully closes connections
   */
  @Override
  public Deferred<Object> shutdown() {
    try {
      httpClient.close();
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
    collector.record("search.tsmeta_added", tsmetaAdded.get());
    collector.record("search.tsmeta_deleted", tsmetaDeleted.get());
    collector.record("search.uid_added", uidAdded.get());
    collector.record("search.uid_deleted", uidDeleted.get());
    collector.record("search.queries_executed", queriesExecuted.get());
  }

  /**
   * Parses semicoln separated hosts from a config line into a host list. If
   * a given host includes a port, e.g. "host:port", the port will be parsed,
   * otherwise port 9200 will be used.
   * @param config The config line to parse
   * @throws IllegalArgumentException if the line was empty or no hosts were
   * parsed
   * @throws NumberFormatException if a parsed port can't be converted to an
   * integer
   */
  private void setHosts(final String config) {
    if (config == null || config.isEmpty()) {
      throw new IllegalArgumentException("The hosts config was empty");
    }

    Builder<HttpHost> host_list = ImmutableList.<HttpHost>builder();
    String[] split_hosts = config.split(";");
    for (String host : split_hosts) {
      String[] host_split = host.split(":");
      int port = 9200;
      if (host_split.length > 1) {
        port = Integer.parseInt(host_split[1]);
      }
      host_list.add(new HttpHost(host_split[0], port));
    }
    this.hosts = host_list.build();
    if (this.hosts.size() < 1) {
      throw new IllegalArgumentException(
          "No hosts were found to load into the list");
    }
  }

  /**
   * Helper that loads config settings and throws exceptions if something is
   * amiss.
   * @throws IllegalArgumentException if a config value is invalid
   * @throws NumberFormatException if a config value is invalid
   */
  private void setConfiguration() {
    final String host_config =
      config.getString("tsd.search.elasticsearch.hosts");
    if (host_config == null || host_config.isEmpty()) {
      throw new IllegalArgumentException("Missing search hosts configuration");
    }
    setHosts(host_config);

    // set index/types
    index = config.getString("tsd.search.elasticsearch.index");
    if (index == null || index.isEmpty()) {
      throw new IllegalArgumentException("Invalid index configuration value");
    }
    tsmeta_type = config.getString("tsd.search.elasticsearch.tsmeta_type");
    if (tsmeta_type == null || tsmeta_type.isEmpty()) {
      throw new IllegalArgumentException(
          "Invalid tsmeta_type configuration value");
    }
    uidmeta_type = config.getString("tsd.search.elasticsearch.uidmeta_type");
    if (uidmeta_type == null || uidmeta_type.isEmpty()) {
      throw new IllegalArgumentException(
          "Invalid uidmeta_type configuration value");
    }
  }

  final class AsyncCB implements FutureCallback<HttpResponse> {

    final Deferred<Object> deferred;

    public AsyncCB(final Deferred<Object> deferred) {
      this.deferred = deferred;
    }

    @Override
    public void cancelled() {
      LOG.warn("Post was cancelled");
    }

    @Override
    public void completed(final HttpResponse content) {
      deferred.callback(true);
    }

    @Override
    public void failed(Exception e) {
      LOG.error("Post Exception", e);
    }

  }

  final class SearchCB implements FutureCallback<HttpResponse> {

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
}
