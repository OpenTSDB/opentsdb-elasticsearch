// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
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

import httpfailover.FailoverHttpClient;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.fluent.Async;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.search.SearchQuery.SearchType;
import net.opentsdb.stats.StatsCollector;
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

  private final ConcurrentLinkedQueue<TSMeta> tsuids;
  private final ConcurrentLinkedQueue<String> tsuids_delete;
  private final ConcurrentLinkedQueue<UIDMeta> uids;
  private final ConcurrentLinkedQueue<UIDMeta> uids_delete;
  private final ConcurrentLinkedQueue<Annotation> annotations;
  private final ConcurrentLinkedQueue<Annotation> annotations_delete;
  private final ExecutorService threadpool = Executors.newFixedThreadPool(2);

  private Thread[] indexers = null;
  private ImmutableList<HttpHost> hosts;
  private FailoverHttpClient httpClient;
  private int index_threads = 1;
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
    tsuids = new ConcurrentLinkedQueue<TSMeta>();
    tsuids_delete = new ConcurrentLinkedQueue<String>();
    uids = new ConcurrentLinkedQueue<UIDMeta>();
    uids_delete = new ConcurrentLinkedQueue<UIDMeta>();
    annotations = new ConcurrentLinkedQueue<Annotation>();
    annotations_delete = new ConcurrentLinkedQueue<Annotation>();
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

    // setup a connection pool for reuse
    PoolingClientConnectionManager http_pool =
      new PoolingClientConnectionManager();
    http_pool.setDefaultMaxPerRoute(
        config.getInt("tsd.search.elasticsearch.pool.max_per_route"));
    http_pool.setMaxTotal(
        config.getInt("tsd.search.elasticsearch.pool.max_total"));
    httpClient = new FailoverHttpClient(http_pool);

    // start worker threads
    indexers = new SearchIndexer[index_threads];
    for (int i = 0; i < index_threads; i++) {
      indexers[i] = new SearchIndexer();
      indexers[i].start();
    }
  }

  /**
   * Queues the given TSMeta object for indexing
   * @param meta The meta data object to index
   * @return null
   */
  @Override
  public Deferred<Object> indexTSMeta(final TSMeta meta) {
    if (meta != null) {
      tsuids.add(meta);
      tsmetaAdded.increment();
    }
    return Deferred.fromResult(null);
  }

  /**
   * Queues the given TSMeta object for deletion
   * @param meta The meta data object to delete
   * @return null
   */
  public Deferred<Object> deleteTSMeta(final String tsuid) {
    if (tsuid != null) {
      tsuids_delete.add(tsuid);
      tsmetaDeleted.increment();
    }
    return Deferred.fromResult(null);
  }

  /**
   * Queues the given UIDMeta object for indexing
   * @param meta The meta data object to index
   * @return null
   */
  @Override
  public Deferred<Object> indexUIDMeta(final UIDMeta meta) {
    if (meta != null) {
      uids.add(meta);
      uidAdded.increment();
    }
    return Deferred.fromResult(null);
  }

  /**
   * Queues the given UIDMeta object for deletion
   * @param meta The meta data object to delete
   * @return null
   */
  public Deferred<Object> deleteUIDMeta(final UIDMeta meta) {
    if (meta != null) {
      uids_delete.add(meta);
      uidDeleted.increment();
    }
    return null;
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
    if (note != null) {
      annotations.add(note);
      annotationAdded.increment();
    }
    return Deferred.fromResult(null);
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
    if (note != null) {
      annotations_delete.add(note);
      annotationDeleted.increment();
    }
    return Deferred.fromResult(null);
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

    final Request request = Request.Post(uri.toString());
    request.bodyByteArray(JSON.serializeToBytes(body));

    final Async async = Async.newInstance().use(threadpool);
    async.execute(request, new SearchCB(query, result));
    queriesExecuted.increment();
    return result;
  }

  /**
   * Gracefully closes connections
   */
  @Override
  public Deferred<Object> shutdown() {
    httpClient.getConnectionManager().shutdown();
    return null;
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

    // set thread count
    index_threads = config.getInt("tsd.search.elasticsearch.index_threads");

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

  /**
   * A worker thread that watches all of the queues, pushing waiting objects out
   * to the indexer
   */
  final class SearchIndexer extends Thread {
    public SearchIndexer() {
      super("SearchIndexer");
    }

    /**
     * Loops indefinitely and processes the queues
     */
    public void run() {
      // as long as we find some data, keep looping, otherwise sleep a bit
      while (true) {
        boolean found_one = false;
        final TSMeta tsmeta = tsuids.poll();
        if (tsmeta != null) {
          indexTSUID(tsmeta);
          found_one = true;
        }

        final String tsuid = tsuids_delete.poll();
        if (tsuid != null) {
          deleteTSUID(tsuid);
          found_one = true;
        }

        UIDMeta uidmeta = uids.poll();
        if (uidmeta != null) {
          indexUID(uidmeta);
          found_one = true;
        }

        uidmeta = uids_delete.poll();
        if (uidmeta != null) {
          deleteUID(uidmeta);
          found_one = true;
        }

        Annotation note = annotations.poll();
        if (note != null) {
          indexAnnotation(note);
          found_one = true;
        }

        note = annotations_delete.poll();
        if (note != null) {
          deleteAnnotation(note);
          found_one = true;
        }

        if (!found_one) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            LOG.info("Search worker thread interrupted. Quiting");
            return;
          }
        }
      }
    }

    /**
     * Pushes a TSMeta object to the Elastic Search boxes over HTTP
     * @param meta The meta data to publish
     */
    private void indexTSUID(final TSMeta meta) {
      final HttpPost request = new HttpPost("/" + index + "/" +
          tsmeta_type + "/" + meta.getTSUID() + "?replication=async");
      try {
        request.setEntity(new StringEntity(JSON.serializeToString(meta)));
      } catch (UnsupportedEncodingException e) {
        LOG.error("Encoding Error", e);
      }
      final String response = execute(request);
      if (response == null) {
        LOG.trace("Successfully indexed TSUID: " + meta);
      } else {
        LOG.error("Failed to indexed TSUID: " + meta + " with error: " +
            response);
      }
    }

    /**
     * Deletes a TSMeta document from the search servers
     * @param meta The meta data to delete
     */
    private void deleteTSUID(final String tsuid) {
      final HttpDelete request = new HttpDelete("/" + index + "/" +
          tsmeta_type + "/" + tsuid +
          "?replication=async");
      final String response = execute(request);
      if (response == null) {
        LOG.trace("Successfully deleted TSUID: " + tsuid);
      } else {
        LOG.error("Failed to delete TSUID: " + tsuid + " with error: " +
            response);
      }
    }

    /**
     * Pushes a UIDMeta object to the Elastic Search boxes over HTTP
     * @param meta The meta data to publish
     */
    private void indexUID(final UIDMeta meta) {
      final HttpPost request = new HttpPost("/" + index + "/" +
          uidmeta_type + "/" + meta.getType().toString() + meta.getUID() +
          "?replication=async");
      try {
        request.setEntity(new StringEntity(JSON.serializeToString(meta)));
      } catch (UnsupportedEncodingException e) {
        LOG.error("Encoding Error", e);
      }
      final String response = execute(request);
      if (response == null) {
        LOG.trace("Successfully indexed UID: " + meta);
      } else {
        LOG.error("Failed to index UID: " + meta + " with error: " + response);
      }
    }

    /**
     * Deletes a UIDMeta document from the search servers
     * @param meta The meta data to delete
     */
    private void deleteUID(final UIDMeta meta) {
      final HttpDelete request = new HttpDelete("/" + index + "/" +
          uidmeta_type + "/" + meta.getType().toString() + meta.getUID() +
          "?replication=async");
      final String response = execute(request);
      if (response == null) {
        LOG.trace("Successfully deleted UID: " + meta);
      } else {
        LOG.error("Failed to delete UID: " + meta + " with error: " + response);
      }
    }

    /**
     * Pushes an annotation object to the Elastic Search boexes over HTTP
     * @param note The annotation to index
     */
    private void indexAnnotation(final Annotation note) {
      final HttpPost request = new HttpPost("/" + index + "/" +
          annotation_type + "/" + note.getStartTime() +
          note != null ? note.getTSUID() : "" + "?replication=async");
      try {
        request.setEntity(new StringEntity(JSON.serializeToString(note)));
      } catch (UnsupportedEncodingException e) {
        LOG.error("Encoding Error", e);
      }
      final String response = execute(request);
      if (response == null) {
        LOG.trace("Successfully indexed annotation: " + note);
      } else {
        LOG.error("Failed to index annotation: " + note + " with error: " +
            response);
      }
    }

    /**
     * Deletes an annotation document from the search servers
     * @param note The annotation to delete
     */
    private void deleteAnnotation(final Annotation note) {
      final HttpDelete request = new HttpDelete("/" + index + "/" +
          annotation_type + "/" +  note.getStartTime() +
          note != null ? note.getTSUID() : "" + "?replication=async");
      final String response = execute(request);
      if (response == null) {
        LOG.trace("Successfully deleted annotation: " + note);
      } else {
        LOG.error("Failed to delete annotation: " + note + " with error: " +
            response);
      }
    }

    /**
     * Executes an HTTP request against the ES servers and returns an error
     * string if the request fails.
     * TODO need to parse the output, for now we're just dumping the full JSON
     * response
     * @param request The request to execute
     * @return null if the request was successful, false if there was an error
     */
    private String execute(final HttpRequest request) {
      try {
        final HttpContext context = new BasicHttpContext();
        HttpResponse response = httpClient.execute(hosts, request, context);
        HttpEntity entity = response.getEntity();
        if (response.getStatusLine().getStatusCode() == 200 ||
            response.getStatusLine().getStatusCode() == 201){
          if (entity != null) {
            EntityUtils.consume(entity);
          }
          return null;
        } else {
          String error = "[" + response.getStatusLine().getStatusCode() + "] ";
          if (entity != null) {
            error += EntityUtils.toString(entity);
            EntityUtils.consume(entity);
          } else {
            error += "Unknown error occurred";
          }
          return error;
        }
      } catch (ClientProtocolException e) {
        LOG.error("Protocol Error", e);
      } catch (IOException e) {
        LOG.error("Communications Error", e);
      }
      return "An exception was thrown";
    }
  }

  final class SearchCB implements FutureCallback<Content> {

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
    public void completed(final Content content) {

      final JsonParser jp = JSON.parseToStream(content.asStream());
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
    }

    @Override
    public void failed(final Exception e) {
      LOG.error("Query failed", e);
      throw new RuntimeException(e);
    }
  }
}
