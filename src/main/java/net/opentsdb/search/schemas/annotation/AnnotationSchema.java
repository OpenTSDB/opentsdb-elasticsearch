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

import java.io.IOException;
import java.util.concurrent.CancellationException;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.util.EntityUtils;
import org.hbase.async.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import net.opentsdb.meta.Annotation;
import net.opentsdb.search.ElasticSearch;
import net.opentsdb.utils.JSON;

public abstract class AnnotationSchema {
  private static final Logger LOG = LoggerFactory.getLogger(
      AnnotationSchema.class);
  
  /** The parent plugin this belongs to. */
  protected final ElasticSearch es;
  
  /** The type of document used for indexing. */
  protected final String doc_type;
  
  /** Counters for stats */
  protected final Counter added_ctr = new Counter();
  protected final Counter deleted_ctr = new Counter();
  protected final Counter errors_ctr = new Counter();
  
  /**
   * Default ctor. All implementations must have this CTOR as we'll call that
   * based on the class given in the config.
   * @param es The plugin this schema belongs to.
   * @throws IllegalArgumentException if 'tsd.search.elasticsearch.annotation_type'
   * was null or empty.
   */
  public AnnotationSchema(final ElasticSearch es) {
    this.es = es;
    doc_type = es.config().getString("tsd.search.elasticsearch.annotation_type");
    if (Strings.isNullOrEmpty(doc_type)) {
      throw new IllegalArgumentException("Missing config "
          + "'tsd.search.elasticsearch.annotation_type'");
    }
  }
  
  /**
   * Sends the data to the Elastic Search index.
   * @param note A non-null annotation object.
   * @return A deferred resolving to <code>true</code> on success or an 
   * exception on failure.
   */
  public Deferred<Object> index(final Annotation note) {
    if (note == null) {
      return Deferred.fromError(new IllegalArgumentException(
          "Annotation cannot be null."));
    }
    final Deferred<Object> result = new Deferred<Object>();
    
    final class AsyncCB implements FutureCallback<HttpResponse> {
      @Override
      public void cancelled() {
        result.callback(new CancellationException("Index call was cancelled."));
        errors_ctr.increment();
      }

      @Override
      public void completed(final HttpResponse content) {
        try {
          if (content.getStatusLine().getStatusCode() < 200 || 
              content.getStatusLine().getStatusCode() > 299) {
            result.callback(new IllegalStateException("Unable to post annotation. "
                + "Status code: " + content.getStatusLine().getStatusCode() 
                + " Content: " + EntityUtils.toString(content.getEntity())));
            errors_ctr.increment();
          } else {
            result.callback(true);
            added_ctr.increment();
          } 
        } catch (Exception e) {
          LOG.error("Unexpected exception parsing content", e);
          result.callback(e);
        } finally {
          try {
            EntityUtils.consume(content.getEntity());
          } catch (IOException e) { }
        }
      }

      @Override
      public void failed(final Exception e) {
        result.callback(e);
        errors_ctr.increment();
      }

    }
    
    final StringBuilder uri = new StringBuilder(es.host())
      .append("/")
      .append(es.index())
      .append("/")
      .append(doc_type)
      .append("/")
      .append(note.getStartTime());
    if (!Strings.isNullOrEmpty(note.getTSUID())) {
      uri.append(note.getTSUID());
    }
    if (es.asyncReplication()) {
      uri.append("?replication=async");
    }
    
    final HttpPost post = new HttpPost(uri.toString());
    post.setEntity(new ByteArrayEntity(JSON.serializeToBytes(note)));
    es.httpClient().execute(post, new AsyncCB());
    return result;
  }
  
  /**
   * Deletes the data in the ES index.
   * @param note A non-null annotation object.
   * @return A deferred resolving to <code>true</code> on success or an 
   * exception on failure.
   */
  public Deferred<Object> delete(final Annotation note) {
    if (note == null) {
      return Deferred.fromError(new IllegalArgumentException(
          "Annotation cannot be null."));
    }
    final Deferred<Object> result = new Deferred<Object>();
    
    final class AsyncCB implements FutureCallback<HttpResponse> {
      @Override
      public void cancelled() {
        result.callback(new CancellationException("Index call was cancelled."));
        errors_ctr.increment();
      }

      @Override
      public void completed(final HttpResponse content) {
        try {
          if (content.getStatusLine().getStatusCode() < 200 || 
              content.getStatusLine().getStatusCode() > 299) {
            result.callback(new IllegalStateException("Unable to post meta data. "
                + "Status code: " + content.getStatusLine().getStatusCode() 
                + " Content: " + EntityUtils.toString(content.getEntity())));
            errors_ctr.increment();
          } else {
            result.callback(true);
            deleted_ctr.increment();
          } 
        } catch (Exception e) {
          LOG.error("Unexpected exception parsing content", e);
          result.callback(e);
        } finally {
          try {
            EntityUtils.consume(content.getEntity());
          } catch (IOException e) { }
        }
      }

      @Override
      public void failed(final Exception e) {
        result.callback(e);
        errors_ctr.increment();
      }

    }
    
    final StringBuilder uri = new StringBuilder(es.host())
      .append("/")
      .append(es.index())
      .append("/")
      .append(doc_type)
      .append("/")
      .append(note.getStartTime());
    if (!Strings.isNullOrEmpty(note.getTSUID())) {
      uri.append(note.getTSUID());
    }
    if (es.asyncReplication()) {
      uri.append("?replication=async");
    }
    
    final HttpDelete delete = new HttpDelete(uri.toString());
    es.httpClient().execute(delete, new AsyncCB());
    return result;
  }
  
  /** @return The doc type configured in 'tsd.search.elasticsearch.annotation_type' */
  public String docType() {
    return doc_type;
  }
  
  /** @return The successfully indexed doc counter value. */
  public long added() {
    return added_ctr.get();
  }
  
  /** @return The successfully deleted doc counter value. */
  public long deleted() {
    return deleted_ctr.get();
  }
  
  /** @return The count of operations throwing exceptions. */
  public long errors() {
    return errors_ctr.get();
  }

}
