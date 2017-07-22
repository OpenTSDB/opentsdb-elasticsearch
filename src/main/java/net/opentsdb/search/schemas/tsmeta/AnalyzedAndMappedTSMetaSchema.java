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

import java.io.IOException;
import java.util.concurrent.CancellationException;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Deferred;

import net.opentsdb.meta.TSMeta;
import net.opentsdb.search.ElasticSearch;

/**
 * Same as {@link DefaultTSMetaSchema} except it extends the information with 
 * a map of tag/key values and analyzes in lower case using {@link TSMetaAugment}.
 * This will send a JSON object similar to:
 * <p>
 * <pre>
 * {
  "tsuid": "010101",
  "metric": {
    "uid": "01",
    "type": "METRIC",
    "name": "sys.cpu.user",
    "description": "",
    "notes": "",
    "created": 1500686742,
    "custom": null,
    "displayName": ""
  },
  "tags": [{
    "uid": "01",
    "type": "TAGK",
    "name": "host",
    "description": "",
    "notes": "",
    "created": 1500686742,
    "custom": null,
    "displayName": ""
  }, {
    "uid": "01",
    "type": "TAGV",
    "name": "web01",
    "description": "",
    "notes": "",
    "created": 1500686742,
    "custom": null,
    "displayName": ""
  }],
  "description": "",
  "notes": "",
  "created": 0,
  "units": "",
  "retention": 0,
  "max": "NaN",
  "min": "NaN",
  "displayName": "Testing",
  "dataType": "",
  "lastReceived": 0,
  "totalDatapoints": 0,
  "keys": [{
    "uid": "01",
    "type": "TAGK",
    "name": "host",
    "description": "",
    "notes": "",
    "created": 1500686742,
    "custom": null,
    "displayName": ""
  }],
  "values": [{
    "uid": "01",
    "type": "TAGV",
    "name": "web01",
    "description": "",
    "notes": "",
    "created": 1500686742,
    "custom": null,
    "displayName": ""
  }],
  "kv_map": ["TAGK_01=TAGV_01"],
  "kv_map_name": ["TAGK_host=TAGV_web01"]
}
 * </pre>
 */
public class AnalyzedAndMappedTSMetaSchema extends TSMetaSchema {
  private static final Logger LOG = LoggerFactory.getLogger(
      AnalyzedAndMappedTSMetaSchema.class);

  /**
   * Default ctor.
   * @param es The plugin this schema belongs to.
   * @throws IllegalArgumentException if 'tsd.search.elasticsearch.tsmeta_type'
   * was null or empty.
   */
  public AnalyzedAndMappedTSMetaSchema(final ElasticSearch es) {
    super(es);
  }

  @Override
  public Deferred<Object> index(final TSMeta meta) {
    if (meta == null) {
      return Deferred.fromError(new IllegalArgumentException(
          "Meta cannot be null."));
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
      .append(meta.getTSUID());
    if (es.asyncReplication()) {
      uri.append("?replication=async");
    }
    
    final HttpPost post = new HttpPost(uri.toString());
    post.setEntity(new ByteArrayEntity(TSMetaAugment.serializeToBytes(meta)));
    es.httpClient().execute(post, new AsyncCB());
    return result;
  }
}
