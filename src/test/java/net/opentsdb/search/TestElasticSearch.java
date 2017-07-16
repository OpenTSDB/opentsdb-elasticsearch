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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

import net.opentsdb.core.TSDB;
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
  
  @Before
  public void before() throws Exception {
    tsdb = PowerMockito.mock(TSDB.class);
    config = new Config(false);
    connection_manager = mock(PoolingNHttpClientConnectionManager.class);
    client_builder = mock(HttpAsyncClientBuilder.class);
    client = mock(CloseableHttpAsyncClient.class);
    
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
}
