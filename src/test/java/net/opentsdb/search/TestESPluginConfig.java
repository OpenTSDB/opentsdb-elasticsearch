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

import org.junit.Test;

import net.opentsdb.utils.Config;

public class TestESPluginConfig {

  @Test
  public void defaults() throws Exception {
    final ESPluginConfig config = new ESPluginConfig(new Config(false));
    
    assertEquals("", config.getString("tsd.search.elasticsearch.hosts"));
    assertEquals(1, config.getInt("tsd.search.elasticsearch.index_threads"));
    assertEquals("opentsdb", config.getString("tsd.search.elasticsearch.index"));
    assertEquals("tsmeta", config.getString("tsd.search.elasticsearch.tsmeta_type"));
    assertEquals("uidmeta", config.getString("tsd.search.elasticsearch.uidmeta_type"));
    assertEquals("annotation", config.getString("tsd.search.elasticsearch.annotation_type"));
    assertEquals(25, config.getInt("tsd.search.elasticsearch.pool.max_per_route"));
    assertEquals(50, config.getInt("tsd.search.elasticsearch.pool.max_total"));
    
  }
}
