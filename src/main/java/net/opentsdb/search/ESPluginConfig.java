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

import java.util.Map;

import net.opentsdb.utils.Config;

/**
 * Overloads the default Config class with defaults for the ElasticSearch plugin
 */
public final class ESPluginConfig extends Config {

  /**
   * Default constructor copies settings from the TSD
   * @param parent The TSD config to copy
   */
  public ESPluginConfig(final Config parent) {
    super(parent);
  }

  /**
  * Loads default entries that were not provided by a file or command line
  */
  @Override
  protected void setDefaults() {

    default_map.put("tsd.search.elasticsearch.hosts", "");
    default_map.put("tsd.search.elasticsearch.index_threads", "1");
    default_map.put("tsd.search.elasticsearch.index", "opentsdb");
    default_map.put("tsd.search.elasticsearch.tsmeta_type", "tsmeta");
    default_map.put("tsd.search.elasticsearch.uidmeta_type", "uidmeta");
    default_map.put("tsd.search.elasticsearch.annotation_type", "annotation");
    default_map.put("tsd.search.elasticsearch.pool.max_per_route", "25");
    default_map.put("tsd.search.elasticsearch.pool.max_total", "50");
    
    default_map.put("tsd.search.elasticsearch.schema.tsmeta", 
        "net.opentsdb.search.schemas.tsmeta.DefaultTSMetaSchema");
    default_map.put("tsd.search.elasticsearch.schema.uidmeta", 
        "net.opentsdb.search.schemas.uidmeta.DefaultUIDMetaSchema");
    default_map.put("tsd.search.elasticsearch.schema.annotation", 
        "net.opentsdb.search.schemas.annotation.DefaultAnnotationSchema");

    for (Map.Entry<String, String> entry : default_map.entrySet()) {
      if (!properties.containsKey(entry.getKey()))
        properties.put(entry.getKey(), entry.getValue());
    }
  }
}
