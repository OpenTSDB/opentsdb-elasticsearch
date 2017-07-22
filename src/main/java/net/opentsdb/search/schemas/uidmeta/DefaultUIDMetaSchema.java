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
package net.opentsdb.search.schemas.uidmeta;

import net.opentsdb.search.ElasticSearch;

/**
 * Default implementation of the {@link UIDMetaSchema}.
 */
public class DefaultUIDMetaSchema extends UIDMetaSchema {

  /**
   * Default ctor.
   * @param es The plugin this schema belongs to.
   * @throws IllegalArgumentException if 'tsd.search.elasticsearch.uidmeta_type'
   * was null or empty.
   */
  public DefaultUIDMetaSchema(final ElasticSearch es) {
    super(es);
  }

}
