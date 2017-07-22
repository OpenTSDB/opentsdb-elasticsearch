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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.JSON;

/**
 * Helper function to add data to the base TSMeta when it is serialized to JSON.
 *
 * This method adds keys, values, kv_map and kv_map_name to the generated JSON
 * allowing for searches to find specific key/values by name/uid and to find
 * TSUIDs with specific key=value.
 *
 * keys: Array of normal UIDMeta objects only containing TAGK
 * values: Array of normal UIDMeta objects only containing TAGV
 * kv_map: Array of strings in the form TAGK_UID=TAGV_UID
 * kv_map_name: Array of strings in the form TAGK_keyname=TAGV_valuename
 *
 * Note: the tags object was not removed, this means keys/values is just
 * duplicate data. For search purposes tags should probably be removed.
 */
public class TSMetaAugment {
  public static final byte[] serializeToBytes(final TSMeta meta) {
    ArrayList<UIDMeta> keys = new ArrayList<UIDMeta>();
    ArrayList<UIDMeta> values = new ArrayList<UIDMeta>();
    ArrayList<String> kv_map = new ArrayList<String>();
    ArrayList<String> kv_map_name = new ArrayList<String>();

    String key = null;
    String key_name = null;
    String value = null;
    String value_name = null;
    for (UIDMeta m : meta.getTags()) {
      if (m.getType() == UniqueIdType.TAGK) {
        keys.add(m);
        key = "TAGK_" + m.getUID();
        key_name = "TAGK_" + m.getName();
      } else if (m.getType() == UniqueIdType.TAGV) {
        values.add(m);
        value = "TAGV_" + m.getUID();
        value_name = "TAGV_" + m.getName();
      }

      if (key != null && value != null) {
        kv_map.add(key + "=" + value);
        kv_map_name.add(key_name + "=" + value_name);
        key_name = value_name = key = value = null;
      }
    }

    ObjectMapper mapper = JSON.getMapper();
    ObjectNode root = (ObjectNode) mapper.valueToTree(meta);

    root.put("keys", mapper.valueToTree(keys));
    root.put("values", mapper.valueToTree(values));
    root.put("kv_map", mapper.valueToTree(kv_map));
    root.put("kv_map_name", mapper.valueToTree(kv_map_name));

    try {
      ByteArrayOutputStream o = new ByteArrayOutputStream();
      mapper.writeTree(mapper.getFactory().createGenerator(o), root);
      return o.toByteArray();
    } catch (IOException e) {
      return new byte[0];
    }
  }
}
