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

import java.util.Map;
import java.util.TreeMap;

import com.google.common.base.Joiner;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * Provides static helper function to generate UID hash
 * and namt string from MetaEvent
 */
public class UID {
  //private static LongHashFunction hash = LongHashFunction.xx_r39();
  private static HashFunction HASH_FUNCTION = Hashing.murmur3_128();

  /**
   * Generate MD5 Hash UID 
   * @param nat String form of MetaEvent, Use generateNAMTfromEvent to get the string
   * @return returns md5 btye array 
   */
  public static long generateUID(String nat) {
    return HASH_FUNCTION.newHasher().putBytes(nat.getBytes()).hash().asLong();
  }
  
  /**
   * Helper function to parse the MetaEvent and return namt string
   * Used for regular index
   * @param event MetaEvent
   * @return namt string form of namt
   */
  public static String generateNAMTfromEvent(MetaEvent event) {
    Map<String, String> tagsMap = new TreeMap<String, String>();
    tagsMap.putAll(event.getTags());
    try {
      Joiner.MapJoiner joiner = Joiner.on(" ").withKeyValueSeparator("=");
      StringBuilder namt = new StringBuilder();
      namt.append(event.getNamespace());
      namt.append(".");
      namt.append(event.getApplication());
      namt.append(".");
      for(int i=0; i<event.getMetric().length;i++) {
      
        namt.append(event.getMetric()[i]);
      
        namt.append(".");     
      
      }
      namt.append(joiner.join(tagsMap));
      return namt.toString();
    } catch (NullPointerException e) {
      throw e;
    }
  }
  
  /**
   * Returns namespace.applicaiton.[metric1..metricn].tagk1.tagk2 from MetaEvent,
   * Used for tagkeys index
   * @param event MetaEvent with all metrics as an array
   * @return namespace.applicaiton.[metric1..metricn].[tagk1,tagk2]
   */
  public static String generateNATKeysfromEvent(MetaEvent event) {
    Map<String, String> tagsMap = new TreeMap<String, String>();
    tagsMap.putAll(event.getTags());
    try {
      StringBuilder namt = new StringBuilder();
      namt.append(event.getNamespace());
      namt.append(".");
      namt.append(event.getApplication());
      namt.append(".");
      namt.append(tagsMap.keySet());
      return namt.toString();
    } catch (NullPointerException e) {
      throw e;
    }
  }
  
  /**
   * Returns namespace.application.metric from MetaEvent , used for AM index as key
   * @param event MetaEvent
   * @param metric Single metric in the event
   * @return namespace.application.metric
   */
  public static String generateNAMfromEvent(MetaEvent event, String metric) {

    try {
      StringBuilder namt = new StringBuilder();
      namt.append(event.getNamespace());
      namt.append(".");
      namt.append(event.getApplication());
      namt.append(".");     
      namt.append(metric);    
      return namt.toString();
    } catch (NullPointerException e) {
      throw e;
    }
  }
  
 /**
  * Returns namespace.application.tagk=tagv from MetaEvent
  * @param event MetaEvent
  * @return namespace.application.tagk=tagv
  */
  public static String generateNATfromEvent(MetaEvent event) {
    Map<String, String> tagsMap = new TreeMap<String, String>();
    tagsMap.putAll(event.getTags());
    try {
      Joiner.MapJoiner joiner = Joiner.on(" ").withKeyValueSeparator("=");
      StringBuilder namt = new StringBuilder();
      namt.append(event.getNamespace());
      namt.append(".");
      namt.append(event.getApplication());
      namt.append(".");  
      namt.append(joiner.join(tagsMap));
      return namt.toString();
    } catch (NullPointerException e) {
      throw e;
    }
  }

}
