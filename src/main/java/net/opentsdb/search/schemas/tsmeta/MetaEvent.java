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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Stores the meta information of the YMSEvent
 */
public class MetaEvent {

  protected String namespace;
  protected String application;
  protected String[] metric;
  protected Long firstSeenTime;
  protected Long lastSeenTime;
  protected Map<String, String> tags = new HashMap<String, String>();
  protected int partition;
  protected Boolean useScript;
  protected Boolean isRefresh = false;

  public MetaEvent() {

  }

  public MetaEvent(String namespace, String application, String[] metric, Long firstSeenTime, Long lastSeenTime, 
      Map<String, String> tags, int partition, Boolean useScript) {
    this.namespace = namespace;
    this.application = application;
    this.metric = metric;
    this.firstSeenTime = firstSeenTime;
    this.lastSeenTime = lastSeenTime;
    this.tags = tags;
    this.partition = partition;
    this.useScript = useScript;
  }

  public String getNamespace() {
    return namespace;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  public String getApplication() {
    return application;
  }

  public void setApplication(String application) {
    this.application = application;
  }

  public Long getFirstSeenTime() {
    return firstSeenTime;
  }
  
  public Long getLastSeenTime() {
    return lastSeenTime;
  }

  public void setFirstSeenTime(Long firstSeenTime) {
    this.firstSeenTime = firstSeenTime;
  }
  
  public void setLastSeenTime(Long lastSeenTime) {
    this.lastSeenTime = lastSeenTime;
  }

  public String[] getMetric() {
    return metric;
  }

  public void setMetric(String[] metric) {
    this.metric = metric;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public void setTags(Map<String, String> tags) {
    this.tags = tags;
  }

  public List<Map<String, String>> getTagsNested() {
    ArrayList<Map<String, String>> entryList = new ArrayList<Map<String, String>>(tags.entrySet().size());
    for (Map.Entry<String, String> entry : tags.entrySet()) {
      HashMap<String, String> keyValuePair = new HashMap<String, String>(4);
      keyValuePair.put("key.raw", entry.getKey());
      keyValuePair.put("key.lowercase", entry.getKey().toLowerCase());
      keyValuePair.put("value.raw", entry.getValue());
      keyValuePair.put("value", entry.getValue().toLowerCase());
      entryList.add(keyValuePair);
    }
    return entryList;
  }
  
  public List<Map<String, String>> getTagsKeysNested() {
    ArrayList<Map<String, String>> entryList = new ArrayList<Map<String, String>>(tags.entrySet().size());
    for (Map.Entry<String, String> entry : tags.entrySet()) {
      HashMap<String, String> keyPair = new HashMap<String, String>(2);
      keyPair.put("key.raw", entry.getKey());
      keyPair.put("key.lowercase", entry.getKey().toLowerCase());
      entryList.add(keyPair);
    }
    return entryList;
  }
  
  public String[] generateAM() {
    String[] AM = new String[metric.length];
    for (int i=0; i< metric.length; i++) {
      AM[i] = application+"."+metric[i];
    }
    return AM;
  }

  public int getPartition() {
    return partition;
  }

  public void setPartition(int partition) {
    this.partition = partition;
  }
  
  public Boolean useScript() {
    return useScript;
  }
  
  public Boolean setRefresh(Boolean refresh) {
    return this.isRefresh = refresh;
  }
  
  public Boolean isRefresh() {
    return isRefresh;
  }

  @Override
  public String toString() {
    return UID.generateNAMTfromEvent(this) + this.getFirstSeenTime() + this.getLastSeenTime(); // returns  n.a.m.t based on the event
  }

  public List<Map<String, Object>> getMetricNested() {
    ArrayList<Map<String, Object>> metricsNested = new ArrayList<Map<String, Object>>(metric.length);
    for (int i = 0; i< metric.length; i++) {
      HashMap<String, Object> keyValuePair = new HashMap<String, Object>(2);
      keyValuePair.put("name", metric[i].toLowerCase());
      keyValuePair.put("name.raw", metric[i]);
      metricsNested.add(keyValuePair);
    }
    return metricsNested;
  }
  
  public List<Map<String, Object>> getMetricNested(String metric) {
    ArrayList<Map<String, Object>> metricsNested = new ArrayList<Map<String, Object>>();
      HashMap<String, Object> keyValuePair = new HashMap<String, Object>(2);
      keyValuePair.put("name", metric.toLowerCase());
      keyValuePair.put("name.raw", metric);
      metricsNested.add(keyValuePair);
    
    return metricsNested;
  }
  
  public List<Map<String, Object>> getAMNested() {
    ArrayList<Map<String, Object>> AMNested = new ArrayList<Map<String, Object>>(metric.length);
    for (int i = 0; i< metric.length; i++) {
      HashMap<String, Object> keyValuePair = new HashMap<String, Object>(2);
      keyValuePair.put("name.lowercase", application.toLowerCase()+ "." +metric[i].toLowerCase());
      keyValuePair.put("name.raw", application + "." + metric[i]);
      AMNested.add(keyValuePair);
    }
    return AMNested;
  }
  
  public List<Map<String, Object>> getAMNested(String metric) {
    ArrayList<Map<String, Object>> AMNested = new ArrayList<Map<String, Object>>();
      HashMap<String, Object> keyValuePair = new HashMap<String, Object>(2);
      keyValuePair.put("name.lowercase", application.toLowerCase()+ "." +metric.toLowerCase());
      keyValuePair.put("name.raw", application + "." + metric);
      AMNested.add(keyValuePair);
    
    return AMNested;
  }
  
}
