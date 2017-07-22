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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.search.ESPluginConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import javax.xml.bind.DatatypeConverter;

/**
 * 
 */
public class ElasticsearchWriter implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchWriter.class);
  private TransportClient client;
  public final static LinkedBlockingQueue<MetaEvent> blockingQueueNAMT = new LinkedBlockingQueue<MetaEvent>(40000);
  private ConcurrentHashMap<String, MetaEvent> inFlightMap; 
  private boolean threadStatus;
  private long startTime = System.currentTimeMillis();
  public static final AtomicLong writesToEs = new AtomicLong();
  public static final AtomicLong successResponsesFromEs = new AtomicLong();
  public static final AtomicLong failedResponsesFromEs = new AtomicLong();
  private long lastIndexedTime = System.currentTimeMillis();
  BulkRequestBuilder bulkRequest;
  private final int threadId;
  int count = 0;
  // private String index;
  private static Set<String> indicesExists = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
  private int bulkRequestSize;
  private TagKeyAMWriter tagKeyAMWriter;
  private ESPluginConfig metaconf;

//  private HashMap<String, CounterMetric> writesToESCounter = new HashMap<>();
//  private HashMap<String, CounterMetric> failedResponseFromES = new HashMap<>();
//  private HashMap<String, CounterMetric> successResponseFromES = new HashMap<>();
//  private HashMap<String, CounterMetric> writesToKafkaMeta = new HashMap<>();
//  private HashMap<String, CounterMetric> droppedMetaFromES = new HashMap<>();
  private static ElasticsearchWriter esWriter = null;


//  void initializeYamasMetric(String namespace) {
//
//    if (writesToESCounter.containsKey(namespace)) {
//      return;
//    }
//    YamasMetrics client =
//        YamasMetrics.getInstance(Arrays.asList("ElasticSearchTid", "namespace", "Type"),
//            Arrays.asList(String.valueOf(threadId), namespace, "namt"));
//    CounterMetric counterMetric1 = client.getMetricRegistry().registerCounter("writesToESCounter");
//    CounterMetric counterMetric2 = client.getMetricRegistry().registerCounter("failedResponseFromES");
//    CounterMetric counterMetric3 = client.getMetricRegistry().registerCounter("successResponseFromES");
//    CounterMetric counterMetric4 = client.getMetricRegistry().registerCounter("writesToKafkaMeta");
//    CounterMetric counterMetric5 = client.getMetricRegistry().registerCounter("droppedMetaFromES");
//    writesToESCounter.put(namespace, counterMetric1);
//    failedResponseFromES.put(namespace, counterMetric2);
//    successResponseFromES.put(namespace, counterMetric3);
//    writesToKafkaMeta.put(namespace, counterMetric4);
//    droppedMetaFromES.put(namespace, counterMetric5);
//  }


  /**
   * Does the basic needed things for ES.
   * @param taskIndex taskIndex storm
   * @param threadId threadId of es writer
   * @param metaConf Metadata Store config
   * @param bulkSize bulk size to write to es
   */
  public ElasticsearchWriter(int taskIndex, int threadId, ESPluginConfig metaConf, int bulkSize) {
    LOG.info("In ES writer const");
    this.metaconf = metaConf;
    this.threadId = threadId;
    inFlightMap = new ConcurrentHashMap<String, MetaEvent>();
    if (System.getProperties().get("isLocalMode") == null) {
      this.client = getNewEsClient();
    
    InputStream inputReader =  this.getClass().getClassLoader().getResourceAsStream("namespace.json");
    BufferedReader reader =new BufferedReader(new InputStreamReader(inputReader));
    StringBuilder builder = new StringBuilder();
    String line;
    try {
      while ((line = reader.readLine()) != null) {
          builder.append(line);
      }
    } catch (IOException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    final String namtMap = builder.toString();

    try {
      client.admin().indices().prepareCreate("all_namespace")
      .setSettings(getIndexSettings("all_namespace"))
      .addMapping("namt", namtMap.toString()).execute().actionGet();
    } catch (ElasticsearchException e) {
     LOG.error("Error creating all namespace index");
    } catch (IOException e) {
      LOG.error("Error creating all namespace index");
    }
    client.admin().cluster().prepareHealth()
      .setWaitForYellowStatus().execute().actionGet();
    bulkRequestSize = bulkSize;
    }
    tagKeyAMWriter = new TagKeyAMWriter();
  }

  /**
   * Create and get the client for Elastic Search
   * @return TransportClient Elastic Search transport client
   */
  public TransportClient getNewEsClient() {
    LOG.info("Cluster name is " + metaconf.getString("es.cluster_name"));
    LOG.info("ES nodes is " + metaconf.getString("es.nodes"));

    Settings settings = Settings.builder()
        .put("cluster.name", metaconf.getString("es.cluster_name"))
        .put("transport.netty.workerCount", "12")
        .put("client.transport.ping_timeout", "60s")
        .put("client.transport.nodes_sampler_interval","30s").build();
    
    client = new PreBuiltTransportClient(settings);
    client.threadPool();
    String[] HOSTS = metaconf.getString("es.nodes").split(",");

    for (String host : HOSTS) {
      try {
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), 9300));
      } catch (UnknownHostException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    
    bulkRequest = client.prepareBulk();
    return client;
  }
  
  public TransportClient getEsClient() {
    return client;
  }

 

  /**
   * Handles the response from the bulk response
   * @param item Each bulk response item
   */
  private void handleResponse(BulkItemResponse item) {
    // For each failed item, put it back in our blocking queue and remove it from inFlightMap in all cases
    if (item.isFailed()) {
      LOG.debug("Failed item == " +item.getFailureMessage());
      failedResponsesFromEs.incrementAndGet();
      handleFailedNAMTResponse(item.getId()); 

    } 
     else { 
      // For each success item, remove the item from our inflight map
      if (inFlightMap.containsKey(item.getId())) {

        MetaEvent event = inFlightMap.get(item.getId());
        String namt = UID.generateNATfromEvent(event);
        //writesToKafkaMeta.get(event.getNamespace()).incr();
      } 

      count++;
      successResponsesFromEs.incrementAndGet();
      if (inFlightMap.containsKey(item.getId())) {
        inFlightMap.remove(item.getId());
      }
    }
  }

  private void handleFailedNAMTResponse(String id) {
    try {
      LOG.debug("Adding " +inFlightMap.get(id)+ " back into queue of type namt"); 
      if (inFlightMap.containsKey(id)) {
        //failedResponseFromES.get(inFlightMap.get(id).getNamespace()).incr();
        boolean timedOut = blockingQueueNAMT.offer(inFlightMap.get(id));
//        LOG.info("Sleeping for 3 seconds possible because of a bulk failure from es");
//        Thread.sleep(3000);
        if (!timedOut) {
          LOG.info("Dropping meta " + inFlightMap.get(id) + " because there is not space in our blocking queue of type namt");
          //droppedMetaFromES.get(inFlightMap.get(id).getNamespace()).incr();
        }
      }
    } catch (Throwable e) {
      LOG.error("Exception in sleeping", e);
    } finally {
      inFlightMap.remove(id);
    }
  }

  public boolean isRunning() {
    return threadStatus;
  }

  public void stopRunning() {
    threadStatus = false;
  }

  /**
   * Adds metaevent to blocking queue and es bulk processor will read
   * @param metaEvent MetaEvent
   */
  public static void queue(MetaEvent metaEvent) {
    try {

      if (!isBlacklisted(metaEvent)) {
        
        blockingQueueNAMT.put(metaEvent);
      }
    } catch (InterruptedException e) {
      LOG.error("Elastic Search failed putting in blocking queue", e);
    }
    //timer();
  }

  public static Boolean isBlacklisted(MetaEvent metaEvent) {
//    if (BlackListCache.Blacklist != null) {
//      for (Pattern p:BlackListCache.BlacklistPattern) {
//        if (p.matcher(metaEvent.getNamespace()+"."+metaEvent.getApplication()).matches()) {
//          return true;
//        }    
//      }
//    }
    return false;
  }




  /**
   * Adds the metaevent to inflight map to elastic search
   * elements will be removed from here once a success a response 
   * is receied from ES
   * @param key n
   * @param event MetaEvent
   */
  public void putInFlightMap(String key, MetaEvent event) {
    inFlightMap.put(key, event);
  }

  public int sizeofInFlightMap() {
    return inFlightMap.size();
  }

  /**
   * Checks if a index exists in Elastic Search
   * @param index
   * @return
   */
  private boolean indexExists(String index) {
    client.admin().cluster().prepareHealth() // Doing this because we want to make sure that cluster is yellow 
    // before we search for a index
    .setWaitForYellowStatus().execute().actionGet();
    return client.admin().indices().prepareExists(index).execute().actionGet().isExists();     
  }

  public Settings getIndexSettings(String index) throws IOException {
    // TODO - cwl
    int shards = 0;//Integer.parseInt(metaconf.getNumShards(index));
  
    if (index.endsWith("_am")) {
      shards = 1;
    }
    Settings indexSettings;
      indexSettings = Settings.builder()
          .put("number_of_shards", shards)
          .put("number_of_replicas", 1)
//          .loadFromSource( XContentFactory.jsonBuilder()
//              .startObject()
//              .startObject("analysis")
//              .startObject("filter")
//              .startObject("ngram_filter")
//              .field("type", "nGram")
//              .field("min_gram", 2) // we need it from be 3 atleast for looking up something like "match": cpu
//              .field("max_gram" , 8) // do we really need 8?
//              .endObject()
//              .endObject()
//              .startObject("analyzer")
//              .startObject("ngram_filter_analyzer")
//              .field("type", "custom")
//              .field("tokenizer", "keyword")
//              .field("filter", new String[]{"ngram_filter", "lowercase"})
//              .endObject()
//              .endObject()
//              .startObject("analyzer")
//              .startObject("lowercase_analyzer_keyword")
//              .field("tokenizer", "keyword")
//              .field("filter", "lowercase")
//              .endObject()
//              .endObject()
//              .endObject()
//              .endObject().string())
          .build();
    return indexSettings;
    }
  /**
   * Creates a new index in Elastic Search
   * @param index
   */
  private void createIndex(String index) throws ElasticsearchException, IOException {
   
    
//      JsonParser parser = new JsonParser();
//      InputStream  inputReader =  this.getClass().getClassLoader().getResourceAsStream("namtMap.json");
//      BufferedReader reader =new BufferedReader(new InputStreamReader(inputReader));
//
//      Object obj_namt = parser.parse(reader);
//      JsonObject namtMap = (JsonObject) obj_namt;
//
//      client.admin().indices().prepareCreate(index).setSettings(getIndexSettings(index)).addMapping("namt", namtMap.toString()).execute().actionGet();
//      client.admin().cluster().prepareHealth()
//      .setWaitForYellowStatus().execute().actionGet();
      // PUT namespace mapping

    
  }
  
  public void createNATIndex(String index) throws ElasticsearchException, IOException {
//    JsonParser parser = new JsonParser();
//    InputStream inputReader =  this.getClass().getClassLoader().getResourceAsStream("nat.json");
//    BufferedReader reader =new BufferedReader(new InputStreamReader(inputReader));
//
//    Object obj_namt = parser.parse(reader);
//    JsonObject namtMap = (JsonObject) obj_namt;
//    client.admin().indices().prepareCreate(index).setSettings(getIndexSettings(index)).addMapping("namt", namtMap.toString()).execute().actionGet();
//    client.admin().cluster().prepareHealth()
//    .setWaitForYellowStatus().execute().actionGet();
  }
  
  
  public void createAMIndex(String index) throws ElasticsearchException, IOException {
//    JsonParser parser = new JsonParser();
//    InputStream inputReader =  this.getClass().getClassLoader().getResourceAsStream("nam.json");
//    BufferedReader reader =new BufferedReader(new InputStreamReader(inputReader));
//
//    Object obj_namt = parser.parse(reader);
//    JsonObject namtMap = (JsonObject) obj_namt;
//    client.admin().indices().prepareCreate(index).setSettings(getIndexSettings(index)).addMapping("namt", namtMap.toString()).execute().actionGet();
//    client.admin().cluster().prepareHealth()
//    .setWaitForYellowStatus().execute().actionGet();
  }

  public Boolean checkAndCreateIndex(String index) {
    try {
    if (!indicesExists.contains(index))  {
      if (indexExists(index)) {
        indicesExists.add(index);
      } else {
        createIndex(index);
        indicesExists.add(index);
      }
    }
    
    if (!indicesExists.contains(index+"_tagkeys"))  {
      if (indexExists(index+"_tagkeys")) {
        indicesExists.add(index+"_tagkeys");
      } else {
        createNATIndex(index+"_tagkeys");
        indicesExists.add(index+"_tagkeys");
      }
    }
    
    if (!indicesExists.contains(index+"_am"))  {
      if (indexExists(index+"_am")) {
        indicesExists.add(index+"_am");
      } else {
        createAMIndex(index+"_am");
        indicesExists.add(index+"_am");
      }
    }
    return true;
    } catch (Exception e) {
      LOG.error("Error creating new index" , e);
      return false;
    }
  }

  @Override
  public void run() {
    long threadId = Thread.currentThread().getId();
    Thread.currentThread().setName("ElasticSearchWriter-"+threadId);
    LOG.info("Elastic search writer thread started");
    while (true) { // catch throwable 
      try {
        processBulk();
        MetaEvent metaEvent = blockingQueueNAMT.take();
        
        // Create index if it does not exist.
        String index = metaEvent.getNamespace().toLowerCase();
    
        Boolean indexExists = checkAndCreateIndex(index);
        
        //initializeYamasMetric(metaEvent.getNamespace());
        //writesToESCounter.get(metaEvent.getNamespace()).incr();
        if (indexExists) {
          addNamtDoc(metaEvent, bulkRequest);
        }

      } catch (InterruptedException e) {
        LOG.error("Error taking the event from the queue", e);
      } catch (NoNodeAvailableException ex) {
        LOG.error("Node not available, creating client again", ex);
        client.close();
        try {
          Thread.sleep(60 * 1000);
        } catch (InterruptedException e) {
          LOG.error("Error sleeping thread", e);
        }
        getNewEsClient();

      } catch (Throwable th) {
        LOG.error("Unknown exception in ElasticSearchWriter", th);
      }
    }
  }

  /**
   * Add a namt doc to the elastic search bulk processing queue
   * @param metaEvent Event that needs to be sent to ES
   */
  public void addNamtDoc(MetaEvent metaEvent, BulkRequestBuilder bulkRequest) {
    Boolean newNamespace = ESNamespace.namespace.add(metaEvent.getNamespace());
    if (newNamespace) {
      ESNamespace.addNamespaceDoc(metaEvent.getNamespace(), bulkRequest);
    }
    
    tagKeyAMWriter.addNATDoc(metaEvent, bulkRequest);
    tagKeyAMWriter.addNAMDoc(metaEvent, bulkRequest);
    String uid = Long.toString(UID.generateUID(UID.generateNATfromEvent(metaEvent)));
    HashMap<String, Object> namtMap = new HashMap<String, Object>();

    namtMap.put("application.raw", metaEvent.getApplication());
    namtMap.put("application.lowercase", metaEvent.getApplication().toLowerCase());

    namtMap.put("AM_nested", metaEvent.getAMNested());

    namtMap.put("tags_value", metaEvent.getTagsNested().size());

    namtMap.put("tags", metaEvent.getTagsNested());

    namtMap.put("timestamp", System.currentTimeMillis());
    namtMap.put("firstSeenTime", metaEvent.getFirstSeenTime()/1000);
    namtMap.put("lastSeenTime", metaEvent.getLastSeenTime()/1000);
    
    IndexRequest namtRequest = new IndexRequest(metaEvent.getNamespace().toLowerCase(), "namt", uid).source(namtMap);
    UpdateRequest namtUpdateReq;

      namtUpdateReq = new UpdateRequest(metaEvent.getNamespace().toLowerCase(), "namt", uid).doc(namtMap)
          .upsert(namtRequest).retryOnConflict(10);
    inFlightMap.put(namtUpdateReq.id(), metaEvent);
    bulkRequest.add(namtUpdateReq);
  }

  public String[] convertAllToLowerCase(String[] s) {
    String[] lowercase = new String[s.length];
    for (int i = 0; i<s.length; i++) {
      lowercase[i] = s[i].toLowerCase();
    }
    return lowercase;
  }

  /**
   * Processes the bulk request when the bulk processing queue
   * reaches the limits set
   */
  public void processBulk() {
    try {
      if (bulkRequest.numberOfActions() >= bulkRequestSize 
          || (System.currentTimeMillis() - lastIndexedTime) >= 300000
          ) {
        lastIndexedTime = System.currentTimeMillis();
        LOG.debug("Sending bulk request to Elastic Search from thread " + threadId + " at " + System.currentTimeMillis());
        
        bulkRequest.execute().addListener(new ActionListener<BulkResponse>() {
          
          @Override
          public void onResponse(BulkResponse response) {
            LOG.debug("Time taken for the bulk request to process " +response.getTookInMillis() + " and queue size " +blockingQueueNAMT.size() + 
            " from thread " + threadId);
            for (BulkItemResponse item : response.getItems()) {
              handleResponse(item);
            }
        
          }
          
          @Override
          public void onFailure(Exception e) {
            LOG.error("Failure in bulk request ", e);
          }
        });
      
        LOG.debug("Done sending bulk request to Elastic Search from thread " + threadId + " at " + System.currentTimeMillis());

        
        LOG.debug("Preparing Client again for thread " + threadId);
        bulkRequest = client.prepareBulk();
        LOG.debug("Done Preparing Client again for thread " + threadId);

        LOG.debug("Elastic search removed " +count+ " and inflightmap is " +inFlightMap.size());
        count = 0;
      } 
    } catch (ElasticsearchException e) {
      LOG.error("Exception in processing bulk request", e);
    }
  }


  /**
   * List of all indices in Elastic Search
   * @return list of all indices created in elastic search
   */
  public Set<String> allIndexes() {
    return indicesExists;
  }

  
//  public static void main(String[] args) {
//    MetaSystemConfig metaConf = new MetaSystemConfig("/tmp/es-bf1.conf");
//    ElasticsearchWriter writer = new ElasticsearchWriter(1, 1, metaConf, 1);
//    String[] metric = {"metric1", "metric2", "metric3"};
//    Map<String, String> tags = new HashMap<>();
//    tags.put("tagk1", "tagv1");
//    tags.put("tagk2", "tagv2");
//    tags.put("tagk3", "tagv3");
//    
//    MetaEvent metaEvent = new MetaEvent("sid_test", "app2", metric, 1200000000L, 1200000000L, tags, 1, false);
//    
//    writer.addNamtDoc(metaEvent);
//    writer.processBulk();
//    System.out.println("Sent app2");
//  }
}
