package com.threetaps.search;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;

public class BulkIndexer extends AbstractIndexer {
	private static final Logger log = Logger.getLogger(BulkIndexer.class);
	
	boolean dryRun = false;
	
	public BulkIndexer(Client client) {
		super(client);
	}
	
	public void index(List<Map<String, Object>> docs, String type, IndexAssigner assigner) 
			throws ElasticSearchException, BulkIndexException {
		log.trace("creating bulk index request");
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		int n=0;
		for (Map<String, Object> doc: docs) {
			String id = String.valueOf(doc.get("id"));
			for (String index: assigner.getIndices(doc)) {
				if (dryRun) {
					log.info(String.format("adding %s/%s to bulk index request", index, id));
				} else {
					bulkRequest.add(client.prepareIndex(index, type, id).setSource(doc));
				}
				++n;
			}
		}
		if (n>0) {
			if (log.isTraceEnabled())
				log.trace(String.format("executing bulk index request with %d operations", n));
			if (dryRun) {
				log.info(String.format("executing bulk index request with %d operations", n));
			} else {
				BulkResponse bulkResponse = bulkRequest.execute().actionGet();
				if (bulkResponse.hasFailures()) {
					BulkIndexException e = new BulkIndexException();
									try { 
										new org.codehaus.jackson.map.ObjectMapper().writeValue(new java.io.FileOutputStream("/tmp/docs.json"), docs);
									} catch (Exception x) {
											x.printStackTrace();
									}
					for (BulkItemResponse item: bulkResponse.items()) {
						if (item.failed()) {
//							for (Map<String, Object> doc: docs) {
//								String id = String.valueOf(doc.get("id"));
//								if (id == item.getFailure().getId()) {
//								}
//							}
							e.failures.add(item.getFailure());
						}
					}
					throw e;
				}
			}
		}
	}
	
	public static interface IndexAssigner {
		public Collection<String> getIndices(Map<String, Object> doc);
	}
}
