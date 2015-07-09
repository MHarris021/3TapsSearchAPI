package com.threetaps.search;

import java.util.Map;

import org.apache.log4j.Logger;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHit;

public class BulkUpdater extends AbstractIndexer {
	private static final Logger log = Logger.getLogger(BulkUpdater.class);
	
	public BulkUpdater(Client client) {
		super(client);
	}

	public void update(DocumentFinder finder, DocumentUpdater updater) throws UpdateException, BulkIndexException {
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		try {
			for (SearchHit hit: finder.findDocuments(client)) {
				String index = hit.index();
				try {
					Map<String, Object> post;
					if (hit.isSourceEmpty()) {
						post = client.prepareGet(hit.index().replace("mem", "disk"), hit.type(), hit.id()).execute().actionGet().getSource();
					} else {
						post = hit.getSource();
					}
					Map<String, Object> updated = updater.update(post);
					bulkRequest.add(client.prepareIndex(index, "post", hit.id()).setSource(updated));
				} catch (UpdateException e) {
					log.error(String.format("failed to update document %s", hit.id()), e);
				} catch (ElasticSearchException e) {
					log.error(String.format("error fetching document %s from disk", hit.id()), e);
				}
			}
		} catch (ElasticSearchException e) {
			throw new UpdateException("error searching for updated documents", e);
		}
		try {
			BulkResponse bulkResponse = bulkRequest.execute().actionGet();
			if (bulkResponse.hasFailures()) {
				BulkIndexException e = new BulkIndexException();
				for (BulkItemResponse item: bulkResponse.items()) {
					if (item.failed()) {
						e.failures.add(item.getFailure());
					}
				}
				throw e;
			}
		} catch (ElasticSearchException e) {
			throw new UpdateException("error updating documents", e);
		}
	}
	
	public static interface DocumentFinder {
		public Iterable<SearchHit> findDocuments(Client client);
	}

	public static interface DocumentUpdater {
		public Map<String, Object> update(Map<String, Object> doc) throws UpdateException;
	}
}