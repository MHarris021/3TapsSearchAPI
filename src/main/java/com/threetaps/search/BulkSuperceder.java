package com.threetaps.search;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.OrFilterBuilder;
import org.elasticsearch.search.SearchHit;


public class BulkSuperceder extends AbstractIndexer {
	private static final Logger log = Logger.getLogger(BulkSuperceder.class);
	
	private Map<UniqueId, String> supercedes;
	private SourceMap sourceMap;
	
	public BulkSuperceder(Client client) {
		super(client);
		supercedes = new HashMap<UniqueId, String>();
		sourceMap = new SourceMap();
	}

	public void add(Map<String, Object> post) {
		String source = post.get("source").toString();
		String externalID = post.get("external_id").toString();
		supercedes.put(new UniqueId(source, externalID), post.get("id").toString());
		sourceMap.add(source, externalID);
	}
	
	public boolean isEmpty() {
		return supercedes.isEmpty();
	}
	
	public void process() throws SearchException, UpdateException, BulkIndexException {
		if (log.isDebugEnabled())
			log.debug(String.format("dealing with %d reposts", supercedes.size()));
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		Set<String> new_ids = new HashSet<String>(supercedes.values());
		for (Map.Entry<String, Set<String>> source2externalID: sourceMap.entrySet()) {
			OrFilterBuilder idFilter = FilterBuilders.orFilter();
			for (String externalID: source2externalID.getValue()) {
				idFilter.add(FilterBuilders.termFilter("external_id", externalID));
			}
			if (log.isTraceEnabled())
				log.trace(String.format("searching for %d external ids in %s", source2externalID.getValue().size(), source2externalID.getKey()));
			try {
				SearchResponse response = client.prepareSearch()
						.setTypes("post")
						.setFilter(FilterBuilders.andFilter()
								.add(FilterBuilders.missingFilter("repost_id"))
								.add(FilterBuilders.termFilter("source", source2externalID.getKey()))
								.add(idFilter))
						.setSize(10000)
						.execute().actionGet();
				if (log.isTraceEnabled())
					log.trace(String.format("found %d non-reposts matching those ids", response.getHits().totalHits()));
				for (SearchHit hit: response.hits()) {
					if (new_ids.contains(hit.id()))
						continue;
					Map<String, Object> post;
					if (hit.isSourceEmpty()) {
						post = client.prepareGet(hit.index().replace("mem", "disk"), hit.type(), hit.id()).execute().actionGet().getSource();
					} else {
						post = hit.getSource();
					}
					String repostID = supercedes.get(new UniqueId(post));
//					if (hit.id().equals(repostID))
//						continue;
					post.put("repost_id", repostID);
					bulkRequest.add(client.prepareIndex(hit.index(), hit.type(), hit.id()).setSource(post));
				}
			} catch (ElasticSearchException e) {
				throw new SearchException("error searching for superceded posts", e);
			}
		}
		if (log.isTraceEnabled())
			log.trace(String.format("reindexing %d superceded posts", bulkRequest.request().requests().size()));
		if (!bulkRequest.request().requests().isEmpty()) {
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
				throw new UpdateException("error updating superceded posts", e);
			}
		}
		supercedes.clear();
		sourceMap.clear();
	}
	
	private static class SourceMap extends HashMap<String, Set<String>> {
		private static final long serialVersionUID = 1L;

		public void add(String source, String externalID) {
			if (!containsKey(source))
				put(source, new HashSet<String>());
			get(source).add(externalID);
		}
	}
}