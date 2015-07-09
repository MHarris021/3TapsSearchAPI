package com.threetaps.search;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.OrFilterBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;

public class BulkDeleter extends BulkUpdater {
	private DocumentDeleter deleter;
	
	public BulkDeleter(Client client) {
		super(client);
		deleter = new DocumentDeleter();
	}

	public void delete(Long ... ids) throws UpdateException, BulkIndexException {
		if (ids.length == 0)
			throw new IllegalArgumentException("no document ids specified");
		update(new IdFinder(ids), deleter);
	}
	
	public static class IdFinder implements DocumentFinder {
		Long[] ids;
		
		public IdFinder(Long ... ids) {
			this.ids = ids;
		}

		@Override
		public Iterable<SearchHit> findDocuments(Client client) {
			OrFilterBuilder idFilter = FilterBuilders.orFilter();
			for (int i=0; i<ids.length; ++i) {
				idFilter.add(FilterBuilders.termFilter("id", ids[i]));
			}
			return client.prepareSearch().setTypes("post").setFilter(idFilter).execute().actionGet().hits();
		}
	}
	
	public static class DocumentDeleter implements DocumentUpdater {
		@Override
		public Map<String, Object> update(Map<String, Object> doc) throws UpdateException {
			doc.put("deleted", true);
			return doc;
		}
	}

	public static void main(String[] args) {
		List<Long> ids = new ArrayList<Long>();
		for (String arg: args) {
			if (arg.equals("-")) {
				Scanner console = new Scanner(System.in);
				while (console.hasNext()) {
					ids.add(Long.parseLong(console.next()));
				}
			}
			ids.add(Long.parseLong(arg));
		}
		Node node = NodeBuilder.nodeBuilder().client(true).node();
		try {
			new BulkDeleter(node.client()).delete(ids.toArray(new Long[0]));
		} catch (BulkIndexException e) {
			System.err.println("error during bulk delete:");
			System.err.println(e.getDetailString());
		} catch (UpdateException e) {
			e.printStackTrace();
		}
		node.close();
	}
}
