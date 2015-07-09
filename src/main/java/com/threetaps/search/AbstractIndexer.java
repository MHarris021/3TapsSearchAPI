package com.threetaps.search;

import org.elasticsearch.client.Client;

public class AbstractIndexer {
	protected Client client;
	
	protected AbstractIndexer(Client client) {
		this.client = client;
	}
}