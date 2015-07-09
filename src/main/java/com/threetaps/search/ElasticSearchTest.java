package com.threetaps.search;

import java.io.IOException;

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

public class ElasticSearchTest {
	public static void main(String[] args) throws IOException {
		Settings settings = ImmutableSettings.settingsBuilder()
				.put("cluster.name", "search-dev.3taps.com")
				.put("network.host", "127.0.0.1").build();
		Node node = NodeBuilder.nodeBuilder().settings(settings).client(true).node();
		Client client = node.client();
		ClusterStateResponse response = client.admin().cluster().state(new ClusterStateRequest()).actionGet();
		response.writeTo(new OutputStreamStreamOutput(System.out));
		node.close();
	}
}
