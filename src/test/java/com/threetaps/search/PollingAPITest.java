package com.threetaps.search;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.threetaps.search.PollingAPI.DeleteResponse;
import com.threetaps.search.PollingAPI.PostResponse;

public class PollingAPITest {
	static PollingAPI pollingAPI;
	
	@BeforeClass
	public static void setupBeforeClass() {
		pollingAPI = new PollingAPI();
	}
	
	@Test
	public void testConvertPostResponse() throws IOException, UpdateException {
		PostResponse response = new ObjectMapper().readValue(
				PollingAPITest.class.getResourceAsStream("/posts.json"),
				PostResponse.class);
		List<Map<String, Object>> posts = response.getPostings();
		assertEquals("wrong number of posts", 1, posts.size());
		Map<String, Object> post = posts.get(0);
		new IndexDaemon.PostFixer().update(post);
		assertEquals("bad price conversion", 24000, Double.parseDouble((String)post.get("price")), 0.01);
		assertTrue("images [null] not converted to empty list", ((List<?>)post.get("images")).isEmpty());
		assertTrue("missing geolocation", post.containsKey("geolocation"));
		assertTrue("missing deleted flag", post.containsKey("deleted"));
	}
	
	@Test
	public void testConvertDeleteResponse() throws IOException {
		DeleteResponse response = new ObjectMapper().readValue(
				PollingAPITest.class.getResourceAsStream("/deletes.json"),
				DeleteResponse.class);
		assertEquals("incorrect timestamp", 1367888972, response.getTimestamp());
		assertEquals("wrong number of deletes", 1000, response.getDeleteIDs().size());
	}
	
	@Test
	@Ignore
	public void testPollDeletes() throws IOException {
		DeleteResponse response = pollingAPI.pollDeletes(null);
		assertEquals("incorrect timestamp", 1367888949, response.getTimestamp());
		assertEquals("wrong number of deletes", 986, response.getDeleteIDs().size());
	}
}
