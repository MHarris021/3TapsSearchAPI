package com.threetaps.search;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.threetaps.search.PollingAPI.PostResponse;

public class MaratPollingAPITest {
	@SuppressWarnings("unused")
	private static Logger log = Logger.getLogger(MaratPollingAPITest.class);
	
	static PollingAPI pollingAPI;
	
	@BeforeClass
	public static void setupBeforeClass() {
		pollingAPI = new IndexDaemonMarat.MaratPollingAPI();
	}
	
	@Test
	public void testConvertPostResponse() throws IOException, UpdateException {
		PostResponse response = new ObjectMapper().readValue(
				MaratPollingAPITest.class.getResourceAsStream("/postsMarat.json"),
				PostResponse.class);
		List<Map<String, Object>> posts = response.getPostings();
		assertEquals("wrong number of posts", 1, posts.size());
		Map<String, Object> post = posts.get(0);
		new IndexDaemonMarat.MaratFixer().update(post);
		checkAnnotations((Map<String, Object>)post.get("annotations"));
	}	
	private void checkAnnotations(Map<String, Object> annotations) {
		for (Object v: annotations.values()) {
			if (v instanceof Map)
				fail(String.format("found nested annotation %s", v));
		}
	}

//	@Test
//	public void testConvertDeleteResponse() throws IOException {
//		DeleteResponse response = new ObjectMapper().readValue(
//				MaratPollingAPITest.class.getResourceAsStream("/deletes.json"),
//				DeleteResponse.class);
//		assertEquals("incorrect timestamp", 1367888972, response.getTimestamp());
//		assertEquals("wrong number of deletes", 1000, response.getDeleteIDs().size());
//	}
	
	@Test
	@Ignore
	public void testPollPostings() throws IOException, UpdateException {
		PostResponse response = pollingAPI.pollPosts(1l, null);
		new ObjectMapper().writeValue(new File("/tmp/marat.json"), response);
		IndexDaemonMarat.MaratFixer maratFixer = new IndexDaemonMarat.MaratFixer();
		for (Map<String,Object> doc: response.getPostings())
			maratFixer.update(doc);
	}
}
