package com.threetaps.search;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

public class PollingAPI {
	private static final Logger log = Logger.getLogger(PollingAPI.class);
	public static final String POSTS_URL = "http://polling.3taps.com/poll/lucene.pl";
	public static final String DELETES_URL = "http://polling.3taps.com/poll/lucene_deletes.pl";
	public static final String AUTH_TOKEN = "50d6125935648d39a8a0f1a27464c783";
	
	protected String postsURL;
	private String deletesURL;
	private HttpClient httpClient;
	private HttpResponseHandler httpResponseHandler;
	private ObjectMapper jsonMapper;
	
	public PollingAPI() {
		this(POSTS_URL, DELETES_URL);
	}
	
	public PollingAPI(String postsURL, String deletesURL) {
		this.postsURL = postsURL;
		this.deletesURL = deletesURL;
		httpClient = new DefaultHttpClient();
		httpResponseHandler = new HttpResponseHandler(Charset.forName("latin1"));
		httpClient.getParams().setParameter(CoreProtocolPNames.USER_AGENT, "3taps search-indexer");
		jsonMapper = new ObjectMapper();
	}
	
	public PostResponse pollPosts(Long start, Integer size) throws IOException {
		URI url;
		try {
			URIBuilder builder = new URIBuilder(postsURL);
			if (start != null)
				builder.setParameter("anchor", start.toString());
			if (size != null)
				builder.setParameter("max_num", size.toString());
			addPollParameters(builder, start, size);
			url = transformURL(builder.build());
		} catch (URISyntaxException e) {
			throw new IOException("invalid URL " + deletesURL);
		}
		return get(url, PostResponse.class);
	}
	
	protected void addPollParameters(URIBuilder builder, Long start, Integer size) {
	}
	
	protected URI transformURL(URI url) {
		return url;
	}
	
	public DeleteResponse pollDeletes(Long timestamp) throws IOException {
		URI url;
		try {
			URIBuilder builder = new URIBuilder(deletesURL);
			if (timestamp != null)
				builder.setParameter("timestamp", timestamp.toString());
			url = builder.build();
		} catch (URISyntaxException e) {
			throw new IOException("invalid URL " + deletesURL);
		}
		return get(url, DeleteResponse.class);
	}
	
	<T> T get(URI url, Class<T> responseClass) throws IOException {
		log.trace(String.format("fetching URL %s", url));
		InputStream content = null;
		String response = null;
		try {
			HttpGet get = new HttpGet(url);
			response = httpClient.execute(get, httpResponseHandler);
			if (StringUtils.isEmpty(response))
				throw new PollingException(String.format("empty response from %s", url));
			return jsonMapper.readValue(response, responseClass);
		} catch (JsonProcessingException e) {
			throw PollingException.create(url, response, e);
		} finally {
			if (content != null)
				content.close();
		}
	}
	
	static class HttpResponseHandler implements ResponseHandler<String> {
		private Charset charset;
		
		public HttpResponseHandler(Charset charset) {
			this.charset = charset;
		}

		public String handleResponse(HttpResponse response) throws ClientProtocolException, IOException {
			StatusLine statusLine = response.getStatusLine();
	        HttpEntity entity = response.getEntity();
	        if (statusLine.getStatusCode() >= 300) {
	            EntityUtils.consume(entity);
	            throw new HttpResponseException(statusLine.getStatusCode(), statusLine.getReasonPhrase());
	        }
	        return entity == null ? null : EntityUtils.toString(entity, charset);
		}
	}
	
	public static class PostResponse {
		private boolean success;
		private long anchor;
		private List<Map<String, Object>> postings;
		private String error;
		public boolean isSuccess() {
			return success;
		}
		public void setSuccess(boolean success) {
			this.success = success;
		}
		public long getAnchor() {
			return anchor;
		}
		public void setAnchor(long anchor) {
			this.anchor = anchor;
		}
		public List<Map<String, Object>> getPostings() {
			return postings;
		}
		public void setPostings(List<Map<String, Object>> postings) {
			this.postings = postings;
		}
		public String getError() {
			return error;
		}
		public void setError(String error) {
			this.error = error;
		}
	}
	
	public static class DeleteResponse {
		private long timestamp;
		private List<Long> deleteIDs;
		public long getTimestamp() {
			return timestamp;
		}
		public void setTimestamp(long timestamp) {
			this.timestamp = timestamp;
		}
		public List<Long> getDeleteIDs() {
			return deleteIDs;
		}
		public void setDeleteIDs(List<Long> deleteIDs) {
			this.deleteIDs = deleteIDs;
		}
	}
	
	public static class PollingException extends IOException {
		private static final long serialVersionUID = 1L;
		
		public PollingException(String message) {
			super(message);
		}
		
		public PollingException(String message, Throwable cause) {
			super(message, cause);
		}

		public static PollingException create(URI url, String content, Throwable t) {
			try {
				File tmp = File.createTempFile("poll", ".json");
				IOUtils.write(content, new FileOutputStream(tmp));
				return new PollingException(String.format("unable to parse response from %s; content written to %s", url, tmp.getAbsolutePath()), t);
			} catch (IOException e) {
				return new PollingException(String.format("unable to parse response from %s", url), t);
			}
		}
	}
}
