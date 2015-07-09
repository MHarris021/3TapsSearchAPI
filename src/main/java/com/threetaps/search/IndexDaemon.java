package com.threetaps.search;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.client.HttpResponseException;
import org.apache.log4j.Logger;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.admin.indices.status.IndexStatus;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.sort.ScriptSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import sun.misc.Signal;
import sun.misc.SignalHandler;

import com.threetaps.search.BulkIndexer.IndexAssigner;
import com.threetaps.search.BulkUpdater.DocumentUpdater;
import com.threetaps.search.PollingAPI.DeleteResponse;
import com.threetaps.search.PollingAPI.PostResponse;

@SuppressWarnings("restriction") // for Signal/SignalHandler
public class IndexDaemon implements IndexAssigner {
	private static final Logger log = Logger.getLogger(IndexDaemon.class);
	
	private Client client;
	private BulkIndexer indexer;
	private BulkDeleter deleter;
	protected PostFixer fixer;
	private BulkSuperceder superceder;
	protected PollingAPI api;
	private Long postAnchor;
	private Long stopAnchor;
	private Long deleteTimestamp;
	protected String postAnchorFile = "/dev/shm/3taps/max_id";
	protected String deleteTimestampFile = "/dev/shm/3taps/delete_ts";
	private String openIndexDir = "/dev/shm/3taps/indices";
	private String memoryIndexDir = "/dev/shm/elasticsearch/data/";
	private String elasticSearchIndexDir = "/opt/elasticsearch/data/search-dev.3taps.com/nodes/0/indices/";
	private int pollSize;
	private long lastRequestTime;
	private long lastPostTime;
	private int noPostWaitInterval;
	private int noPostWaitMax;
	private long lastWarnTime;
	private int noPostWarnInterval;
	private int noPostWarnMax;
	private boolean noState;
	private boolean running;
	Date memoryCutoff;
	Date diskCutoff;
	protected String type;

	
	public IndexDaemon(Client client) {
		this.client = client;
		indexer = new BulkIndexer(client);
		deleter = new BulkDeleter(client);
		fixer = new PostFixer();
		superceder = new BulkSuperceder(client);
		api = new PollingAPI();
		postAnchor = getPostAnchor();
		stopAnchor = null;
		deleteTimestamp = null;
		pollSize = 1000;
		lastRequestTime = 0;
		lastPostTime = System.currentTimeMillis();
		noPostWaitInterval = 1;
		noPostWaitMax = 300;
		lastWarnTime = 0;
		noPostWarnInterval = 900;
		noPostWarnMax = 900;
		noState = false;
		running = true;
		initializeDaily();
		type = "post";
	}
	
	private void initializeDaily() {
		Calendar mem = Calendar.getInstance();
		mem.add(Calendar.DATE, -2);
		mem.set(Calendar.HOUR_OF_DAY, 0);
		mem.set(Calendar.MINUTE, 0);
		mem.set(Calendar.SECOND, 0);
		mem.set(Calendar.MILLISECOND, 0);
		memoryCutoff = mem.getTime();
		Calendar disk = Calendar.getInstance();
		disk.add(Calendar.DATE, -9);
		disk.set(Calendar.HOUR_OF_DAY, 0);
		disk.set(Calendar.MINUTE, 0);
		disk.set(Calendar.SECOND, 0);
		disk.set(Calendar.MILLISECOND, 0);
		diskCutoff = disk.getTime();
		try {
			Map<String, IndexStatus> indexStatus = 
					client.admin().indices().prepareStatus().execute().actionGet().indices();
			for (String index: indexStatus.keySet()) {
				Date indexDate = getIndexDate(index);
				if (indexDate == null) {
					if (log.isDebugEnabled())
						log.debug(String.format("skipping cutoff check for index %s", index));
					continue;
				}
				if (index.contains("_mem_")) {
					if (indexDate.before(memoryCutoff))
						try {
							deleteIndex(index);
						} catch (SearchException e) {
							log.error(e.getMessage(), e);
						}
				} else {
					if (indexDate.before(diskCutoff))
						try {
							closeIndex(index);
						} catch (SearchException e) {
							log.error(e.getMessage(), e);
						}
				}
			}
			Calendar tomorrow = Calendar.getInstance();
			tomorrow.add(Calendar.DATE, 1);
			String tomorrowMem = getMemoryIndex(tomorrow.getTime());
			if (!indexStatus.containsKey(tomorrowMem)) {
				try {
					//createIndex(tomorrowMem, true);
					createIndex(tomorrowMem, false);
				} catch (SearchException e) {
					log.error(e.getMessage(), e);
				}
			}
			String tomorrowDisk = getDiskIndex(tomorrow.getTime());
			if (!indexStatus.containsKey(tomorrowDisk)) {
				try {
					createIndex(tomorrowDisk, false);
				} catch (SearchException e) {
					log.error(e.getMessage(), e);
				}
			}
		} catch (ElasticSearchException e) {
			log.error("error listing open indexes", e);
		}
	}
	
	private void createIndex(String index, boolean inMemory) throws SearchException {
		try {
			if (inMemory) {
				File target = new File(memoryIndexDir, index);
				if (log.isTraceEnabled())
					log.trace(String.format("creating directory %s", target));
				if (!target.mkdir())
					throw new IOException("mkdir failed");
				File link = new File(elasticSearchIndexDir, index);
				if (log.isTraceEnabled())
					log.trace(String.format("creating link from %s to %s", link, target));
				// can do this natively in Java 1.7, but I'm in a hurry...
				Process p = Runtime.getRuntime().exec( new String[] { 
						"ln", "-s", target.getAbsolutePath(), link.getAbsolutePath() } );
				p.waitFor();
				if (p.exitValue() > 0)
					throw new IOException("ln failed");
				p.destroy();
			}
			if (log.isDebugEnabled())
				log.debug(String.format("creating new index %s", index));
			client.admin().indices().prepareCreate(index).execute().actionGet();
			addOpenIndex(index);
		} catch (InterruptedException e) {
			throw new SearchException(String.format("interrupted while waiting for symlink %s", index), e);
		} catch (IOException e) {
			throw new SearchException(String.format("error creating symlink for index %s", index), e);
		} catch (ElasticSearchException e) {
			if (e.getMessage().contains("Already exists")) {
				addOpenIndex(index);
			} else {
				throw new SearchException(String.format("error creating index %s", index), e);
			}
		}
	}
	
	private void closeIndex(String index) throws SearchException {
		if (log.isDebugEnabled())
			log.debug(String.format("closing index %s", index));
		try {
			client.admin().indices().prepareClose(index).execute().actionGet();
			deleteOpenIndex(index);
		} catch (ElasticSearchException e) {
			throw new SearchException(String.format("error closing index %s", index), e);
		}
	}
	
	private void deleteIndex(String index) throws SearchException {
		if (log.isDebugEnabled())
			log.debug(String.format("deleting index %s", index));
		try {
			client.admin().indices().prepareDelete(index).execute().actionGet();
			deleteOpenIndex(index);
		} catch (ElasticSearchException e) {
			throw new SearchException(String.format("error deleting index %s", index), e);
		}
	}
	
	private void addOpenIndex(String index) {
		if (log.isTraceEnabled())
			log.trace(String.format("adding index %s to %s", index, openIndexDir));
		try {
			new File(openIndexDir, index).createNewFile();
		} catch (IOException e) {
			log.error(String.format("error creating %s/%s", openIndexDir, index), e);
		}
	}
	private boolean isOpenIndex(String index) {
		return new File(openIndexDir, index).exists();
	}
	private void deleteOpenIndex(String index) {
		if (log.isTraceEnabled())
			log.trace(String.format("removing index %s from %s", index, openIndexDir));
		new File(openIndexDir, index).delete();
	}

	public Long getPostAnchor() {
		try {
			postAnchor = readLongFromFile(postAnchorFile);
		} catch (IOException e) {
			log.error(String.format("error reading from %s: %s", postAnchorFile, e.getMessage()));
			//setPostAnchor(searchMaxID() + 1);
		}
		return postAnchor;
	}
	
	public void setPostAnchor(Long postAnchor) {
		this.postAnchor = postAnchor;
		if (!noState)
			try {
				writeLongToFile(postAnchorFile, postAnchor);
			} catch (IOException e) {
				log.error(String.format("error writing to %s: %s", postAnchorFile, e.getMessage()));
			}
	}
	
	public Long getDeleteTimestamp() {
		try {
			deleteTimestamp = readLongFromFile(deleteTimestampFile);
		} catch (IOException e) {
			log.error(String.format("error reading from %s", deleteTimestampFile), e);
		}
		return deleteTimestamp;
	}
	
	public void setDeleteTimestamp(Long deleteTimestamp) {
		this.deleteTimestamp = deleteTimestamp;
		if (!noState)
			try {
				writeLongToFile(deleteTimestampFile, deleteTimestamp);
			} catch (IOException e) {
				log.error(String.format("error writing to %s", deleteTimestampFile), e);
			}
	}
	
	public Long searchMaxID() {
		log.debug("searching for max ID");
		SearchResponse response = client.prepareSearch().setTypes("post")
				.addSort(new ScriptSortBuilder("Long.valueOf(doc['_id'].value)", "number").order(SortOrder.DESC))
				.execute().actionGet();
		return Long.valueOf(response.hits().getAt(0).getId());
	}

	private static Long readLongFromFile(String file) throws IOException {
		return new Scanner(new FileInputStream(file)).nextLong();
	}
	
	private static void writeLongToFile(String file, Long l) throws IOException {
		PrintWriter writer = new PrintWriter(new FileOutputStream(file));
		writer.print(l);
		writer.close();
	}

	public void run() {
		while (running) {
			/* fetch posts via polling API and bulk index
			 */
			try {
				if (log.isTraceEnabled())
					log.trace("sending request to polling API...");
				lastRequestTime = System.currentTimeMillis();
				PostResponse response = api.pollPosts(postAnchor, pollSize);
				if (!response.isSuccess()) {
					throw new IOException(StringUtils.defaultIfBlank(response.getError(),
							"polling API returned {success: false} with no error message"));
				}
				List<Map<String, Object>> posts = response.getPostings();
				if (log.isDebugEnabled())
					log.debug(String.format("polling API returned %d new posts.", posts.size()));
				if (!posts.isEmpty()) {
					lastPostTime = System.currentTimeMillis();
					noPostWaitInterval = 1;
					noPostWarnInterval = 900;
					if (log.isTraceEnabled())
						log.trace("fixing posts...");
					for (Map<String, Object> post: posts) {
						try {
							fixer.update(post);
						} catch (UpdateException e) {
							log.error("error fixing post", e);
						}
						if (Config.handleReposts())
							maybeAddToSupercedes(post);
					}
					indexer.index(posts, type, this);
					setPostAnchor(response.getAnchor());
					if (log.isDebugEnabled())
						log.debug(String.format("new anchor is %d.", response.getAnchor()));
					if (!superceder.isEmpty()) {
						try {
							superceder.process();
						} catch (SearchException e) {
							log.error(e.getMessage(), e);
							running = false;
						} catch (UpdateException e) {
							log.error(e.getMessage(), e);
							running = false;
						} catch (BulkIndexException e) {
							log.error(String.format("failure updating superceded posts: %s", e.summarizeFailures()));
							running = false;
						}
					}
				} else {
					long noPostInterval = (lastRequestTime-lastPostTime)/1000;
					long lastWarnInterval = (lastRequestTime-lastWarnTime)/1000;
					if (Math.min(noPostInterval, lastWarnInterval) > noPostWarnInterval) {
						log.warn(String.format("no new postings for %d seconds", noPostInterval));
						lastWarnTime = System.currentTimeMillis();
						noPostWarnInterval = Math.min(noPostWarnInterval*2, noPostWarnMax);
					} else {
						log.info(String.format("no new postings for %s seconds", noPostInterval));
					}
				}
			} catch (HttpResponseException e) {
				log.error(String.format("error polling for posts: HTTP %d %s", e.getStatusCode(), e.getMessage()));
				waitForRecovery(300);
			} catch (IOException e) {
				log.error(String.format("error polling for posts: %s", e.getMessage()));
				waitForRecovery(300);
			} catch (ElasticSearchException e) {
				log.error("error indexing documents", e);
				running = false;
			} catch (BulkIndexException e) {
				log.error(String.format("bulk index failure: %s", e.summarizeFailures()));
				running = false;
			}
			
			/* fetch deletes and update index
			 */
			if (Config.handleDeletes()) {
				try {
					DeleteResponse response = api.pollDeletes(getDeleteTimestamp());
					deleter.delete(response.getDeleteIDs().toArray(new Long[0]));
					setDeleteTimestamp(response.getTimestamp());
				} catch (IOException e) {
					log.error("error polling for deletes", e);
				} catch (BulkIndexException e) {
					log.error(String.format("failure indexing deletes: %s", e.summarizeFailures()));
					running = false;
				} catch (UpdateException e) {
					if (e.getCause() != null) {
						// log the cause directly for the logfile monitors...
						log.error("error updating deleted documents", e.getCause());
					} else {
						log.error("error updating deleted documents", e);
					}
				}
			}
			
			/* don't overwhelm Chris if there's no activity
			 */
			if (stopAnchor != null && postAnchor >= stopAnchor)
				break;
			if (System.currentTimeMillis()-lastRequestTime < 1000) {
				if (log.isTraceEnabled())
					log.trace(String.format("no activity; sleeping for %ds...", noPostWaitInterval));
				try {
					if (running)
						Thread.sleep(noPostWaitInterval*1000);
				} catch (InterruptedException e) {
					log.warn("sleep interrupted", e);
				}
				noPostWaitInterval = Math.min(noPostWaitInterval*2, noPostWaitMax);
			}
		}
	}
	
//	TODO
//	private void wait(int s, String message) {	
//	}
	
	private void waitForRecovery(int s) {
		log.warn(String.format("waiting %ds for error recovery", s));
		try {
			Thread.sleep(s * 1000);
		} catch (InterruptedException e) {
			log.warn("sleep interrupted", e);
		}
	}

	@Override
	public Collection<String> getIndices(Map<String, Object> doc) {
		Collection<String> indices = new ArrayList<String>();
		Date date = getIndexDate(convertTimestamp(doc.get("timestamp")));
		if (date.after(diskCutoff)) {
			String index = maybeAddIndex(indices, getDiskIndex(date), false);
			if (date.after(memoryCutoff)) {
				//String memIndex = maybeAddIndex(indices, getMemoryIndex(date), true);
				// indices no longer fit in memory; probably just phase out
				// the search-only index...
				String memIndex = maybeAddIndex(indices, getMemoryIndex(date), false);
				if (memIndex != null)
					index = memIndex;
			}
			if (index != null) {
				client.admin().indices().prepareAliases().addAlias(index, getAlias(date)).execute().actionGet();
			}
		} else {
			// log.trace(String.format("skipping posting from %s", date));
		}
		return indices;
	}
	private String maybeAddIndex(Collection<String> indices, String index, boolean inMemory) {
		if (isOpenIndex(index)) {
			indices.add(index);
			return null;
		} else {
			try {
				createIndex(index, inMemory);
				indices.add(index);
				return index;
			} catch (Exception e) {
				log.error(String.format("error creating index %s", index), e);
				return null;
			}
		}
	}
	
	private boolean maybeAddToSupercedes(Map<String, Object> post) {
		@SuppressWarnings("unchecked")
		Map <String, Object> annots = (Map<String, Object>)((List<?>)post.get("annotations")).get(0);
		Object timestamp = annots.get("original_posting_date");
		if (timestamp != null) {
//			Date date = IndexDaemon.getIndexDate(IndexDaemon.convertTimestamp(timestamp));
//			if (date.after(diskCutoff)) {
//				superceder.add(getDiskIndex(date), post);
//				if (date.after(memoryCutoff)) {
//					superceder.add(getMemoryIndex(date), post);
//				}
//				return true;
//			}
			superceder.add(post);
			return true;
		}
		return false;
	}
	
	private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	private static final Pattern datePattern = Pattern.compile("\\d{4}-\\d{2}-\\d{2}");
	
	protected static Date getIndexDate(long timestamp) {
		return new Date(timestamp * 1000); // fuck you, Java
	}
	
	protected static Long convertTimestamp(Object timestamp) {
		if (timestamp instanceof Long)
			return (Long)timestamp;
		else if (timestamp instanceof Integer)
			return ((Integer)timestamp).longValue();
		else if (timestamp instanceof String)
			return Long.valueOf((String)timestamp);
		else
			return Long.valueOf(String.valueOf(timestamp));
	}
	
	protected static String getMemoryIndex(Date date) {
		return String.format("v%s_mem_%s", Config.getVersion(), dateFormat.format(date));
	}
	protected static String getDiskIndex(Date date) {
		return String.format("v%s_disk_%s", Config.getVersion(), dateFormat.format(date));
	}
	protected static String getAlias(Date date) {
		return String.format("v%s_%s", Config.getVersion(), dateFormat.format(date));
	}
	
	protected static Date getIndexDate(String index) {
		Matcher matcher = datePattern.matcher(index);
		if (matcher.find()) {
			try {
				return dateFormat.parse(matcher.group());
			} catch (ParseException e) {
				log.error(String.format("error parsing date %s", matcher.group()), e);
			}
		}
		return null;
	}
	
	protected static class PostFixer implements DocumentUpdater {
		@Override
		public Map<String, Object> update(Map<String, Object> doc) throws UpdateException {
			// decode HTML in header/body and trim surrounding whitespace...
			if (doc.containsKey("heading"))
				doc.put("heading", fixText(doc.get("heading")));
			if (doc.containsKey("body"))
				doc.put("body", fixText(doc.get("body")));
			
			// price is annoying
			if (doc.containsKey("price") && doc.get("price") instanceof String)
				doc.put("price", fixPrice(doc.get("price")));
			
			// annotations is a list that only ever has one member, but that's not a big deal...
			
			// images has an unnecessary extra list around it...
			if (doc.containsKey("images")) {
				if (doc.get("images") == null) {
					doc.put("images", new ArrayList<Object>(0));
				} else if (doc.get("images") instanceof List) {
					List<?> images = (List<?>)doc.get("images");
					if (!images.isEmpty()) {
						if (images.get(0) == null)
							doc.put("images", new ArrayList<Object>(0));
						else if (images.get(0)instanceof List)
							doc.put("images", images.get(0));
					}
				}
			}
			
			// separate out lat/lon so we can search by geolocation...
			if (doc.containsKey("location")) {
				@SuppressWarnings("unchecked")
				Map<String, Object> location = (Map<String, Object>)doc.get("location");
				if (location.containsKey("lat") && location.containsKey("long")) {
					Map<String, Object> geolocation = new HashMap<String, Object>();
					geolocation.put("lat", location.get("lat"));
					geolocation.put("lon", location.get("long"));
					doc.put("geolocation", geolocation);
				}
			}
			
			// add deleted as false, since elasticsearch default is only applied if null, not if missing...
			if (!doc.containsKey("deleted"))
				doc.put("deleted", false);
			
			return doc;
		}
		private String fixText(Object o) {
			String text = (String)o;
			if (text != null) {
				text = StringEscapeUtils.unescapeHtml(text);
				text = text.trim();
			}
			return text;
		}
		Pattern nondecimal = Pattern.compile("[^\\d\\.]+");
		private String fixPrice(Object o) {
			String price = (String)o;
			if (price != null) {
				try {
					Double.parseDouble(price);
				} catch (NumberFormatException e) {
					price = nondecimal.matcher(price).replaceAll("");
				}
			}
			return price;
		}
	}
	
	private static class Args {
		@Option(name="-c", aliases={ "--cluster" }, metaVar="CLUSTER",
				usage="elasticsearch cluster name")
		String cluster;
		
		@Option(name="-h", aliases={ "--host" }, metaVar="HOST",
				usage="elasticsearch host name")
		String host;
		
		@Option(name="-p", aliases={ "--port" }, metaVar="PORT",
				usage="elasticsearch port number (transport client)")
		Integer port;
		
		@Option(name="-s", aliases={ "--start" }, metaVar="ID",
				usage="start at ID")
		Long start;
		
		@Option(name="-S", aliases={ "--stop" }, metaVar="ID",
				usage="stop at ID")
		Long stop;
		
		@Option(name="-z", aliases={ "--size" }, metaVar="SIZE",
				usage="poll for SIZE posts at a time")
		Integer size;
		
		@Option(name="-1", aliases={ "--single" }, metaVar="ID",
				usage="poll for single post ID")
		Long single;
		
		@Option(name="-n", aliases={ "--dry-run" }, 
				usage="don't actually index, just show what would be done")
		boolean dryRun;
		
		@Option(name="-N", aliases={ "--no-state" }, 
				usage="don't update global state variables; useful for reindexing")
		boolean noState;
	}
	
	public static void main(String[] args) throws CmdLineException {
		init(args, "index daemon", null, new DaemonConstructor());
	}
	
	protected static class DaemonConstructor {
		public IndexDaemon constructDaemon(Client client) {
			return new IndexDaemon(client);
		}
	}
	
	protected static void init(String[] args, String nodeName, String pidFile, DaemonConstructor constructor) throws CmdLineException {
		Args a = new Args();
		CmdLineParser parser = new CmdLineParser(a);
		parser.parseArgument(args);
		log.trace("initializing client node...");
		Builder settings = ImmutableSettings.settingsBuilder()
				.put("cluster.name", StringUtils.defaultString(a.cluster, "search-dev.3taps.com"))
				.put("node.name", nodeName != null ? nodeName : "index daemon");
		Client client;
		if (a.port != null) {
			client = new TransportClient(settings.build())
					.addTransportAddress(new InetSocketTransportAddress(StringUtils.defaultString(a.host, "127.0.0.1"), a.port));
		} else {
			// TODO maybe separate bind host and transport host?
			settings.put("network.host", StringUtils.defaultString(a.host, "127.0.0.1"));
			Node node = NodeBuilder.nodeBuilder()
					.settings(settings.build())
					.client(true)
					.node();
			client = node.client();
		}
		log.trace("instantiating daemon...");
		IndexDaemon daemon = constructor.constructDaemon(client);
		if (a.start != null)
			daemon.postAnchor = a.start;
		if (a.stop != null)
			daemon.stopAnchor = a.stop;
		if (a.size != null)
			daemon.pollSize = a.size;
		if (a.single != null) {
			daemon.postAnchor = a.single;
			daemon.stopAnchor = a.single + 1;
			daemon.pollSize = 1;
			daemon.noState = true;
		}
		if (a.dryRun) {
			daemon.indexer.dryRun = true;
			daemon.noState = true;
		}
		if (a.noState)
			daemon.noState = true;
		log.trace("setting signal handlers...");
		setSignalHandlers(daemon);
		log.trace("starting daemon.");
		try {
			IOUtils.write(getPID(), new FileOutputStream(pidFile != null ? pidFile : "/var/run/3taps/indexd.pid"));
		} catch (IOException e) {
			log.error(String.format("error writing pid file: %s", e.getMessage()), e);
		}
		daemon.run();
		log.trace("cleaning up...");
//		node.close();
		log.trace("done.");
	}
	
	private static Pattern pidPattern = Pattern.compile("^\\d+");

	private static String getPID() {
		Matcher matcher = pidPattern.matcher(ManagementFactory.getRuntimeMXBean().getName());
		if (matcher.find())
			return matcher.group();
		else
			return null;
	}

	/* stick this off in its own method so we can quickly neutralize it if
	 * we find a JVM where its unsupported...
	 */
	private static void setSignalHandlers(final IndexDaemon daemon) {
		SignalHandler niceExit = new SignalHandler() {
			public void handle(Signal sig) {
				daemon.running = false;
				Thread t = Thread.currentThread();
				if (t.getState().equals(Thread.State.TIMED_WAITING))
					t.interrupt();
			}
		};
		Signal.handle(new Signal("TERM"), niceExit);
//		Signal.handle(new Signal("INT"), niceExit);
		Signal.handle(new Signal("HUP"), new SignalHandler() {
			public void handle(Signal sig) {
				daemon.initializeDaily();
			}
		});
	}
}
