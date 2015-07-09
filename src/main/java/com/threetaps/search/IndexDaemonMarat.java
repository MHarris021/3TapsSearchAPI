package com.threetaps.search;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.http.client.utils.URIBuilder;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.client.Client;
import org.kohsuke.args4j.CmdLineException;

public class IndexDaemonMarat extends IndexDaemon {
	private static final Logger log = Logger.getLogger(IndexDaemon.class);

	public IndexDaemonMarat(Client client) {
		super(client);
	}
	
	public static void main(String[] args) throws CmdLineException {
		init(args, "index daemon (Marat)", "/var/run/3taps/indexd_marat.pid", new DaemonConstructor() {
			public IndexDaemon constructDaemon(Client client) {
				IndexDaemon daemon = new IndexDaemonMarat(client);
				daemon.api = new MaratPollingAPI();
				daemon.fixer = new MaratFixer();
				daemon.type = "post_marat";
				daemon.postAnchorFile = "/dev/shm/3taps/max_id_marat";
				daemon.deleteTimestampFile = "/dev/shm/3taps/delete_ts_marat";
				return daemon;
			}
		});
	}

	private static final ObjectMapper json = new ObjectMapper();
	private static Object decodeDoubleEncodedString(String s) {
		if (s.length() > 0 ) {
			try {
				return json.readValue(s, Object.class);
			} catch (Exception e) {
				log.error(e.getMessage(), e);
				return s;
			}
		} else {
			return null;
		}
	}

	static class MaratFixer extends PostFixer {
		@Override
		public Map<String, Object> update(Map<String, Object> doc) throws UpdateException {
			if (doc.get("timestamp") == null)
				doc.put("timestamp", System.currentTimeMillis()/1000l);
			if (doc.get("images") instanceof String)
				doc.put("images", decodeDoubleEncodedString((String)doc.get("images")));
			if (doc.get("images") instanceof List) {
				for (Iterator<?> i = ((List<?>)doc.get("images")).iterator(); i.hasNext(); ) {
					if (!checkImage(i.next()))
						i.remove();
				}
			} else if (doc.get("images") instanceof Map) {
				if (!checkImage(doc.get("images"))) {
					doc.put("images", new ArrayList<Object>(0));
				}
			}
			if (doc.get("annotations") instanceof String)
				doc.put("annotations", decodeDoubleEncodedString((String)doc.get("annotations")));
			if (doc.get("origin_ip_address") == "")
				doc.put("origin_ip_address", null);
			if (doc.get("transit_ip_address") == "")
				doc.put("transit_ip_address", null);
			fixAnnotations((Map<String, Object>) doc.get("annotations"));
			Object deleted = doc.get("deleted");
			if (deleted == null) {
				doc.put("deleted", false);
			} else if (deleted instanceof String) {
				try {
					doc.put("deleted", Boolean.valueOf((String)deleted));
				} catch (Exception e) {
				}
			}
			super.update(doc);
			return doc;
		}

		private static boolean checkImage(Object o) {
			return o instanceof Map;
		}
		
		private static void fixAnnotations(Map<String, Object> annotations) {
			if (annotations == null)
				return;
			Map<String, Object> newMap = new HashMap<String, Object>();
			for (Iterator<Map.Entry<String, Object>> i = annotations.entrySet().iterator(); i.hasNext(); ) {
				Map.Entry<String, Object> entry = i.next();
				if (entry.getValue() instanceof Map) {
					newMap.putAll(convertMap(entry.getKey(), (Map)entry.getValue()));
					i.remove();
				}
			}
			annotations.putAll(newMap);
		}
		private static Map<String, Object> convertMap(String prefix, Map m) {
			Map<String, Object> newMap = new HashMap<String, Object>();
			for (Object k: m.keySet()) {
				Object v = m.get(k);
				String newPrefix = String.format("%s.%s", prefix, k);
				if (v instanceof Map) {
					newMap.putAll(convertMap(newPrefix, (Map)v));
				} else {
					newMap.put(newPrefix, v);
				}
			}
			return newMap;
		}
	}
	
	static class MaratPollingAPI extends PollingAPI {
		private static final String[] retvals = {
//			"id", "account_id", "source", "category", "category_group", "location", 
			"id", "source", "category", "category_group", "location", 
			"external_id", "external_url", "heading", "body", "html", "timestamp",
			"expires", "language", "price", "currency", "images", "annotations",
			"status", "immortal", "deleted", "origin_ip_address", "transit_ip_address",
			"proxy_ip_address", "state", "flagged_status", "timestamp_deleted"
		};
		
		public MaratPollingAPI() {
			super();
			postsURL = "http://posting3.3taps.com/poll";
		}
		
//		@Override
//		public PostResponse pollPosts(Long start, Integer size) throws IOException {
//			return super.pollPosts(start, null);
//		}
		
		@Override
		protected void addPollParameters(URIBuilder builder, Long start, Integer size) {
			builder.setParameter("auth_token", AUTH_TOKEN);
//			builder.setParameter("source", "JBOOM");
//			builder.setParameter("source", "NBCTY");
//			builder.setParameter("source", "EBAYM");
			builder.setParameter("retvals", StringUtils.join(retvals, ","));
//			if (size != null)
//				builder.setParameter("rpp", size.toString());
		}
		
		@Override
		protected URI transformURL(URI url) {
			return URI.create(url.toString().replace("max_num=", "rpp="));
		}
	}
}
