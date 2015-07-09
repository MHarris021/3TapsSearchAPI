package com.threetaps.search;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;

public class BulkIndexException extends Exception {
	private static final long serialVersionUID = 1L;
	
	public Collection<BulkItemResponse.Failure> failures;
	
	public BulkIndexException() {
		failures = new ArrayList<BulkItemResponse.Failure>();
	}
	
	public Collection<BulkItemResponse.Failure> getFailures() {
		return failures;
	}
	
	public String getDetailString() {
		StringBuilder buf = new StringBuilder();
		for (BulkItemResponse.Failure failure: failures) {
			if (buf.length() > 0)
				buf.append(", ");
			buf.append(failure.getId());
			buf.append(": ");
			buf.append(failure.getMessage());
		}
		return buf.toString();
	}
	
	public String summarizeFailures() {
		Map<String, List<String>> errs = new HashMap<String, List<String>>();
		for (Failure failure:getFailures()) {
			if (!errs.containsKey(failure.message())) {
				errs.put(failure.message(), new ArrayList<String>());
			}
			errs.get(failure.message()).add(String.format("%s/%s",
					failure.index(), failure.id()));
		}
		StringBuilder buf = new StringBuilder();
		for (Map.Entry<String, List<String>> entry: errs.entrySet()) {
			if (buf.length() > 0)
				buf.append(", ");
			buf.append("{ error : ");
			buf.append(entry.getKey());
			buf.append(" , docs : ");
			buf.append(entry.getValue());
			buf.append(" }");
		}
		return buf.toString();
	}
}