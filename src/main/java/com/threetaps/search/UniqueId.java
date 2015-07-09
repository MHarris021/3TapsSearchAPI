package com.threetaps.search;

import java.util.Map;

public class UniqueId {
	String source;
	String externalID;
	
	public UniqueId(String source, String externalID) {
		this.source = source;
		this.externalID = externalID;
	}

	public UniqueId(Object source, Object externalID) {
		this(source.toString(), externalID.toString());
	}
	
	public UniqueId(Map<String, Object> post) {
		this(post.get("source"), post.get("external_id"));
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((externalID == null) ? 0 : externalID.hashCode());
		result = prime * result + ((source == null) ? 0 : source.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		UniqueId other = (UniqueId) obj;
		if (externalID == null) {
			if (other.externalID != null)
				return false;
		} else if (!externalID.equals(other.externalID))
			return false;
		if (source == null) {
			if (other.source != null)
				return false;
		} else if (!source.equals(other.source))
			return false;
		return true;
	}
}
