package com.moesol.actm.zookeeperTokenClient;

/**
 * enum containing the key names for the metadata<k, v> block stored in 
 * zookeeper
 * 
 * @author snerd
 *
 */
public enum MetadataKeys {
	TOKEN_COUNT("tokenCount"),
	BLOCK_COUNT("blockCount"),
	DATE_CREATED("dateCreated")
	;
	
	private final String keyValueS;
	
	private MetadataKeys(final String keyValueS) {
		this.keyValueS = keyValueS;
	}
	
	public String toString() {
		return keyValueS;
	}
	
}

