package com.moesol.actm.zookeeperTokenClient;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * callable used to pressure test the zookeeper distributed cluster client. 
 * sets up the cluster client to report metrics data to a grpahite server.
 * 
 * @author snerd
 *
 */
public class PressureTesterMinion implements Callable<Set<Long>> {
	
	private static final Logger LOGGER = 
			LoggerFactory.getLogger(PressureTesterMinion.class.getName());
	
	private int runCountI;
	private long tokenCountL;
	private int requestNumPerGetI;
	String zkConnectS;
	String zkNodeBasePathS;
	
	public PressureTesterMinion(int runCount, 
								long tokenCountL, 
								int requestNumPerGetI, 
								String zkConnectS, 
								String zkNodeBasePathS) {
		
		this.runCountI = runCount;
		this.tokenCountL = tokenCountL;
		this.requestNumPerGetI = requestNumPerGetI;
		this.zkConnectS = zkConnectS;
		this.zkNodeBasePathS = zkNodeBasePathS;
		
		LOGGER.info("birthing callable: " + this.hashCode());
	}
	
	
	public Set<Long> call() throws Exception {		
		Set<Long> returnSet = new HashSet<Long>();
		String graphiteIP_S = "localhost";
		String metricsNamePrefixS = "otm.atomicLong.cluster";
		boolean reportMetricsB = true;

		ZITS zkClusterClient = 
			new ZITS(zkConnectS, 
															zkNodeBasePathS, 
			 			  									graphiteIP_S,
			 			  									metricsNamePrefixS,
			 			  									tokenCountL,
			 			  									reportMetricsB);
		    
	    LOGGER.info(Thread.currentThread().getId() + " starting run...");
	    
		for (int i = 0; i < this.runCountI; i ++) {

			try {
				List<Long> returnedTokensList = 
					zkClusterClient.getAvailableTokens(true, requestNumPerGetI);
				
				returnSet.addAll(returnedTokensList);
			} catch (Exception e) {
				LOGGER.error("something done gone wrong gett'n the stuff!", e);
				e.printStackTrace();
			}

		}
		
		LOGGER.info(Thread.currentThread().getId() + " ending run...");
		return returnSet;
		
	}


}







