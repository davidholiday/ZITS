package com.moesol.actm.zookeeperTokenClient;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Assert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * main method that kicks off the pressure tester
 * 
 * @author snerd
 *
 */
public class PressureTesterBootstrap {
	
	private static final Logger LOGGER = 
			LoggerFactory.getLogger(PressureTesterBootstrap.class.getName());
	
    public static void main( String[] args ) {
    	int threadCountI = 10;
     	int runCountI = 5000; 
     	int requestNumPerGetI = 1000;
     	int numBlocksI = 275;
    	String zkConnectS = "localhost:32810";
    	String zkNodeBasePathS = "/OTM/AtomicLong/cluster";
     	
    	// create cluster client and use it to populate zookeeper with 
    	// [numBlocksI] worth of random data
    	//
     	ZITS zkClusterClient = new ZITS(zkConnectS, zkNodeBasePathS);	
  	
		boolean successB = zkClusterClient.initializeBlocks(numBlocksI);
		Assert.assertTrue("failure detected in initializing blocks!", successB);
     	
		for (int i = 0; i < numBlocksI; i ++) {
			byte[] blockARR = new byte[zkClusterClient.BLOCK_SIZE_IN_BYTES];
			new Random().nextBytes(blockARR);
			
			zkClusterClient.getLock(i);
			successB = zkClusterClient.trySetData(blockARR, i);
			zkClusterClient.releaseLock(i);
			
			Assert.assertTrue(
					"failure detected in serializing blocks!", successB);
		}
     	
    	
		// spin up the pressure tester minions
		//
    	ExecutorService executorPool = 
    			Executors.newFixedThreadPool(threadCountI);
    	
    	Set<Long> combinedReturnSet = new HashSet<Long>();

    	List< Future<Set<Long>> > resultListAL = 
    			new ArrayList< Future<Set<Long>> >();
    	 	
    	for (int i = 0; i < threadCountI; i ++) {

    		long tokenCountL = 
    				zkClusterClient.getTokenCountForBlockNum(numBlocksI);
   		
    		PressureTesterMinion minnieTheMinion = 
    				new PressureTesterMinion(runCountI, 
    										 tokenCountL, 
    										 requestNumPerGetI,
    										 zkConnectS,
    										 zkNodeBasePathS);
		
    		Future<Set<Long>> resultSetF = 
    				executorPool.submit(minnieTheMinion);
	
    		resultListAL.add(resultSetF);
    	}
                	
    	for (Future<Set<Long>> resultFuture : resultListAL) {
    		  		
    		try {
    			combinedReturnSet.addAll(resultFuture.get());
			} catch (Exception e) {
				LOGGER.error("something went wrong aggregating results!", e);
			}
	
    	}
    	
    	
    	// check the results and ensure things get cleaned up properly 
    	//
    	try {
    		Assert.assertTrue("duplicate entries detected!", 
    				combinedReturnSet.size() == (threadCountI 
    											 * runCountI 
    											 * requestNumPerGetI));	
    	} finally {
    		executorPool.shutdown();
    		successB = zkClusterClient.deleteBlocks();
    		
    		if (!successB) {
    			LOGGER.error("failure detected in deleting blocks!");
    		}
    	} 	
    	
    }
    

    
    
}
