package com.moesol.actm.zookeeperTokenClient;

import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import org.junit.Assert;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.RetryUntilElapsed;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Client that uses Apache Curator to perform CRUD operations against a chain
 * of uniformly sized data blocks.  The blocks are sequentially ordered,
 * thereby forming a long chain of bits which conceptually are flags indicating
 * the availability of a given resource in a pool. The tokens are addressable
 * in two ways: 
 * 
 * 1) a long representing the bit number of the token (ie - this token is the
 * [nth] bit in the chain).
 * 
 * 2) an int array containing three members: {blockID, byteID, bitID}. The 
 * byteID is with respect to the blockID, and the bitID is with respect to
 * the byteID. 
 * 
 * For example, if this client's block size was set to one byte, then the two
 * ways to represent an available token in the second block would be:
 * 
 * 1) 9l --> the tenth bit, or token, in the chain is available
 * 
 * 2) {1, 0, 1} --> in the second block, the first byte, the second bit, 
 * or token, is available.   
 * 
 * 
 * @author snerd
 *
 */
public class ZITS {

	public final int BLOCK_SIZE_IN_BYTES = 524288; // should not exceed 524288!
	
	public final int MAX_GET_ATTEMPTS_I = 1000;
	
	// ip:port of zookeeper instance
	private String zkConnectString;
	
	// zNode name you'd like the long to be stored at
	private String zkNodeBasePathS;

	// list of lock objects used to retrieve blocks. lockList[n] is used to 
	// r/w block[n]
	private List<InterProcessMutex> lockList = 
			new ArrayList<InterProcessMutex>();
	
	// the metrics sensor used to measure rate of updates
	private Meter updateRateM;
	
	// the curator-wrapped zookeeper client object
	private CuratorFramework curatorFramework;
	
	// the client interface to graphite
	private GraphiteReporter graphiteReporter;
	
	// caller selectable option to enable/disable metrics reporting
	private boolean reportMetricsB = false;
	
	// used internally to detect whether or not the metrics stuff is initialized
	private boolean metricsSetupB = false;
	
	// number of addressable blocks
	private int numBlocksI = 0;
	
	// used to set/unset specific tokens in a given block
	private int[] byteMaskARR;
	
	// maps a given byte value to an array containing the locations of the
	// available tokens (the locations of the zeros in the bit stream)
	private Map<Byte, List<Integer>> tokenLocationMap = 
			new HashMap<Byte, List<Integer>>();
	
	
	private static final Logger LOGGER = LoggerFactory.getLogger(
			ZITS.class);
	
	/**
	 * private constructor 
	 */
	private ZITS(){};
	
	
	

	/**
	 * constructor that enables metrics reporting to graphite server. does NOT 
	 * assume [numBlocksI] datablocks are already initialized in zookeeper. 
	 * caller MUST do this once before proceeding.  
	 * 
	 * @param zkConnectString
	 * @param zkNodeBasePath
	 * @param graphiteIP
	 * @param metricsNamePrefix
	 * @param reportMetrics
	 */
	public ZITS(String zkConnectString, 
				String zkNodeBasePath, 
				String graphiteIP,
				String metricsNamePrefix,
				boolean reportMetrics) {
		
		this.zkConnectString = zkConnectString;
		this.zkNodeBasePathS = zkNodeBasePath;
		this.reportMetricsB = reportMetrics;
		this.metricsSetupB = true;

	    Graphite graphite = new Graphite(
	    		new InetSocketAddress(graphiteIP, 2003));

	    MetricRegistry metricRegistry = new MetricRegistry();
	    
	    this.graphiteReporter = 
	    		GraphiteReporter.forRegistry(metricRegistry)
                        		.prefixedWith(metricsNamePrefix)
                        		.convertRatesTo(TimeUnit.SECONDS)
                        		.convertDurationsTo(TimeUnit.MILLISECONDS)
                        		.filter(MetricFilter.ALL)
                        		.build(graphite);
		
	    this.updateRateM = metricRegistry.meter(
	    		"meter for: " + Thread.currentThread().getId());
	    
	    graphiteReporter.start(1, TimeUnit.SECONDS);    
	    setupClient();
	}
	
	


	/**
	 * basic constructor that creates an instance with no metrics reporting 
	 * capability. does NOT assume [numBlocksI] datablocks are already 
	 * initialized in zookeeper. caller MUST do this once before proceeding. 
	 * 
	 * @param zkConnectString
	 * @param zkNodeBasePath
	 */
	public ZITS(String zkConnectString, String zkNodeBasePath) {
		this.zkConnectString = zkConnectString;
		this.zkNodeBasePathS = zkNodeBasePath;	
		setupClient();
	}
	
	
		
	/**
	 * constructor that enables metrics reporting to graphite server. assumes
	 * [numTokensL] have already been initialized in zookeeper.
	 * 
	 * @param zkConnectString
	 * @param zkNodeBasePath
	 * @param graphiteIP
	 * @param metricsNamePrefix
	 * @param numTokensL
	 * @param reportMetrics
	 */
	public ZITS(String zkConnectString, 
				String zkNodeBasePath, 
				String graphiteIP,
				String metricsNamePrefix,
				long numTokensL,
				boolean reportMetrics) {
		
		this.zkConnectString = zkConnectString;
		this.zkNodeBasePathS = zkNodeBasePath;
		this.reportMetricsB = reportMetrics;
		this.numBlocksI = getBlockCountforTokenNum(numTokensL);
		this.metricsSetupB = true;

	    Graphite graphite = new Graphite(
	    		new InetSocketAddress(graphiteIP, 2003));

	    MetricRegistry metricRegistry = new MetricRegistry();
	    
	    this.graphiteReporter = 
	    		GraphiteReporter.forRegistry(metricRegistry)
                        		.prefixedWith(metricsNamePrefix)
                        		.convertRatesTo(TimeUnit.SECONDS)
                        		.convertDurationsTo(TimeUnit.MILLISECONDS)
                        		.filter(MetricFilter.ALL)
                        		.build(graphite);
		
	    this.updateRateM = metricRegistry.meter(
	    		"meter for: " + Thread.currentThread().getId());
	    
	    graphiteReporter.start(1, TimeUnit.SECONDS);
	    
	    setupClient();
		populateLockList();
	}
	
	

	/**
	 * basic constructor that creates an instance with no metrics reporting 
	 * capability. assumes [numTokensL] have already been initialized in 
	 * zookeeper.
	 * 
	 * @param zkConnectString
	 * @param zkNodeBasePath
	 */
	public ZITS(String zkConnectString, 
			    String zkNodeBasePath,
			    long numTokensL) {
		
		this.zkConnectString = zkConnectString;
		this.zkNodeBasePathS = zkNodeBasePath;	
		this.numBlocksI = getBlockCountforTokenNum(numTokensL);;
		
		setupClient();
		populateLockList();
	}
	
	
	
	/**
	 * sets/disables metrics reporting *NOTE* if this object wasn't constructed
	 * with the necessary objects for metrics reporting, this method will always
	 * set boolean reportMetrics to false.
	 * 
	 * @param reportMetrics
	 */
	public void setReportMetrics(boolean reportMetrics) {
		if (!this.metricsSetupB) { return; }	
		this.reportMetricsB = reportMetrics;
				
		if (this.reportMetricsB) {
			this.graphiteReporter.start(1, TimeUnit.SECONDS);
		}
		else {
			this.graphiteReporter.stop();
		}
		
	}
	
	
	
	/**
	 * gets the report metrics boolean
	 * @return
	 */
	public boolean getReportMetrics() {
		return this.reportMetricsB;
	}
	
	
	
	/**
	 * gets the boolean indicator for whether or not this object's metrics 
	 * reporting logic has been initialized;
	 * @return
	 */
	public boolean getIsMetricsSet() {
		return this.metricsSetupB;
	}
	
	
	
	/**
	 * automagically initializes the correct number of blocks ensure a field
	 * of only [totalAvailableTokensL] tokens is available for retrieval.
	 * 
	 * @param totalAvailableTokensL
	 * @return boolean indicating success or failure of operation.
	 */
	public boolean initializeDataBlocks(long totalAvailableTokensL) {
		int numBlocksNeededI = getBlockCountforTokenNum(totalAvailableTokensL);
		
		boolean successB = createBlocks(numBlocksNeededI);
		if (!successB) { return successB; }
		
		successB = populateBlocksWithAvailableTokens();
		if (!successB) { return successB; }
		
		successB = setAllTokensThisAndAbove(totalAvailableTokensL);
			
		Map<String, String> metaDataValueMap = 
				createBasicMetaDataMap(numBlocksI, totalAvailableTokensL);
		
		successB = setMetaData(metaDataValueMap);
		
		return successB;
	}
	
	
	
	
	/**
	 * automagically initializes the correct number of blocks ensure a field
	 * of only [totalAvailableTokensL] tokens is available for retrieval. 
	 * also presets [tokenID_List] tokens as taken.
	 * 
	 * @param totalAvailableTokensL
	 * @param tokenID_List
	 * @return boolean indicating success or failure of operation.
	 */
 	public boolean initializeDataBlocks(long totalAvailableTokensL,
							 			List<Long> tokenID_List) {
 		
 		int numBlocksNeededI = getBlockCountforTokenNum(totalAvailableTokensL);
 		
		boolean successB = createBlocks(numBlocksNeededI);
		if (!successB) { return successB; }
		
		successB = populateBlocksWithAvailableTokens();
		if (!successB) { return successB; }
		
		successB = setTokens(true, tokenID_List);
		if (!successB) { return successB; }
		
		successB = setAllTokensThisAndAbove(totalAvailableTokensL);
		
		Map<String, String> metaDataValueMap = 
				createBasicMetaDataMap(numBlocksI, totalAvailableTokensL);
		
		successB = setMetaData(metaDataValueMap);
		
		return successB;
	}
	
	
	
	/**
	 * initializes [numBlocksI] number of blocks with free tokens.
	 *   
	 * @param numBlocksI
	 * @return boolean indicating success or failure of operation.
	 */
	public boolean initializeBlocks(int numBlocksI) {
		boolean successB = createBlocks(numBlocksI);	
		long totalAvailableTokensL = getTokenCountForBlockNum(numBlocksI);
		
		Map<String, String> metaDataValueMap = 
				createBasicMetaDataMap(numBlocksI, totalAvailableTokensL);
		
		successB = setMetaData(metaDataValueMap);
		
		return successB;
	}
	
	
	
	/**
	 * helper method used by initializers. in cases where the caller requests
	 * a specific number of tokens be available, this method ensures the
	 * number requested doesn't exceed the number available given [numBlocksI]
	 * number of bytes. 
	 * 
	 * @param numBlocksI
	 * @param requestedAvailableL
	 * @return
	 */
	public boolean checkTotalBlockSpace(int numBlocksI, 
										long requestedAvailableL) {
		
		long totalAvailableTokensActualL = getTokenCountForBlockNum(numBlocksI);
		
		boolean successB = 
				(totalAvailableTokensActualL >= requestedAvailableL);
		
		if (!successB) {
			LOGGER.error("NUMBER OF REQUESTED AVAILABLE TOKENS EXCEEDS THE"
					+ " NUMBER AVAILABLE IN " + numBlocksI + " BLOCKS OF "
					+ this.BLOCK_SIZE_IN_BYTES + " BYTES!");
		}
		
		return successB;
	}
	
	
	/**
	 * helper method that, given the value of BLOCK_SIZE_IN_BYTES and a given
	 * number of blocks, returns the number of tokens representable by the
	 * block chain. 
	 *  
	 * @param numblocksI
	 * @return
	 */
	public long getTokenCountForBlockNum(int numBlocksI) {
		return this.BLOCK_SIZE_IN_BYTES * 8 * numBlocksI;
	}
	
	
	/**
	 * sorts out the correct number of blocks needed to store numTokensL tokens.
	 * 
	 * @param numTokensL
	 * @return
	 */
	public int getBlockCountforTokenNum(long numTokensL) {
		int localBlockCountI = (int)(numTokensL / this.BLOCK_SIZE_IN_BYTES / 8);
	
		localBlockCountI = 
				(numTokensL % this.BLOCK_SIZE_IN_BYTES > 0) ? 
						(localBlockCountI + 1) : (localBlockCountI);
						
		return localBlockCountI;
	}	
	
	
	
	/**
	 * deletes all the blocks associated with the namespace the instance of 
	 * this client was created with.
	 * 
	 * @return boolean flag indicating success or failure of operation
	 */
	public boolean deleteBlocks(boolean deleteBasePath) {
		boolean successB  = false;
		
		if (this.numBlocksI == 0) {
			LOGGER.info("Nothing to delete - returning to caller...");
			successB = true;
			return successB;
		}
		
		List<String> nodesToDeleteL = 
				IntStream.range(0, numBlocksI)
		         .parallel()
		         .mapToObj(i -> new String(zkNodeBasePathS + "/" + i))
		         .collect(Collectors.toList());
		
		String pathToDeleteS = zkNodeBasePathS + "/meta";
		nodesToDeleteL.add(pathToDeleteS);
		
		if (deleteBasePath) { 
			String[] spliterfiedBasePathARR = zkNodeBasePathS.split("/");
			String rootPathS = "/" + spliterfiedBasePathARR[1];		
			nodesToDeleteL.add(rootPathS);
		}	
		
		successB = deleteHelper(nodesToDeleteL);
		
		this.lockList.clear();
		this.numBlocksI = 0;
		return successB;	
	}
			
	
	/**
	 * private helper method containing the actual delete logic. exists to
	 * keep the code DRY.
	 * 
	 * @param pathToDelete
	 * @return boolean indicating success or failure
	 */
	private boolean deleteHelper(List<String> nodesToDeleteL) {
		boolean successB = false;
		
		try {
			this.curatorFramework.blockUntilConnected(3, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			LOGGER.error("FAILURE CONNECTING TO ZOOKEEPER!", e);
			return successB;
		}
		
		for (String nodeToDeleteS : nodesToDeleteL) {
			
			try {		
				this.curatorFramework
					.delete()
					.deletingChildrenIfNeeded()
					.forPath(nodeToDeleteS);
	
			} catch (Exception e) {
				LOGGER.error("FAILURE DELETING DATA BLOCKS!", e);
				return successB;
			}
			
		}
			
		successB = true;
		return successB;
	}
	

	/**
	 * gets a set of available tokens. if only able to find some but not all 
	 * the requested quantity, what was found is returned to the caller. this 
	 * means it's up to the caller to check the return list to ensure the 
	 * correct number of tokens was returned! 
	 * 
	 * @param setTokenUponRetrivalB
	 * @param numTokensI
	 * @return
	 */
	public List<Long> getAvailableTokens(
			boolean setTokenUponRetrivalB, int numTokensI) {
		
		int numGetAttemptsI = 1;
		int blockID_I = -1;
		boolean foundB = false;
		List<Long> returnList = new ArrayList<Long>();
		Set<Integer> usedLockIDSet = new HashSet<Integer>();
		
		while (!foundB && (numGetAttemptsI < this.MAX_GET_ATTEMPTS_I)) {
			int lockID_I = getAvailableLock(usedLockIDSet);	
			
			if (lockID_I == -1) { 
				LOGGER.warn(
					"unable to acquire lock - returning empty result list!");
				return returnList; 
			}
			
			usedLockIDSet.add(lockID_I);		
			blockID_I = lockID_I;			
			byte[] blockARR = readBlock(blockID_I);

			//List<int {[blockID], [byteID, bitID]} >
			List<Integer[]> availableTokenIndexArrayList = 
					getTokenIdFromBlock(lockID_I, blockARR, numTokensI);
			
			List<Long> tempReturnList =  
				availableTokenIndexArrayList.stream()
											.parallel()
											.map(x -> indexArray_to_tokenID(x))
											.collect(Collectors.toList());
			
			returnList.addAll(tempReturnList);
			releaseLock(lockID_I);
			numGetAttemptsI ++;
			foundB = (returnList.size() >= numTokensI) ? (true) : (false);			
		}

		if (returnList.size() > numTokensI) {
			returnList = returnList.subList(0, numTokensI);
		}		
		else if (returnList.size() < numTokensI) {
			LOGGER.warn("RETURNING " + returnList.size() + " / " 
					+ numTokensI + " TOKENS!");
		}
		
		if (setTokenUponRetrivalB) { 
			boolean successB = setTokens(true, returnList); 
			Assert.assertTrue(
					"failure detected in setting tokens!", successB);
		}
		
		if (this.reportMetricsB) { this.updateRateM.mark(); }
		return returnList;	
	}
		
	
	/**
	 * sets a token state in the block chain at a given index.
	 * 
	 * @param tokenB what you want the token to be set to
	 * @param tokenL the index at which you want the token set
	 * @return boolean indicating success or failure.
	 */
	public boolean setTokens(boolean valueB, List<Long> tokenID_List) {
		boolean successB = false;
		
		List<Integer[]> indexArrayList = 
				tokenID_List.stream()
							.parallel()
							.map(x -> tokenID_to_IndexArray(x))
							.collect(Collectors.toList());
		
		// figure out how many data blocks this set of tokenIDs addresses
		Set<Integer> blockID_Set = new HashSet<Integer>();
		
		indexArrayList.stream()
					  .parallel()
					  .forEach(x -> blockID_Set.add(x[0]));
		
		for (int blockID_I : blockID_Set) {
			boolean lockAccquiredB = getLock(blockID_I);
			if (!lockAccquiredB) { return successB; }	
			byte[] dataARR = readBlock(blockID_I);
		
			List<Integer[]> tokenIndexArrayList = 
					indexArrayList.stream()
						  		  .parallel()
						  		  .filter(x -> x[0] == blockID_I)
						  		  .collect(Collectors.toList());
						  
			dataARR = setTokensInBlock(tokenIndexArrayList, dataARR, valueB);
			successB = trySetData(dataARR, blockID_I);
			successB = releaseLock(blockID_I);
		}
		
		return successB;
	}	
	

	
	/**
	 * utility method that attempts to serialize a block to the 
	 * zookeeper ensemble. 
	 * 
	 * *NOTE* this method assumes the lock for the provided blockID has already
	 * been obtained. It is the responsibility to acquire/release the lock
	 * prior to and after invoking this method!
	 * 
	 * @param payloadARR
	 * @return boolean indicating operation success or failure.
	 */
	public boolean trySetData(byte[] payloadARR, int blockID_I) {
		if (payloadARR.length != this.BLOCK_SIZE_IN_BYTES) {
			throw new IllegalStateException(
				"payloadARR length must be " + this.BLOCK_SIZE_IN_BYTES + "!");
		}
		
		checkLockListSize(blockID_I);
		boolean successB  = false;
		String fullZkPathS = zkNodeBasePathS + "/" + blockID_I;
		
		InterProcessMutex processLock = lockList.get(blockID_I);
		
		
		if (!processLock.isAcquiredInThisProcess()) {
			throw new IllegalStateException("lock must be accquired by this "
					+ "process prior to read attempt!");
		}
		
		try {
			this.curatorFramework.blockUntilConnected(3, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			LOGGER.error("ERROR CONNECTING TO ZOOKEEPER!", e);
			
		}
		
		
		try {
			this.curatorFramework.setData()
								 .forPath(fullZkPathS, payloadARR);	
			successB = true;
		} catch (Exception e) {
			LOGGER.error("ERROR SERIALIZING DATA BLOCK! ", e);
		}
		
		return successB;
		
	}
	

	/**
	 * utility method that returns the data block associated with a 
	 * given block ID. 
	 * 
	 * *NOTE* this method assumes the lock for the provided blockID has already
	 * been obtained. It is the responsibility to acquire/release the lock
	 * prior to and after invoking this method!
	 * 
	 * @param blockID_I
	 * @return the datablock associated with the provided block ID
	 */
	public byte[] readBlock(int blockID_I) {
		checkLockListSize(blockID_I);
		byte[] returnARR = null;
		String fullZkPathS = zkNodeBasePathS + "/" + blockID_I;
		InterProcessMutex processLock = lockList.get(blockID_I);	
		
		if (!processLock.isAcquiredInThisProcess()) {
			throw new IllegalStateException("lock must be accquired by this "
					+ "process prior to read attempt!");
		}
		
		try {
			this.curatorFramework.blockUntilConnected(3, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			LOGGER.error("ERROR CONNECTING TO ZOOKEEPER!", e);
		}
				
		
		try {			
			returnARR = this.curatorFramework
								 .getData()
								 .forPath(fullZkPathS);			
		} catch (Exception e) {
			LOGGER.error("ERROR READING BLOCK!", e);
		}
		
		return returnARR;
	}
	
	
	
	/**
	 * attempts to acquire the lock associated with the supplied index value.
	 * 
	 * @param lockIndexI
	 * @return boolean indicating success or failure of operation.
	 */
	public boolean getLock(int lockIndexI) {
		boolean successB = false;
		InterProcessMutex lock = this.lockList.get(lockIndexI);
		
		try {
			successB = lock.acquire(5, TimeUnit.MINUTES);
		} catch (Exception e) {
			LOGGER.error("ERROR SECURING LOCK!", e);
		}
		
		return successB;
		
	}
	
	
	/**
	 * attempts to relase the lock associated with the supplied index value.
	 * 
	 * @param lockID_I
	 * @return boolean indicating success or failure of operation.
	 */
	public boolean releaseLock(int lockID_I) {
		InterProcessMutex processLock = this.lockList.get(lockID_I);
		boolean successB = false;
		
		try {			
			processLock.release();
			successB = true;
		} catch (Exception e) {
			LOGGER.error("ERROR RELEASING LOCK!", e);
		}
		
		return successB;
	}
	
	
	
	/**
	 * utility method that translates a token index into an index
	 * array containing the block, byte (with respect to the block) and bit
	 * (with respect to the byte) indexes.
	 * 
	 * 
	 * @param tokenID_L
	 * @return three element index array
	 */
	public Integer[] tokenID_to_IndexArray(long tokenID_L) {
		Integer[] returnARR = {-1, -1, -1};
		
		if (tokenID_L < -1) {
			LOGGER.error("tokenID_L must be non-negative!");
			return returnARR;
		}
		
		int blockSizeInBytesI = this.BLOCK_SIZE_IN_BYTES;
		long byteNumL = tokenID_L / 8;
		int blockID_I = (int)(byteNumL / blockSizeInBytesI);
		int byteID_I = (int) (byteNumL - (blockID_I * blockSizeInBytesI));
		int bitID_I = (int) (tokenID_L % 8);
		
		returnARR[0] = blockID_I;
		returnARR[1] = byteID_I;
		returnARR[2] = bitID_I;
		return returnARR;
	}
	
	
	
	/**
	 * utility method that translates an index array containing the block, 
	 * byte (with respect to the block) and bit (with respect to the byte) 
	 * indexes into a token index.
	 * 
	 * @param byteBitIndexARR
	 * @return the token ID associated with the given token 
	 */
	public long indexArray_to_tokenID(Integer[] byteBitIndexARR) {
		int blockID_I = byteBitIndexARR[0];
		long blockID_inBitsL = (long)(blockID_I * this.BLOCK_SIZE_IN_BYTES) * 8;
		
		// byte index of selected token * eight bits per byte
		// +
		// bit index of selected token 
		// * 
		// datablock index
		// =
		// ID of available token
		return((byteBitIndexARR[1] * 8) + byteBitIndexARR[2]) + blockID_inBitsL;
	}
	
	
	
	/**
	 * helper method used internally to pitch a fit when a caller attempts to 
	 * read a block or use a lock with an ID greater than what's available. 
	 * 
	 * @param lockID_I
	 */
	private void checkLockListSize(int lockID_I) {
		
		if (lockID_I > this.lockList.size()) {
			throw new IllegalArgumentException(
					"REQUESTED LOCK/BLOCK ID EXCEEDS NUMBER OF LOCKS/BLOCKS!" 
							+ lockID_I + " > " + this.lockList.size());
		}
		
	}
	
	
	/**
	 * does the actual work of communicating with zookeeper, creating the 
	 * datablocks, and setting up the lock list. 
	 * 
	 * @param numBlocksI
	 * @return boolean indicating success or failure 
	 */
	private boolean createBlocks(int numBlocksI) {
		boolean successB  = false;
		
		if (this.numBlocksI != 0) {
			LOGGER.error("THIS OBJECT HAS ALREADY BEEN SETUP TO ADDRESS "
				+ this.numBlocksI + " DATABLOCKS AT: " + 
				this.zkNodeBasePathS + "! TRY EITHER THE DELETE BLOCKS METHOD "
				+ "TO REMOVE EXISTING BLOCKS THAT ARE NO LONGER NEEDED "
				+ "OR RECREATE THIS CLIENT OBJECT W/O SPECIFYING A "
				+ "VALUE FOR NUMBER OF EXISTING BLOCKS. " );
			
			return successB;
		}
		
		try {
			this.curatorFramework.blockUntilConnected(3, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			LOGGER.error("FAILURE CONNECTING TO ZOOKEEPER!", e);
			return successB;
		}
		
		
		for (int i = 0; i < numBlocksI; i ++) {
			String fullZkPathS = zkNodeBasePathS + "/" + i;
			
			try {	
				
				this.curatorFramework.create()
									 .creatingParentContainersIfNeeded()
									 .forPath(fullZkPathS);
				
			} catch (Exception e) {
				LOGGER.error("FAILURE CREATING NODES!", e);
				return successB;
			}	
			
		}
		
		this.numBlocksI = numBlocksI;
		successB = populateLockList();
		return successB;
	}

	
	/**
	 * after blocks have been created they need to be populated. this populates
	 * the blocks with byte[this.BLOCK_SIZE_IN_BYTES] worth of 0x00
	 * 
	 * @return
	 */
	private boolean populateBlocksWithAvailableTokens() {
		boolean successB = false;
		byte[] dataARR = new byte[this.BLOCK_SIZE_IN_BYTES];
		
		IntStream.range(0, BLOCK_SIZE_IN_BYTES)
				 .parallel()
				 .forEach(x -> dataARR[x] = 0);
		
		for (int i = 0; i < this.numBlocksI; i ++) {
			successB = getLock(i);
			if (!successB) { return successB; }
			
			successB = trySetData(dataARR, i);
			if (!successB) { return successB; }
			
			successB = releaseLock(i);
			if (!successB) { return successB; }
		}
		
		successB = true;
		return successB;
	}
	
	
	
	
	/**
	 * helper method used when initializing blocks. in cases where the tokens
	 * are a map to available tokens in another datastore, this ensures 
	 * parity between the two token sets by ensuring both have the same number
	 * of addressable tokens. 
	 * 
	 * @param tokenID_L
	 * @return boolean indicating success or failure.
	 */
	private boolean setAllTokensThisAndAbove(long tokenID_L) {
		
		// size of each block in bits
		// *
		// total number of blocks
		// - 
		// 1 <-- to account for indexing starting at zero
		// ------
		// largest addressable token 
		long lastTokenID_L = 
				(this.BLOCK_SIZE_IN_BYTES * 8 ) * (this.numBlocksI) - 1;
		
		List<Long> tokenID_List = LongStream.range(tokenID_L, lastTokenID_L)
									        .boxed()
				  					        .collect(Collectors.toList());
		
		boolean successB = setTokens(true, tokenID_List);
		return successB;
	}
	
	
	/**
	 * helper method that creates a map of meta data values for the number 
	 * of blocks in the chain, the total number of available tokens, 
	 * and the time/date when the block chain was created. 
	 * 
	 * @param blockCount
	 * @param tokenCount
	 * @return map containing the metadata k,v pairs
	 */
	private Map<String, String> createBasicMetaDataMap(int blockCount, 
			                                           long tokenCount) {
		
		Map<String, String> metaDataValueMap = new HashMap<String, String>();

		String keyNameS = MetadataKeys.BLOCK_COUNT.toString();
		String valueS = Integer.toString(blockCount);	
		metaDataValueMap.put(keyNameS, valueS);
		
		keyNameS = MetadataKeys.TOKEN_COUNT.toString();
		valueS = Long.toString(tokenCount);
		metaDataValueMap.put(keyNameS, valueS);
		
		keyNameS = MetadataKeys.DATE_CREATED.toString();
	    Calendar currentDate = Calendar.getInstance();
	    SimpleDateFormat formatter= new SimpleDateFormat("dd-MM-YYYY-hh:mm:ss");
	    valueS = formatter.format(currentDate.getTime());
		metaDataValueMap.put(keyNameS, valueS);
		
		return metaDataValueMap;
	}
	
	
	/**
	 * helper method used when initializing blocks. sets <k,v> pairs as 
	 * subpaths of zkNodeBasePath/meta/. so the full path of a given <k,v>
	 * would be [zkNodeBasePath]/meta/[k]
	 * 
	 * @param metaDataValueMap
	 * @return boolean indicating success or failure.
	 */
	private boolean setMetaData(Map<String, String> metaDataValueMap) {
		boolean successB = false;
		
		try {
			this.curatorFramework.blockUntilConnected(3, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			LOGGER.error("FAILURE CONNECTING TO ZOOKEEPER!", e);
			return successB;
		}
				
		for (String keyS: metaDataValueMap.keySet()) {
			String valueS = metaDataValueMap.get(keyS);
			byte[] valueARR = valueS.getBytes();
			String fullZkPathS = zkNodeBasePathS + "/meta/" + keyS;

			try {			
				this.curatorFramework.create()
									 .creatingParentContainersIfNeeded()
									 .forPath(fullZkPathS);
				
				this.curatorFramework.setData().forPath(fullZkPathS, valueARR);			
			} catch (Exception e) {
				LOGGER.error("FAILURE CREATING NODES!", e);
				return successB;
			}			
			
		}
		
		successB = true;
		return successB;
	}
	
	

	/**
	 * takes a datablock ID, a list of bytes with available tokens, and the
	 * List -o- lists of available tokens at each byte and aggregates them into
	 * a list of available tokens index arrays. NOTE - the byteID_List and the
	 * availableTokenIndexList are cross-indexed.
	 * 
	 * TODO get rid of this @#$@!#$%#@$%$#@#$% nested loop
	 * 
	 * @param blockID_I
	 * @param byteID_List
	 * @param availableTokenIndexList
	 * @return
	 */
	private List<Integer[]> buildTokenIndexArrayList (
								int blockID_I,
								List<Integer> byteID_List,
								List<List<Integer>> availableTokenIndexList) {
			
		List<Integer[]> returnAL = new ArrayList<Integer[]>();
		
		for (int i = 0; i < byteID_List.size(); i ++) {

			List<Integer> availableTokenIndexElementL = 
					availableTokenIndexList.get(i);
			
			for (int k = 0; k < availableTokenIndexElementL.size(); k++) {
				Integer[] indexArrayElement = new Integer[3];
				indexArrayElement[0] = blockID_I;
				indexArrayElement[1] = byteID_List.get(i);	
				indexArrayElement[2] = availableTokenIndexList.get(i).get(k);
				returnAL.add(indexArrayElement);
			}

		}					 

		return returnAL;		
	}
	
	
	
	/**
	 * creates the DistributedAtomicLong object and associated curator client
	 * necessary to create/update the distributed counter. also populates 
	 * the BitMaskList and the TokenLocationMap.
	 */
	private void setupClient() {
		
		this.curatorFramework = CuratorFrameworkFactory.newClient(
						this.zkConnectString, 
						new RetryUntilElapsed(3000, 1000));

		this.curatorFramework.start();	
		buildBitMaskList();
		buildTokenLocationMap();
	}
	
	
	
	
	/**
	 * creates the lock keychain necessary to allow for more than one process 
	 * to r/w blocks in the chain. this method should be run only after the 
	 * blocks have been initialized. 
	 *  
	 * @return boolean indicating success or failure
	 */
	private boolean populateLockList() {
		this.lockList.clear();
		boolean successB  = false;
		
		try {
			this.curatorFramework.blockUntilConnected(3, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			LOGGER.error("ERROR CONNECTING TO ZOOKEEPER!", e);
			return successB;
		}
		
		try {
	
			for (int i = 0; i < this.numBlocksI; i ++) {
				String fullZkPathS = zkNodeBasePathS + "/" + i + "/lock";
				
				InterProcessMutex lock = 
					new InterProcessMutex(this.curatorFramework, fullZkPathS);
				
				lockList.add(lock);
			}
			
			successB = true;
			
		} catch (Exception e) {
			LOGGER.error("ERROR POPULATING LOCK LIST!", e);
		}

	
		return successB;
	}
	
	
	
	
	/**
	 * performs the actual work of setting/unsetting a given token
	 * 
	 * @param tokenID_ARR
	 * @return boolean indicating success or failure.
	 */
	private byte[] setTokensInBlock(List<Integer[]> tokenIndexArrayList, 
								   byte[] dataARR,
								   boolean valueB) {

		// group by byteID so we can operate on the groups in parallel but
		// the members of the group in serial 
		Map<Integer, List<Integer[]>> byteID_to_tokenIndexArrayMap = 
				tokenIndexArrayList.stream()
					   		   	   .parallel()
					   		   	   .collect(Collectors.groupingBy(x -> x[1]));


										//          o
		byteID_to_tokenIndexArrayMap	//    _ 0   .-----\-----.  ,_0 _
			.entrySet()					//  o' / \  |\     \     \    \ `o
			.parallelStream()			//  __|\___ |_`-----\-----`__ /|____
			.forEach(x -> x.getValue()  //    / |      |          |  | \
						   .stream()    //             |          |
						   .forEach(y -> setTokensHelper(y, dataARR, valueB)));
	
		return dataARR;

	}
	
	
	
	/**
	 * helper method for setTokensInBlock() that cleans up the code and 
	 * enables parallel processing of the list of tokenIndexArrays
	 * 
	 * @param tokenIndexARR
	 * @param dataARR
	 * @param valueB
	 * @return
	 */
	private byte[] setTokensHelper(Integer[] tokenIndexARR, 
										  byte[] dataARR, 
										  boolean valueB) {
	
		if (tokenIndexARR.length != 3) {
			throw new IllegalArgumentException(
					"tokenID array should have exactly three members!");
		}

		int byteID_I = tokenIndexARR[1];
		int bitID_I = tokenIndexARR[2];
		int byteMaskI = this.byteMaskARR[bitID_I];
		
		// invert the mask if we're looking to unset a token
		// eg 0001000 -> 1110111 
		byteMaskI = (valueB) ? (byteMaskI) : (~byteMaskI);
	
		if (valueB) {
			dataARR[byteID_I] = 
					(byte) ((dataARR[byteID_I] & 0xFF) | (byteMaskI & 0xFF));			
		}
		else {
			dataARR[byteID_I] = 
					(byte) ((dataARR[byteID_I] & 0xFF) & (byteMaskI & 0xFF));
		}
		
		return dataARR;
	}
	
	

	
	/**
	 * utility method that translates a blockID and block array into an index
	 * array containing the block, byte (with respect to the block) and bit
	 * (with respect to the byte) indexes for an UNSET token. 
	 * 
	 * @param blockID_I
	 * @param dataARR
	 * @param numI limits the number of BYTES (not tokens) searched for tokens
	 * @return
	 */
	private List<Integer[]> getTokenIdFromBlock (
			int blockID_I, byte[] dataARR, int numI) {
		
		List<Integer[]> returnList = new ArrayList<Integer[]>();	
		
		// finds indexes of a dataARR member that has a free token slot
		List<Integer> byteID_List = IntStream.range(0, dataARR.length)
											 .parallel()
											 .filter(i -> dataARR[i] != -1)
											 .limit((long)numI)				      
											 .boxed()
											 .collect(Collectors.toList());
			
		// nothing found in this entire block - return empty result list
		if (byteID_List.size() == 0) { return returnList; }	
		
		// creates a second list indicating where in a given byte the free
		// tokens are
		List<List<Integer>> availableTokenIndexList = 
				byteID_List.stream()
						   .parallel()
						   .map(x -> this.tokenLocationMap.get(dataARR[x]))
						   .collect(Collectors.toList());		
		
		// convert the index lists into tokenIDs
		returnList = buildTokenIndexArrayList (
				blockID_I, byteID_List, availableTokenIndexList);

		return returnList;
	}	
	
	
	/**
	 * randomly selects the first available lock from the lock list, 
	 * acquires it, and returns the index of the acquired lock.
	 * 
	 * @return the lock ID of the acquired lock. if (-1), no lock was acquired.
	 */
	private int getAvailableLock() {
		boolean foundB = false;
		int lockID_I = -1;
		int numGetAttemptsI = 1;
		Random randy = new Random();
		
		while ((!foundB) && (numGetAttemptsI < this.MAX_GET_ATTEMPTS_I)) {
			lockID_I = randy.nextInt(this.lockList.size());
			InterProcessMutex lock = this.lockList.get(lockID_I);
			
			try {
				foundB = lock.acquire(0, TimeUnit.SECONDS);
			} catch (Exception e) {
				LOGGER.error("ERROR SECURING LOCK!", e);
			}		
			
			numGetAttemptsI ++;
		}
		
		return lockID_I;
	}
	
	
	/**
	 * randomly selects the first available lock from the lock list that isn't
	 * a member of [excludeLockSet], acquires it, and returns the index of the
	 * acquired lock.
	 * 
	 * @param excludeLockSet
	 * @return the lock ID of the acquired lock. if (-1), no lock was acquired.
	 */
	private int getAvailableLock(Set<Integer> excludeLockSet) {
		boolean foundB = false;
		int lockID_I = -1;
		int numGetAttemptsI = 1;
		Random randy = new Random();
		Set<Integer> localExcludeLockSet = new HashSet<Integer>();
		localExcludeLockSet.addAll(excludeLockSet);
							
		while ((!foundB) && (numGetAttemptsI < this.MAX_GET_ATTEMPTS_I)) {
			boolean lockOkToUseB = false;
			
			while (!lockOkToUseB) {
				
				// return (-1) if all valid locks have been exhausted
				if (localExcludeLockSet.size() >= this.lockList.size()) {
					LOGGER.warn(
						"lock exclusion set encompasses all available locks!");
					return -1;
				}					
				
				lockID_I = randy.nextInt(this.lockList.size());
				
				// add lock to exclude set so if we do a full iteration on the
				// outer while loop and end up back here we don't retry the 
				// same lockID
				lockOkToUseB = localExcludeLockSet.add(lockID_I);			
			}
			
			InterProcessMutex lock = this.lockList.get(lockID_I);
			
			try {
				foundB = lock.acquire(0, TimeUnit.SECONDS);
			} catch (Exception e) {
				LOGGER.error("ERROR SECURING LOCK!", e);
			}		
			
			numGetAttemptsI ++;
		}
		
		return lockID_I;
	}
	
	

	/**
	 * builds an array of eight binary masks used by methods that set/unset
	 * individual tokens in data blocks
	 * 
	 */
	private void buildBitMaskList() {
		
		this.byteMaskARR = new int[8];
		int valByteI = 0b10000000;

		for (int i = 0; i < 8; i ++) {
			this.byteMaskARR[i] = valByteI;
			valByteI = (byte)(valByteI >>> 1);
		}	
		
	}

	
	
	
	/**
	 * creates a map<k,v> where <k> = all possible byte values and <v> =  
	 * int[] with the indexes of all the zeros (ie - free tokens) in the 
	 * binary representation of that byte. used to quickly find free tokens
	 * in a datablock. 
	 * 
	 */
	private void buildTokenLocationMap() {
		
		for (int i = -128; i < 128; i ++) {
			String byteTokenS = Integer.toBinaryString((byte)i & 0xFF);
			
			String byteTokenAsBitStreamS = 
					String.format("%8s", byteTokenS).replace(' ', '0');

			char[] byteTokenAsCharARR = byteTokenAsBitStreamS.toCharArray();
			
			List<Integer> indexList = 
					IntStream.range(0, byteTokenAsBitStreamS.length())
					     	 .filter(x -> byteTokenAsCharARR[x] == '0')
					 		 .boxed()
					 		 .collect(Collectors.toList());
			
			this.tokenLocationMap.put((byte)i, indexList);
		}
			
	}
	
	 
	
	
	

}










