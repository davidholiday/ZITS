package com.moesol.actm.zookeeper.JBehave.steps;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import org.junit.Assert;
import org.jbehave.core.annotations.AfterScenario;
import org.jbehave.core.annotations.AfterStory;
import org.jbehave.core.annotations.BeforeScenario;
import org.jbehave.core.annotations.Given;
import org.jbehave.core.annotations.Named;
import org.jbehave.core.annotations.Then;
import org.jbehave.core.annotations.When;
import org.jbehave.core.steps.Steps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moesol.actm.zookeeperTokenClient
	.ZITS;


/**
 * Behavior Driven Development step file (ie - the actual guts of the testing)
 * for ZITS class.
 * @author snerd
 *
 */
public class ClusterClientSteps extends Steps {
	private static final Logger LOGGER = 
			LoggerFactory.getLogger(ClusterClientSteps.class);
	
	private ZITS zkClusterClient;
	private int numBlocksI;
	private int numTokensI;
	private int numBlocksTesterA_I;
	private int numBlocksTesterB_I;
	private boolean deleteSuccessB = false;
	private boolean doubleInitializeFailureB = false;
	private List<byte[]> serializedBlockList = new ArrayList<byte[]>();
	private List<Long> generatedTokenList = new ArrayList<Long>();
	private List<Long> returnedTokenList = new ArrayList<Long>();
	private String zkConnectS = "localhost:32810";
	private String zkNodeBasePathS = "/OTM/AtomicLong/cluster";

	
	
	@AfterScenario
	public void cleanup() {	
		this.numBlocksTesterA_I = 0;
		this.numBlocksTesterB_I =0;
		this.deleteSuccessB = false;
		this.doubleInitializeFailureB = false;
		this.serializedBlockList.clear();
		this.generatedTokenList.clear();
		this.returnedTokenList.clear();
		boolean successB = this.zkClusterClient.deleteBlocks();
		
		if (!successB) {
			LOGGER.error("failure detected in deleting blocks!");
		}
	}
	
	
	@Given("a zk atomic long cluster client")
	public void createZkClusterClient() {
		this.zkClusterClient = new ZITS(this.zkConnectS, this.zkNodeBasePathS);				
	}
	
	
	@When("the caller initializes $numTokensI tokens")
	public void initializetokens(@Named("numTokensI") int numTokensI) {
		
		boolean successB = 
				this.zkClusterClient.initializeDataBlocks(numTokensI);
		
		Assert.assertTrue("failure detected in initializing tokens!", successB);
	}
	
	
	
	@Then("an operation requesting $numTokensI free tokens succeeds")
	public void checkGetTokenWorks(@Named("numTokensI") int numTokensI) {
		
		List<Long> availableTokenList = 
				this.zkClusterClient.getAvailableTokens(false, numTokensI);
		
		Assert.assertEquals("incorrect number of tokens retrieved!", 
				numTokensI, availableTokenList.size());
	}
	
	
	
	@Then("all tokens above index $tokenIndexI are set to unavailable")
	public void checkAutoSetTokens(@Named("tokenIndexI") int tokenIndexI) {
		
		Integer[] tokenIndexARR = 
				this.zkClusterClient.tokenID_to_IndexArray(tokenIndexI);
		
		int blockID_I = tokenIndexARR[0];
		int byteID_I = tokenIndexARR[1];
		int bitID_I = tokenIndexARR[2];
		
		this.zkClusterClient.getLock(blockID_I);
		byte[] dataBlockARR = this.zkClusterClient.readBlock(blockID_I);
		this.zkClusterClient.releaseLock(blockID_I);
		
		// check the byte containing the max addressable token
		byte firstByte = dataBlockARR[byteID_I];
		
		String dataByteAsBitString = 
				String.format("%8s", Integer.toBinaryString(firstByte & 0xFF))
					.replace(' ', '0');
		
		for (int i = bitID_I; i < dataByteAsBitString.length(); i ++) {
			char bitC = dataByteAsBitString.charAt(i);
			
			Assert.assertTrue(
					"bit not set that's supposed to be set!", bitC == '1');
		}
		
		// now check the tail of that list - all of which should be xFF
		List<Byte> dataBlockList = new ArrayList<Byte>();
		for (byte b : dataBlockARR) { dataBlockList.add(b); }
		
		dataBlockList = 
				dataBlockList.subList(byteID_I + 1, dataBlockList.size() - 1);
		
		dataBlockList = dataBlockList.stream()
					 				 .filter(x -> x != -1)
					 				 .collect(Collectors.toList());
		LOGGER.info(Arrays.toString(dataBlockList.toArray()));
		Assert.assertTrue(
				"tokens above threshhold are unset when they shouldn't be!", 
				dataBlockList.size() == 0);		 
	}
	
	
	
	
	@When("the caller initializes $numBlocksI blocks twice")
	public void doubleInitializeBlocks(@Named("numBlocksI") int numBlocksI) {
		this.numBlocksI = numBlocksI;
		boolean successB = this.zkClusterClient.initializeBlocks(numBlocksI);
		Assert.assertTrue("failure detected in initializing blocks!", successB);
		
		successB = this.zkClusterClient.initializeBlocks(numBlocksI);
		
		this.doubleInitializeFailureB = (successB) ? (false) : (true);
	}

	
	
	@Then("the second init operation fails")
	public void checkInitFailFlag() {
		Assert.assertTrue(
				"second block initialization call succeeded "
				+ "when it should have failed!", this.doubleInitializeFailureB);
	}
	
	
	
	@When("the caller initializes $numBlocksI blocks")
	public void initializeBlocks(@Named("numBlocksI") int numBlocksI) {
		this.numBlocksI = numBlocksI;
		boolean successB = this.zkClusterClient.initializeBlocks(numBlocksI);
		Assert.assertTrue("failure detected in initializing blocks!", successB);
	}
	
	
	
	@Then("the correct number of empty zNodes are created")
	public void checkZnodeCreation() {
			
		for (int i = 0; i < this.numBlocksI; i ++) {
			this.zkClusterClient.getLock(i);
			byte[] blockARR = this.zkClusterClient.readBlock(i);
			this.zkClusterClient.releaseLock(i);
			
			Assert.assertNotNull(
				"failure detected in reading initialized blocks!", blockARR);
		}
		
	}
	
	
	
	@When("the caller serializes $numBlocksI blocks of random data")
	public void serializeBlocks(@Named("numBlocksI") int numBlocksI) {
		
		for (int i = 0; i < numBlocksI; i ++) {
			byte[] blockARR = 
					new byte[this.zkClusterClient.BLOCK_SIZE_IN_BYTES];
			
			new Random().nextBytes(blockARR);
			
			this.zkClusterClient.getLock(i);
			boolean successB = this.zkClusterClient.trySetData(blockARR, i);
			this.zkClusterClient.releaseLock(i);
			
			Assert.assertTrue(
					"failure detected in serializing blocks!", successB);
			
			serializedBlockList.add(blockARR);
		}
		
	}
	
	
	
	@Then("the $numBlocksI blocks are not empty")
	public void checkBlocksNotEmpty(@Named("numBlocksI") int numBlocksI) {
		
		for (int i = 0; i < this.numBlocksI; i ++) {
			this.zkClusterClient.getLock(i);
			byte[] blockARR = this.zkClusterClient.readBlock(i);
			this.zkClusterClient.releaseLock(i);
			
			Assert.assertEquals(
				"failure detected in reading initialized blocks!", 
				this.zkClusterClient.BLOCK_SIZE_IN_BYTES,
				blockARR.length);
		}
		
	}
	
	
	
	@Then("the $numBlocksI blocks are identical to the serialized blocks")
	public void checkReadBlockIntegrity(@Named("numBlocksI") int numBlocksI) {
		
		for (int i = 0; i < this.numBlocksI; i ++) {
			this.zkClusterClient.getLock(i);
			byte[] blockARR = this.zkClusterClient.readBlock(i);
			this.zkClusterClient.releaseLock(i);
			
			Assert.assertArrayEquals(
				"data integrity error detected in retrieved blocks!", 
				this.serializedBlockList.get(i),
				blockARR);
		}
		
	}
	
	
	
	@When("the caller deletes the blocks")
	public void deleteBlocks() {
		this.deleteSuccessB = this.zkClusterClient.deleteBlocks();
	}
	
	
	
	@Then("there are no remaining blocks in zookeeper")
	public void checkDeleteBlock() {
		Assert.assertTrue(
				"failure detected in deleting blocks!", this.deleteSuccessB);
	}
	

	
	@Given("$numBlocksI serialized blocks of random data")
	public void initializeAndSerializeRandom(
			@Named("numBlocksI") int numBlocksI) {
		
		initializeBlocks(numBlocksI);
		serializeBlocks(numBlocksI);
	}
	
	
	
	@When("the caller gets $numTokensI tokens")
	public void getTokens(@Named("numTokensI") int numTokensI) {
		this.numTokensI = numTokensI;
		
		this.returnedTokenList = 
				this.zkClusterClient.getAvailableTokens(false, numTokensI);
	}
	
	

	@Then("all the returned tokens are independently retrieved as valid")
	public void checkReturnedTokens() {
		
		Assert.assertEquals(
				"returned token list is not what it should be!", 
				this.numTokensI, 
				this.returnedTokenList.size());	
		
		for (long tokenL : this.returnedTokenList) {
			
			Integer[] tokenIndexARR = 
				this.zkClusterClient.tokenID_to_IndexArray(tokenL);
			
			int blockID_I = tokenIndexARR[0];
			int byteID_I = tokenIndexARR[1];
			int bitID_I = tokenIndexARR[2];
			
			this.zkClusterClient.getLock(blockID_I);
			byte[] dataARR = this.zkClusterClient.readBlock(blockID_I);
			this.zkClusterClient.releaseLock(blockID_I);
			int dataByteI = dataARR[byteID_I];
			
			String dataByteAsBitString = 
				String.format("%8s", Integer.toBinaryString(dataByteI & 0xFF))
					.replace(' ', '0');

			char dataBitC = dataByteAsBitString.charAt(bitID_I);
			
			Assert.assertEquals(
					"returned token isn't actually mapped as available!", 
					'0', 
					dataBitC);
		}
		
	}
	
	
	
	@Given("$numRandomTokenIDs randomly generated tokenIDs")
	public void generateRandomTokenIDs(
			@Named("numRandomTokenIDs") int numRandomTokenIDs_I) {
				
		this.numTokensI = numRandomTokenIDs_I;
		long ceilingL = this.numBlocksI 
							* this.zkClusterClient.BLOCK_SIZE_IN_BYTES 
							* 8;
		
		for (int i=0; i < numRandomTokenIDs_I; i++) {
			long tokenID_L = ThreadLocalRandom.current().nextLong(ceilingL);
			this.generatedTokenList.add(tokenID_L);
		}
		
	}
	
	
	
	@When("the caller converts the tokenIDs to index arrays and back again")
	public void convertAndUnConvertTokenID() {
		
		for (long tokenID_L : this.generatedTokenList) {
			
			Integer[] convertedTokenID_ARR = 
					this.zkClusterClient
					    .tokenID_to_IndexArray(tokenID_L);

			long unConvertedTokenID_L = 
					this.zkClusterClient
					    .indexArray_to_tokenID(convertedTokenID_ARR);
			
			this.returnedTokenList.add(unConvertedTokenID_L);
		}
		
	}
	
	
	
	@Then("both sets of tokenIDs should match")
	public void checkTokenLists() {		
		
		for (long generatedTokenL : this.generatedTokenList) {
			Assert.assertTrue("tokenID conversion error detected!", 
					this.returnedTokenList.contains(generatedTokenL));
		}
		
	}
	
	
	
	@When("the caller sets those tokens")
	public void setTokenTaken() {
		this.zkClusterClient.setTokens(true, this.returnedTokenList);	
	}
	
	
	@Then("the tokens are verified as set")
	public void checkTokenSet() {
		
		for (long tokenID_L : this.returnedTokenList) {
			
			Integer[] indexARR = 
					this.zkClusterClient
						.tokenID_to_IndexArray(tokenID_L);
			
			int blockID_I = indexARR[0];
			int byteID_I = indexARR[1];
			int bitID_I = indexARR[2];
			
			this.zkClusterClient.getLock(blockID_I);
			byte[] blockARR = this.zkClusterClient.readBlock(blockID_I);
			this.zkClusterClient.releaseLock(blockID_I);
			byte byteToken = blockARR[byteID_I];
			
			String byteTokenS = 
				String.format("%8s", Integer.toBinaryString(byteToken & 0xFF))
					.replace(' ', '0');

if (!(byteTokenS.charAt(bitID_I) == '1')) {
	LOGGER.warn(blockID_I + " " + byteID_I + " " + bitID_I);
	LOGGER.warn(byteTokenS);
}
			
			
			Assert.assertTrue("token that should be set isn't!",
					byteTokenS.charAt(bitID_I) == '1');
			
		}
		
	}
	
	
	@When("the caller gets $numTokensI tokens that are already set")
	public void getSetTokens(@Named("numTokensI") int numTokensI) {
		
		byte[] blockARR = this.serializedBlockList.get(0);		
	
		for (int i = 0; i < numTokensI; i ++) {
			Integer[] takenTokenIndexARR = 
					getIndexArrayFromBlock(0, blockARR);
			
			long tokenID_L = this.zkClusterClient
					.indexArray_to_tokenID(takenTokenIndexARR);
			
			this.generatedTokenList.add(tokenID_L);
		}
		
		
	}
	
	
	
	@When("the caller unsets those tokens")
	public void unsetTokenThen() {
		this.zkClusterClient.setTokens(false, this.generatedTokenList);
	}
	
	
	
	@Then("the tokens are verified as unset")
	public void checkTokenUnset() {
		
		for (long tokenID_L : this.generatedTokenList) {	
			Integer[] indexARR = 
					this.zkClusterClient
						.tokenID_to_IndexArray(tokenID_L);
			
			int blockID_I = indexARR[0];
			int byteID_I = indexARR[1];
			int bitID_I = indexARR[2];
			
			this.zkClusterClient.getLock(blockID_I);
			byte[] blockARR = this.zkClusterClient.readBlock(blockID_I);
			byte byteToken = blockARR[byteID_I];
			this.zkClusterClient.releaseLock(blockID_I);
			
			String byteTokenS = 
				String.format("%8s", Integer.toBinaryString(byteToken & 0xFF))
					.replace(' ', '0');
			
			Assert.assertTrue("token that should NOT be set is set!",
					byteTokenS.charAt(bitID_I) == '0');
	
		}

		
	}
	

	@When("the caller converts $numBlocksI blocks to token count and back")
	public void convertBlocksToTokenCountandBackAgain(
			@Named("numBlocksI") int numBlocksI) {
		
		this.numBlocksTesterA_I = numBlocksI;
		
		long numTokensL = 
				this.zkClusterClient.getTokenCountForBlockNum(numBlocksI);
		
		this.numBlocksTesterB_I = 
				this.zkClusterClient.getBlockCountforTokenNum(numTokensL);
	}
	
	
	
	@Then("both block counts should be the same size")
	public void checkBlockTokenConversionResults() {
		Assert.assertEquals("error detected in block/token count converters!", 
				this.numBlocksTesterA_I, this.numBlocksTesterB_I);
	}
	
	
	
	
	/**
	 * utility method that translates a blockID and block array into an index
	 * array containing the block, byte (with respect to the block) and bit
	 * (with respect to the byte) indexes for a SET token. 
	 * 
	 * @param dataARR
	 * @return
	 */
	private Integer[] getIndexArrayFromBlock(int blockID_I, byte[] dataARR) {
		Integer[] returnARR = {-1, -1, -1};
		
		// finds an index of a dataARR member that has a free token slot
		int byteID_I = IntStream.range(0, dataARR.length)
							    .filter(i -> dataARR[i] != -1)
							    .findFirst()
								.orElse(-1);
		
		if (byteID_I == -1) { return returnARR; }
		
		byte byteToken = dataARR[byteID_I];
		
		String byteTokenS = 
			String.format("%8s", Integer.toBinaryString(byteToken & 0xFF))
				.replace(' ', '0');
		
		int bitID_I = byteTokenS.indexOf("1");
		returnARR = new Integer[] {blockID_I, byteID_I, bitID_I};		
		return returnARR;
	}
	
	
	
	
}














