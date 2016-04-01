ZITS 
==========================================
zookeeper indefatigable token service
-------------------------------------

----------

what is?
--------
ZITS is a tool that uses [Apache Curator](https://curator.apache.org/) to enable [Apache Zookeeper](https://zookeeper.apache.org/) to be used a high-speed service that provides callers with [n] number of unique tokens per request. Zookeeper is first initialized with however many available tokens the caller wants, which are stored in blocks of 512KB. Support for pre-defining unavailable tokens is provided, thus enabling use as a reference map to a slower data store or restoring previously serialized states. Once initialized, callers need only use the ZITS client to access the token chain.

how to set up?
--------------
Use maven to build:

    mvn clean install

*NOTE* built in tests are disabled by default because I didn't want to assume you'd have zookeeper running when you build this. In order to run the built in tests, build this way instead:

    mvn clean install -DskipTests=false

ZITS also includes a built-in pressure tester that allows you to simulate a number of ZITS clients making simultaneous requests for [n] number of available tokens simultaneously. To set that up, update the following variables at the top of the `main()` method in class `PressureTesterBootstrap`.

    	int threadCountI = *how many ZITS clients you want*
     	int runCountI = *how many get requests each ZIT client should make* 
     	int requestNumPerGetI = *how many tokens each client should request per get*;
     	int numBlocksI = *number of blocks of 512KB tokens zookeeper should be initialized with*
    	String zkConnectS = *ip address and port number of zookeeper ensemble*
    	String zkNodeBasePathS = *base path at which the tokens should be stored. eg /flarp/schmoo/stuff/foo*
 


how to make it go?
------------------
Once you've built the library simply include it as a dependency in your Java project and create a ZITS client. There are four ways to do this to cover all possible answers to the following questions: 

 1. has Zookeeper already been initialized with a token field? 
 2. do I need to send metrics data to a graphite server?

Depending on your needs, create a ZITS client using the appropriate constructor. For example, if you need to set up a token field in Zookeeper and you don't need metrics reporting, you would first create a ZITS client:

    String zkConnectS = "localhost:2181";
    String zkNodeBasePathS = "/OTM/AtomicLong/cluster";
    ZITS zkClusterClient = new ZITS(zkConnectS, zkNodeBasePathS);	

Then you could initialize  Zookeeper with a billion available tokens like this: 
     
    long totalAvailableTokensL = 1000000000;
    zkClusterClient.initializeDataBlocks(totalAvailableTokensL);  

From there, you could retrieve one thousand free tokens (setting them to taken upon retrieval) thusly:

    List<Long> returnedTokensList = zkClusterClient.getAvailableTokens(true, 1000);

Lastly, now that Zookeeper already has a token field set up at a given path, any new ZITS clients you create need to be created using a different constructor. For example:

    String zkConnectS = "localhost:2181";
    String zkNodeBasePathS = "/OTM/AtomicLong/cluster";
    long numTokensL = 1000000000;
    ZITS zkClusterClient = new ZITS(zkConnectS, zkNodeBasePathS, numTokensL);

creates a client that assumes the zookeeper instance at `localhost:2181, `the path` /OTM/AtomicLong/cluster` has an initialized token field of `1000000000` tokens. 


troubleshooting
---------------

 1. If Zookeeper complains that there already exists a node at path `/whatever/path/you/gave/ZITS`, it means you are attempting to initialize a token field where something already exists. If you are confident what's already in zookeeper is something safe to get rid of, you can either go into the zookeeper CLI and do it manually or, if what's there is a ZITS token field, use the `deleteBlocks()` method in the ZITS client. This will remove any ZITS data from that location in Zookeeper. 
 2. If you are using multiple ZITS clients in parallel to read/write blocks manually and are running into funny business, ensure you're using the locking mechanism correctly. When you manually read/write blocks from Zookeeper, you **MUST** acquire and release the lock whose ID matches the block ID you are working with. For example, if you are trying to read block 5, here is how you would do it:

		    int lockAndBlockID_I = 5;
		    this.zkClusterClient.getLock(lockAndBlockID_I);
		    byte[] blockARR = this.zkClusterClient.readBlock(lockAndBlockID_I);
		    this.zkClusterClient.releaseLock(lockAndBlockID_I);


one last thing
--------------
This libary also contains an implenetation of Curator's Distributed Atomic Long recipe. It too has the capability of reporting metrics to a Graphite server. To use, create an instance of class ZookeeperDistributedAtomicLongClient.
