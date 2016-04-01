package com.moesol.actm.zookeeperTokenClient;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.framework.recipes.atomic.PromotedToLock;
import org.apache.curator.framework.recipes.atomic.PromotedToLock.Builder;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.retry.RetryUntilElapsed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;


/**
 * client for grabbing atomic long IDs from zookeeper. 
 * 
 * @TODO add capability for caller to specify retry and metrics options
 * @TODO add caller supplied variable checking 
 * 
 * @author snerd
 *
 */
public class ZookeeperDistributedAtomicLongClient {
	
	// ip:port of zookeeper instance
	private String zkConnectString;
	
	// zNode name you'd like the long to be stored at
	private String zkNodePathS;
	
	// the metrics sensor used to measure rate of updates
	private Meter updateRateM;
	
	// the actual long object to be incremented
	private DistributedAtomicLong atomicLongDAL;
	
	// the curator-wrapped zookeeper client object
	private CuratorFramework curatorFramework;
	
	// the client interface to graphite
	private GraphiteReporter graphiteReporter;
	
	// caller selectable option to enable/disable metrics reporting
	private boolean reportMetricsB = false;
	
	// used internally to detect whether or not the metrics stuff is initialized
	private boolean metricsSetupB = false;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(
			ZookeeperDistributedAtomicLongClient.class.getName());
	
	
	/**
	 * private constructor 
	 */
	private ZookeeperDistributedAtomicLongClient(){};
	
	
	/**
	 * constructor for setting up both metrics reporting and the curator client
	 * 
	 * @param zkConnectString
	 * @param zkNodePath
	 * @param updateRate
	 * @param graphiteIP
	 * @param metricsNamePrefix
	 * @param reportMetrics
	 */
	public ZookeeperDistributedAtomicLongClient(String zkConnectString, 
										 		String zkNodePath, 
										 		String graphiteIP,
										 		String metricsNamePrefix,
										 		boolean reportMetrics) {
		
		this.zkConnectString = zkConnectString;
		this.zkNodePathS = zkNodePath;
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
	    createAtomicLong();
	}
	
	
	/**
	 * constructor for setting up only the curator client
	 * 
	 * @param zkConnectString
	 * @param zkNodePath
	 */
	public ZookeeperDistributedAtomicLongClient(String zkConnectString, 
			 							 		String zkNodePath) {
		this.zkConnectString = zkConnectString;
		this.zkNodePathS = zkNodePath;
		
		createAtomicLong();
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
	 *
	 * @return
	 */
	public boolean getIsMetricsSet() {
		return this.metricsSetupB;
	}
	
	
	/**
	 * creates the DistributedAtomicLong object and associated curator client
	 * necessary to create/update the distributed counter.
	 */
	private void createAtomicLong() {
		
		this.curatorFramework = CuratorFrameworkFactory.newClient(
						this.zkConnectString, 
						new RetryUntilElapsed(3000, 1000));

		this.curatorFramework.start();
		
		Builder builder = PromotedToLock
				.builder()
				.lockPath(this.zkNodePathS + "/lock")
				.retryPolicy(new ExponentialBackoffRetry(10, 10));

		this.atomicLongDAL = new DistributedAtomicLong(this.curatorFramework, 
													   this.zkNodePathS, 
													   new RetryNTimes(5, 20), 
													   builder.build());
	}
	
	
	
	/**
	 * returns an updated counter from zookeeper.
	 * @return
	 */
	public long getNewCounter() {
		
		long returnValL = -1;
		
		try {
			this.curatorFramework.blockUntilConnected();
			AtomicValue<Long> maybeUpdatedAV = this.atomicLongDAL.increment();
			
			if (this.atomicLongDAL.get().succeeded()) {
				returnValL = maybeUpdatedAV.postValue();
				System.out.println(maybeUpdatedAV.postValue());
				if (this.reportMetricsB) { this.updateRateM.mark(); }
			}
			
		} catch (Exception e) {
			LOGGER.error("error detected getting atomic long!", e);
		}
		
		
		return returnValL;
	}
	
	

	/**
	 * attempts to set a new DistributedAtomicLong value to (long)value
	 * @return
	 */
	public boolean initializeCounter(long value) {
		boolean returnValB = false;
		
		try {
			this.curatorFramework.blockUntilConnected();
			returnValB = this.atomicLongDAL.initialize(value);
		} catch (Exception e) {
			LOGGER.error("error detected initializing counter!", e);
		}
		
		return returnValB;
	}


	/**
	 * attempts to set an existing counter to (long)value
	 * @return
	 */
	public AtomicValue<Long> trySetCounter(long value) {
		AtomicValue<Long> maybeUpdatedAtomicValue = null;;
		
		try {
			this.curatorFramework.blockUntilConnected();
			
			maybeUpdatedAtomicValue = 
					this.atomicLongDAL.trySet(value);
			
		} catch (Exception e) {
			LOGGER.error("error detected setting counter", e);;
		}
		
		return maybeUpdatedAtomicValue;
	}
	
	
}







