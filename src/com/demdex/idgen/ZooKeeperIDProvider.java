package com.demdex.idgen;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An IDProvider that reads/writes ID generation state data to/from a Hadoop Zookeeper system.
 * 
 * @author D.Rosenstrauch, Demdex Inc.
 * $Revision: 7 $
 * $Date: 2011-05-19 06:09:35 +0000 (Thu, 19 May 2011) $
 * $LastChangedBy: darose $
 * 
 * ======
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *  http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

public class ZooKeeperIDProvider implements IDProvider {

	public ZooKeeperIDProvider(String hostList, int sessionTimeout) {
		this(hostList, sessionTimeout, new NoOpWatcher());
	}

	public ZooKeeperIDProvider(String hostList, int sessionTimeout, Watcher watcher) {
		this.hostList = hostList;
		this.sessionTimeout = sessionTimeout;
		this.watcher = watcher;
		logger = LoggerFactory.getLogger(ZooKeeperIDProvider.class);
	}

	public void open() throws IDGeneratorException {
		try {
			logger.info("Opening connection(s) to Zookeeper(s) at {}", hostList);
			zooKeeper = new ZooKeeper(hostList, sessionTimeout, watcher);
			logger.info("Zookeeper connection(s) open");
		}
		catch (IOException e) {
			throw new IDGeneratorException(e);
		}
	}

	public String getName() {
		return ZooKeeperIDProvider.class.getSimpleName()+": "+hostList;
	}
	
	public RawIDSetData getData(IDCategory category) throws IDGeneratorException {
		logger.trace("Getting data for category {}", category);
		String categoryName = category.getName();
		Stat stat = new Stat();
		byte[] bytes;
		try {
			bytes = zooKeeper.getData(categoryName, false, stat);
		}
		catch(KeeperException e) {
			if (e.code().equals(KeeperException.Code.NONODE)) {
				throw new IDGeneratorException("No id data found for category: "+categoryName);
			}
			throw new IDGeneratorException(e);
		}
		catch(Exception e) {
			throw new IDGeneratorException(e);
		}
		return new RawIDSetData(stat.getVersion(), bytes);
	}

	public void setData(IDCategory category, RawIDSetData idContents) throws IDGeneratorException {
		int expectedVersion = idContents.getVersion();
		logger.trace("Setting data for category {} and version {}", category, expectedVersion);
		String categoryName = category.getName();
		try {
			zooKeeper.setData(categoryName, idContents.getData(), expectedVersion);
		}
		catch(KeeperException.BadVersionException e) {
			int actualVersion = getDataVersion(category);
			throw new IDProviderVersionException("Set data failed", category, expectedVersion, actualVersion, e);
		}
		catch(KeeperException e) {
			if (e.code().equals(KeeperException.Code.NONODE)) {
				throw new IDGeneratorException("No id data found for category: "+categoryName);
			}
			throw new IDGeneratorException(e);
		}
		catch(Exception e) {
			throw new IDGeneratorException(e);
		}
	}

	public void close() throws IDGeneratorException {
		try {
			logger.info("Closing connection(s) to Zookeeper(s) at {}", hostList);
			zooKeeper.close();
			logger.info("Zookeeper connection(s) closed");
		}
		catch (InterruptedException e) {
			throw new IDGeneratorException(e);
		}
	}


	ZooKeeper getZookeeper() {
		return zooKeeper;
	}

	private int getDataVersion(IDCategory category) {
		int version;
		try {
			RawIDSetData idContents = getData(category);
			version = idContents.getVersion();
		}
		catch (Exception e) {
			version = IDProviderVersionException.UNKNOWN;
		}
		return version;
	}


	private static class NoOpWatcher implements Watcher {
		public void process(WatchedEvent event) {
		}		
	}

	private String hostList;
	private int sessionTimeout;
	private Watcher watcher;
	private Logger logger;
	private ZooKeeper zooKeeper;
}
