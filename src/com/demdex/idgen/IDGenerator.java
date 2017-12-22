package com.demdex.idgen;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.demdex.util.io.CharacterStreamUtils;

/**
 * The IDGenerator utility.
 * 
 * The IDGenerator provides the ability for users to take a set of ID's from it (and push back unused ID's).
 * 
 * The IDGenerator requires an IDProvider.  The provider supplies the I/O mechanism that will be used when reading/writing
 * ID data.
 * 
 * Note that the IDGenerator is threadsafe.
 * 
 * @author D.Rosenstrauch, Demdex Inc.
 * $Revision: 12 $
 * $Date: 2017-02-07 23:23:40 +0000 (Tue, 07 Feb 2017) $
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

public class IDGenerator {

	public IDGenerator(IDProvider idProvider) {
		this.idProvider = idProvider;
		logger = LoggerFactory.getLogger(IDGenerator.class);
		logger.info("Initializing ID generator, using ID provider: {}", idProvider.getName());
	}

	public void open() throws IDGeneratorException {
		logger.info("Opening ID generator ...");
		logger.info("Opening ID provider ...");
		idProvider.open();
		logger.info("ID provider and generator opened");
	}

	public IDSet takeIDs(IDCategory category, long idSetSize) throws IDGeneratorException {
		return takeIDsWithRetry(category, idSetSize, 1);
	}

	public IDSet takeIDsWithRetry(IDCategory category, long idSetSize) throws IDGeneratorException {
		return takeIDsWithRetry(category, idSetSize, getDefaultTryCount());
	}

	public IDSet takeIDsWithRetry(IDCategory category, long idSetSize, int maxTryCount) throws IDGeneratorException {
		logger.debug("Taking {} ID's from category {}", idSetSize, category.getName());
		// TODO - remove duplication
		int currTryCount = 0;
		IDSet takenIDs = null;
		boolean succeeded = false;
		while (!succeeded ) {
			currTryCount++;
			if (maxTryCount > 1) {
				logger.debug("Attempt # {} of {}", currTryCount, maxTryCount);
			}
			RawIDSetData currData = idProvider.getData(category);
			int currVersion = currData.getVersion();
			IDSet currIDs = deserialize(category, currData.getData());
			takenIDs = currIDs.takeIDs(idSetSize);
			byte[] newData = serialize(currIDs);
			try {
				idProvider.setData(category, new RawIDSetData(currVersion, newData));
				succeeded = true;
			}
			catch(IDProviderVersionException e) {
				if (logger.isDebugEnabled()) {
					logger.debug("Take ID's failed due to versioning error {}", getVersionDetailText(e));
				}
				if (currTryCount >= maxTryCount) {
					if (maxTryCount > 1) {
						logger.error("Take ID's request has failed after " + maxTryCount + " tries", e);
					}
					throw e;
				}
				logger.debug("Retrying ...");
			}
		}
		if (logger.isDebugEnabled()) logger.debug("Successfully took ID's: {}", takenIDs.toString());
		return takenIDs;
	}

	public void pushIDs(IDSet idSet) throws IDGeneratorException {
		pushIDs(idSet.getCategory(), idSet);
	}

	public void pushIDs(IDCategory category, IDSet idSet) throws IDGeneratorException {
		pushIDsWithRetry(category, idSet, 1);
	}

	public void pushIDsWithRetry(IDSet idSet) throws IDGeneratorException {
		pushIDsWithRetry(idSet.getCategory(), idSet);
	}

	public void pushIDsWithRetry(IDCategory category, IDSet idSet) throws IDGeneratorException {
		pushIDsWithRetry(category, idSet, getDefaultTryCount());
	}

	public void pushIDsWithRetry(IDCategory category, IDSet idSet, int maxTryCount) throws IDGeneratorException {
		IDSet.validateNotReadOnly(idSet);

		if (logger.isDebugEnabled()) logger.debug("Pushing ID's: {} to category: {}", idSet.toString(), category.toString());
		// TODO - remove duplication
		int currTryCount = 0;
		boolean succeeded = false;
		while (!succeeded ) {
			currTryCount++;
			if (maxTryCount > 1) {
				logger.debug("Attempt # {} of {}", currTryCount, maxTryCount);
			}
			RawIDSetData currData = idProvider.getData(category);
			int currVersion = currData.getVersion();
			IDSet currIDs = deserialize(category, currData.getData());
			currIDs.pushIDs(idSet);
			byte[] newData = serialize(currIDs);
			try {
				idProvider.setData(category, new RawIDSetData(currVersion, newData));
				succeeded = true;
			}
			catch(IDProviderVersionException e) {
				if (logger.isDebugEnabled()) {
					logger.debug("Push ID's failed due to versioning error {}", getVersionDetailText(e));
				}
				if (currTryCount >= maxTryCount) {
					if (maxTryCount > 1) {
						logger.error("Push ID's request has failed after " + maxTryCount + " tries", e);
					}
					throw e;
				}
				logger.debug("Retrying ...");
			}
		}
		logger.debug("Successfully pushed ID's");
	}

	public void setDefaultTryCount(int defaultTryCount) {
		logger.info("Setting default try count to {}", defaultTryCount);
		if (defaultTryCount <= 0) {
			throw new IllegalArgumentException("Invalid default try count; try count must be set to at least 1 try");
		}
		this.defaultTryCount = defaultTryCount;
	}

	public int getDefaultTryCount() {
		return defaultTryCount;
	}

	public void close() throws IDGeneratorException {
		logger.info("Closing ID generator ...");
		try {
			logger.info("Closing ID provider ...");
			idProvider.close();
			logger.info("ID provider and generator closed");
		}
		finally {
			idProvider = new ClosedIDProvider();
		}
    }

    public boolean isOpen() {
        return !(idProvider instanceof ClosedIDProvider);
	}

	public IDSet peekIDs(IDCategory category) throws IDGeneratorException {
		// TODO - remove duplication
		RawIDSetData currContents = idProvider.getData(category);
		IDSet currIDs = deserialize(category, currContents.getData());
		//currIDs.setReadOnly();
		if (logger.isDebugEnabled()) logger.debug("Peeked at ID's in category {}: {}", category.toString(), currIDs.toString());
		return currIDs;
	}


	private IDSet deserialize(IDCategory category, byte[] currData) throws IDGeneratorException {
		BufferedReader in = CharacterStreamUtils.toBufferedReader(
			new InputStreamReader(new ByteArrayInputStream(currData), IDGeneratorConstants.UTF8_CHARSET),
			IDGeneratorConstants.DEFAULT_BUFFER_SIZE
		);
		List<IDRange> idRanges = new LinkedList<IDRange>();
		try {
			String line;
			while((line = in.readLine()) != null) {
				IDRange range = IDRange.parse(line);
				idRanges.add(range);
			}
		}
		catch (Exception e) {
			throw new IDGeneratorException(
				"Error deserializing data for category "+category.toString()+": "+new String(currData),
				e
			);
		}
		finally {
			CharacterStreamUtils.close(in);
		}
		return new IDSet(category, idRanges);
	}

	private byte[] serialize(IDSet currIDs) throws IDGeneratorException {
		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		PrintWriter out = CharacterStreamUtils.toBufferedPrintWriter(
			new OutputStreamWriter(bytesOut),
			IDGeneratorConstants.DEFAULT_BUFFER_SIZE
		);
		try {
			try {
				currIDs.write(out);
			}
			finally {
				CharacterStreamUtils.flushAndClose(out);
			}
		}
		catch (Exception e) {
			throw new IDGeneratorException(
				"Error serializing data for id set "+currIDs.toString(),
				e
			);
		}
		return bytesOut.toByteArray();
	}

	private String getVersionDetailText(IDProviderVersionException e) {
		String versionDetails =
			e.getExpectedVersion() != IDProviderVersionException.UNKNOWN
				? "; expected version: " + e.getExpectedVersion() + ", actual version: " + e.getActualVersion()
				: e.getCause() != null
					? e.getCause().getMessage()
					: e.getMessage();
		return versionDetails;
	}

	private static class ClosedIDProvider implements IDProvider {

		public void open() {
			throw new UnsupportedOperationException();
		}		

		public String getName() {
			return "no ID provider";
		}

		public RawIDSetData getData(IDCategory category) throws IDGeneratorException {
			throw new IDGeneratorException(CLOSED_MSG);
		}

		public void setData(IDCategory category, RawIDSetData idContents) throws IDGeneratorException {
			throw new IDGeneratorException(CLOSED_MSG);
		}

		public void close() {
		}		

		private static final String CLOSED_MSG = "ID Generator has been closed";
	}

	private IDProvider idProvider;
	private Logger logger;
	private int defaultTryCount = IDGeneratorConstants.DEFAULT_MAX_TRY_COUNT;
}
