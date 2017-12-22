package com.demdex.idgen;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import junit.framework.TestCase;
import junitx.framework.ObjectAssert;

/**
 * Junit tests for the functionality in the IDGenerator class. 
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

public class TestIDGenerator extends TestCase {

	protected void setUp() throws IDGeneratorException {
		generator = new IDGenerator(new MemoryIDProvider(TestingConstants.TEST_IDGEN_CATEGORY, MIN_ID, MAX_ID));
		generator.open();
	}

	public void testTakeIDs() throws IDGeneratorException {
		IDSet idSet = generator.takeIDs(TestingConstants.TEST_IDGEN_CATEGORY, NUM_IDS);
		assertEquals(TestingConstants.TEST_IDGEN_CATEGORY, idSet.getCategory());
		assertEquals(NUM_IDS, idSet.getSize());
		Iterator<IDRange> idRanges = idSet.peekRanges();
		IDRange idRange = idRanges.next();
		assertFalse(idRanges.hasNext());
		assertEquals(NUM_IDS, idRange.getSize());
		assertEquals(1, idRange.getStartID());
		assertEquals(NUM_IDS, idRange.getEndID());
	}

	public void testTakeWithInsufficientIDs() throws IDGeneratorException {
		IDSet idSet = generator.takeIDs(TestingConstants.TEST_IDGEN_CATEGORY, MAX_ID + 1);
		assertEquals(MAX_ID, idSet.getSize());
	}

	public void testTakeWithNoIDs() throws IDGeneratorException {
		generator.takeIDs(TestingConstants.TEST_IDGEN_CATEGORY, MAX_ID);
		try {
			generator.takeIDs(TestingConstants.TEST_IDGEN_CATEGORY, NUM_IDS);
			fail();
		}
		catch(NoSuchElementException e) {
		}
	}

	public void testPush() throws IDGeneratorException {
		final int halfNumIDs = NUM_IDS >> 1;
		IDSet takenIDs = generator.takeIDs(TestingConstants.TEST_IDGEN_CATEGORY, NUM_IDS);
		takenIDs.takeIDs(halfNumIDs);
		generator.pushIDs(takenIDs);
		IDSet allIDs = generator.peekIDs(TestingConstants.TEST_IDGEN_CATEGORY);
		assertEquals(MAX_ID - halfNumIDs, allIDs.getSize());
		assertEquals(halfNumIDs + 1, allIDs.peekNextID());
	}

	public void testTakeIDsWithMultipleRanges() throws IDGeneratorException {
		generator = new IDGenerator(new MemoryIDProvider(TestingConstants.TEST_IDGEN_CATEGORY));
		IDRange idRange1 = new IDRange(101, 200);
		IDRange idRange2 = new IDRange(301, 400);
		IDRange idRange3 = new IDRange(501, 600);
		IDRange idRange4 = new IDRange(1001, 2000);
		IDSet initIDs = new IDSet(TestingConstants.TEST_IDGEN_CATEGORY, idRange1, idRange2, idRange3, idRange4);
		generator.pushIDs(initIDs);

		IDSet taken = generator.takeIDs(TestingConstants.TEST_IDGEN_CATEGORY, 400);
		assertEquals(400, taken.getSize());
		IDSet allIDs = generator.peekIDs(TestingConstants.TEST_IDGEN_CATEGORY);
		assertEquals(900, allIDs.getSize());
	}

	public void testPushedRangesSorted() throws IDGeneratorException {
		generator.takeIDs(TestingConstants.TEST_IDGEN_CATEGORY, NUM_IDS);
		generator.pushIDs(new IDSet(TestingConstants.TEST_IDGEN_CATEGORY, MAX_ID + 1000, MAX_ID + 2000));
		generator.pushIDs(new IDSet(TestingConstants.TEST_IDGEN_CATEGORY, MIN_ID, 100));
		IDSet allIDs = generator.peekIDs(TestingConstants.TEST_IDGEN_CATEGORY);
		Iterator<IDRange> it = allIDs.peekRanges();
		IDRange range = it.next();
		assertEquals(MIN_ID, range.getStartID());
		assertEquals(100, range.getEndID());

		range = it.next();
		assertEquals(NUM_IDS + 1, range.getStartID());
		assertEquals(MAX_ID, range.getEndID());

		range = it.next();
		assertEquals(MAX_ID + 1000, range.getStartID());
		assertEquals(MAX_ID + 2000, range.getEndID());
	}

	public void testVersionErrorOnConcurrentTake() throws IDGeneratorException, InterruptedException {
		Future<IDSet> takenFuture = doTestConcurrentTake(NO_RETRY);
		try {
			takenFuture.get();
			fail();
		}
		catch (ExecutionException e) {
			Throwable cause = e.getCause();
			ObjectAssert.assertInstanceOf(IDProviderVersionException.class, cause);
		}
	}

	public void testVersionErrorOnConcurrentPush() throws IDGeneratorException, InterruptedException {
		Future<Object> pushFuture = doTestConcurrentPush(NO_RETRY);
		try {
			pushFuture.get();
			fail();
		}
		catch (ExecutionException e) {
			Throwable cause = e.getCause();
			ObjectAssert.assertInstanceOf(IDProviderVersionException.class, cause);
		}
	}

	public void testConcurrentTakeWithRetry() throws IDGeneratorException, InterruptedException, ExecutionException {
		Future<IDSet> takenFuture = doTestConcurrentTake(RETRY);
		IDSet taken = takenFuture.get();
		assertNotNull(taken);
	}

	public void testConcurrentPushWithRetry() throws IDGeneratorException, InterruptedException, ExecutionException {
		@SuppressWarnings("rawtypes")
		Future pushFuture = doTestConcurrentPush(RETRY);
		pushFuture.get();
	}


	private Future<IDSet> doTestConcurrentTake(boolean retry) throws IDGeneratorException {
		BlockingMemoryIDProvider blockingProvider =
			new BlockingMemoryIDProvider(TestingConstants.TEST_IDGEN_CATEGORY, MIN_ID, MAX_ID);
		generator = new IDGenerator(blockingProvider);
		Future<IDSet> takenFuture = executeThreadedTask(
			new IDTakerTask(generator, TestingConstants.TEST_IDGEN_CATEGORY, NUM_IDS, retry)
		);

		// wait until the blocking starts
		while(!blockingProvider.isBlocking()) {}

		blockingProvider.setBlockingEnabled(false);
		generator.takeIDs(TestingConstants.TEST_IDGEN_CATEGORY, NUM_IDS);
		
		blockingProvider.stopBlocking();
		return takenFuture;
	}

	private Future<Object> doTestConcurrentPush(boolean retry) throws IDGeneratorException {
		BlockingMemoryIDProvider blockingProvider =
			new BlockingMemoryIDProvider(TestingConstants.TEST_IDGEN_CATEGORY, MIN_ID, MAX_ID);
		generator = new IDGenerator(blockingProvider);
		blockingProvider.setBlockingEnabled(false);
		IDSet taken = generator.takeIDs(TestingConstants.TEST_IDGEN_CATEGORY, NUM_IDS);
		taken.takeIDs(100);
		blockingProvider.setBlockingEnabled(true);
		Future<Object> pushFuture = executeThreadedTask(new IDPusherTask(generator, taken, retry));

		// wait until the blocking starts
		while(!blockingProvider.isBlocking()) {}

		blockingProvider.setBlockingEnabled(false);
		generator.takeIDs(TestingConstants.TEST_IDGEN_CATEGORY, NUM_IDS);
		
		blockingProvider.stopBlocking();
		return pushFuture;
	}

	private static <T> Future<T> executeThreadedTask(Callable<T> task) {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		return executor.submit(task);
	}

	private static class IDTakerTask implements Callable<IDSet> {

		public IDTakerTask(IDGenerator generator, IDCategory category, int idSetSize, boolean retry) {
			this.generator = generator;
			this.category = category;
			this.idSetSize = idSetSize;
			this.retry = retry; 
		}

		public IDSet call() throws IDGeneratorException {
			return
				(retry == RETRY)
					? generator.takeIDsWithRetry(category, idSetSize)
					: generator.takeIDs(category, idSetSize);
		}

		private IDGenerator generator;
		private IDCategory category;
		private int idSetSize;
		private boolean retry;
	}

	private static class IDPusherTask implements Callable<Object> {

		public IDPusherTask(IDGenerator generator, IDSet idSet, boolean retry) {
			this.generator = generator;
			this.idSet = idSet;
			this.retry = retry;
		}

		public Object call() throws Exception {
			if (retry == RETRY) {
				generator.pushIDsWithRetry(idSet);
			}
			else {
				generator.pushIDs(idSet);
			}
			return null;
		}

		private IDGenerator generator;
		private IDSet idSet;
		private boolean retry;
	}

	private static final int MIN_ID = 1;
	private static final int MAX_ID = 10000;
	private static final int NUM_IDS = 1000;
	private static final boolean RETRY = true;
	private static final boolean NO_RETRY = !RETRY;
	private IDGenerator generator;
}
