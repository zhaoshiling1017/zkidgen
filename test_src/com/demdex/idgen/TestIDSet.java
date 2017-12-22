package com.demdex.idgen;

import java.util.Iterator;
import java.util.NoSuchElementException;

import junit.framework.TestCase;

/**
 * Junit tests for the functionality in the IDSet class. 
 * 
 * @author D.Rosenstrauch, Demdex Inc.
 * $Revision: 13 $
 * $Date: 2017-05-24 17:14:25 +0000 (Wed, 24 May 2017) $
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

public class TestIDSet extends TestCase {

	protected void setUp() {
		idSet = new IDSet(TestingConstants.TEST_IDGEN_CATEGORY, LOW_ID, HIGH_ID);
	}

	public void testTakeID() throws IDGeneratorException {
		int i = 1;
		while(idSet.hasMoreIDs()) {
			assertEquals(i, idSet.takeID());
			i++;
		}
		try {
			idSet.takeID();
			fail();
		}
		catch(NoSuchElementException e) {
		}
	}

	public void testTakeIDs() throws IDGeneratorException {
		IDSet taken = idSet.takeIDs(NUM_IDS);
		assertEquals(NUM_IDS, taken.getSize());
		assertEquals(1, taken.takeID());
		assertEquals(HIGH_ID - NUM_IDS, idSet.getSize());
		assertEquals(NUM_IDS + 1, idSet.takeID());
	}

	public void testTakeWithInsufficientIDs() throws IDGeneratorException {
		IDSet taken = idSet.takeIDs(HIGH_ID + 1);
		assertEquals(HIGH_ID, taken.getSize());
	}

	public void testTakeWithNoIDs() throws IDGeneratorException {
		idSet.takeIDs(HIGH_ID);
		try {
			idSet.takeIDs(NUM_IDS);
			fail();
		}
		catch(NoSuchElementException e) {
		}
	}

	public IDSet testTakeIDsWithMultipleRanges() throws IDGeneratorException {
		IDRange idRange1 = new IDRange(101, 200);
		IDRange idRange2 = new IDRange(301, 400);
		IDRange idRange3 = new IDRange(501, 600);
		IDRange idRange4 = new IDRange(1001, 2000);
		idSet = new IDSet(TestingConstants.TEST_IDGEN_CATEGORY, idRange1, idRange2, idRange3, idRange4);
		IDSet taken = idSet.takeIDs(400);
		assertEquals(400, taken.getSize());
		return taken;
	}

	public void testTakeOneFromTakenIDsWithMultipleRanges() throws IDGeneratorException {
		IDSet taken = testTakeIDsWithMultipleRanges();
		long expectedNumIDs = taken.getSize();
		long actualNumIDs = 0;
		while(taken.hasMoreIDs()) {
			taken.takeID();
			actualNumIDs++;
		}
		assertEquals(expectedNumIDs, actualNumIDs);
	}

	public void testTakeMultipleTakenIDsWithMultipleRanges() throws IDGeneratorException {
		IDSet taken = testTakeIDsWithMultipleRanges();
		int idSetSize = 8;
		int expectedNumIDTakes = 50;
		int actualNumIDTakes = 0;
		while(taken.hasMoreIDs()) {
			taken.takeIDs(idSetSize);
			actualNumIDTakes++;
		}
		assertEquals(expectedNumIDTakes, actualNumIDTakes);
	}

	public void testPush() throws IDGeneratorException {
		idSet.takeIDs(NUM_IDS);
		assertEquals(HIGH_ID - NUM_IDS, idSet.getSize());

		idSet.pushIDs(new IDSet(TestingConstants.TEST_IDGEN_CATEGORY, 1, NUM_IDS));
		assertEquals(HIGH_ID, idSet.getSize());

		idSet.pushIDs(new IDSet(TestingConstants.TEST_IDGEN_CATEGORY, HIGH_ID + 1, HIGH_ID + NUM_IDS));
		assertEquals(HIGH_ID + NUM_IDS, idSet.getSize());
	}

	public void testPushWithOverlappingRanges() throws IDGeneratorException {
		idSet.takeIDs(NUM_IDS);
		assertEquals(HIGH_ID - NUM_IDS, idSet.getSize());

		try {
			idSet.pushIDs(new IDSet(TestingConstants.TEST_IDGEN_CATEGORY, LOW_ID, NUM_IDS + 1));
			fail();
		}
		catch(IllegalArgumentException e) {
		}

		try {
			idSet.pushIDs(new IDSet(TestingConstants.TEST_IDGEN_CATEGORY, HIGH_ID - 1, HIGH_ID + NUM_IDS));
			fail();
		}
		catch(IllegalArgumentException e) {
		}
	}

	public void testPushedRangesSorted() throws IDGeneratorException {
		idSet.takeIDs(NUM_IDS);
		idSet.pushIDs(new IDSet(TestingConstants.TEST_IDGEN_CATEGORY, HIGH_ID + 1000, HIGH_ID + 2000));
		idSet.pushIDs(new IDSet(TestingConstants.TEST_IDGEN_CATEGORY, LOW_ID, 100));
		Iterator<IDRange> it = idSet.peekRanges();
		IDRange range = it.next();
		assertEquals(LOW_ID, range.getStartID());
		assertEquals(100, range.getEndID());

		range = it.next();
		assertEquals(NUM_IDS + 1, range.getStartID());
		assertEquals(HIGH_ID, range.getEndID());

		range = it.next();
		assertEquals(HIGH_ID + 1000, range.getStartID());
		assertEquals(HIGH_ID + 2000, range.getEndID());
    }

    public void testMergeConsecutiveRanges() {
        IDSet idSet2 = new IDSet(TestingConstants.TEST_IDGEN_CATEGORY, HIGH_ID + 1, HIGH_ID + NUM_IDS);
        idSet.pushIDs(idSet2);
        Iterator<IDRange> idRanges = idSet.peekRanges();
        IDRange range = idRanges.next();
        assertEquals(LOW_ID, range.getStartID());
        assertEquals(HIGH_ID + NUM_IDS, range.getEndID());
        assertEquals(false, idRanges.hasNext());
    }

    public void testMergeMultipleConsecutiveRangesOnCreate() {
        IDRange idRange1 = new IDRange(101, 200);
        IDRange idRange2 = new IDRange(201, 400);
        IDRange idRange3 = new IDRange(401, 600);
        IDRange idRange4 = new IDRange(1001, 2000);
        IDRange idRange5 = new IDRange(2001, 3000);
        idSet = new IDSet(TestingConstants.TEST_IDGEN_CATEGORY, idRange1, idRange2, idRange3, idRange4, idRange5);
        Iterator<IDRange> idRanges = idSet.peekRanges();
        IDRange range = idRanges.next();
        assertEquals(101, range.getStartID());
        assertEquals(600, range.getEndID());
        range = idRanges.next();
        assertEquals(1001, range.getStartID());
        assertEquals(3000, range.getEndID());
        assertEquals(false, idRanges.hasNext());
	}

    public void testMergeMultipleConsecutiveRangesOnCreate2() {
      idSet = new IDSet(TestingConstants.TEST_IDGEN_CATEGORY, "380569-380569,380570-380570,380571-380571,380572-380572,380573-380573,380574-380574,380575-380575,380576-380576,380577-380577,380578-380578,380579-380579,380580-380580");
      Iterator<IDRange> idRanges = idSet.peekRanges();
      IDRange range = idRanges.next();
      assertEquals(380569, range.getStartID());
      assertEquals(380580, range.getEndID());
      assertEquals(false, idRanges.hasNext());
    }

    public void testMergeMultipleConsecutiveRangesOnPush() {
        IDRange idRange1 = new IDRange(101, 200);
        IDRange idRange3 = new IDRange(401, 600);
        IDRange idRange5 = new IDRange(1001, 2000);
        idSet = new IDSet(TestingConstants.TEST_IDGEN_CATEGORY, idRange1, idRange3, idRange5);

        IDRange idRange2 = new IDRange(201, 400);
        IDRange idRange4 = new IDRange(601, 1000);
        IDRange idRange6 = new IDRange(2001, 3000);
        IDSet idSet2 = new IDSet(TestingConstants.TEST_IDGEN_CATEGORY, idRange2, idRange4, idRange6);
        idSet.pushIDs(idSet2);
        Iterator<IDRange> idRanges = idSet.peekRanges();
        IDRange range = idRanges.next();
        assertEquals(101, range.getStartID());
        assertEquals(3000, range.getEndID());
        assertEquals(false, idRanges.hasNext());
    }

	private static final int LOW_ID = 1;
	private static final int HIGH_ID = 10000;
	private static final int NUM_IDS = 1000;
	private IDSet idSet;
}
