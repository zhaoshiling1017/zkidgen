package com.demdex.idgen;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentSkipListSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A (possibly non-contiguous) set of ID numbers.  IDSets consist of a collection of contiguous IDRanges.
 * ID's can be taken from the set, as well as unused ID's pushed back into it.  
 * 
 * Note that given a call to <code>idSet.takeIDs(idSetSize)</code>, if <code>idSetSize > idSet.getSize()</code>,
 * the IDSet will *not* throw an exception, but rather will return an IDSet of size <code>idSet.getSize()</code>.
 * 
 * Note that an IDSet is threadsafe.
 * 
 * Note:  For data integrity reasons an IDSet must never publicly expose - or allow any calling client to access -
 * its internal collection of IDRanges, or any of the individual IDRanges held within it.  We do not want clients
 * of the IDGenerator/IDSet classes to be able to directly add or remove ranges from an IDSet, else it could compromise
 * the data integrity of the set.  Similarly, we don't want clients to be able to have access to specific IDRanges held
 * within an IDSet, as they would then be able to alter the contents of the middle of a set (i.e., by taking id's from
 * a range in the middle of the set).  As a result, the set's collection of IDRanges must be kept as a private, and neither
 * the collection nor any of the ranges held in it can be exposed to calling classes.   
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

public class IDSet {

    public IDSet(IDCategory category, String initIdRangesStr) {
        this(category, IDRange.parseMultiple(initIdRangesStr));
    }

	public IDSet(IDCategory category, List<IDRange> initIDRanges) {
		this(category, initIDRanges, EXTERNAL);
	}

	public IDSet(IDCategory category, IDRange idRange) {
		this(category, toList(idRange));
	}

	public IDSet(IDCategory category, long startID, long endID) {
		this(category, new IDRange(startID, endID));
	}

	public IDSet(IDCategory category) {
		// empty IDSet
		this(category, toList(null));
	}

	public IDSet(IDCategory category, IDRange... initIDRanges) {
		this(category, Arrays.asList(initIDRanges));
	}

	private IDSet(IDCategory category, List<IDRange> initIDRanges, boolean idRangeSource) {
		this.category = category;
		for (IDRange initIDRange : initIDRanges) {
			validateNoOverlap(initIDRange);
			if (idRangeSource == EXTERNAL) {
				initIDRange = initIDRange.copy();
			}
			addIDRange(initIDRange);
		}
		logger = LoggerFactory.getLogger(IDSet.class);
	}

    public IDCategory getCategory() {
		return category;
	}

	public long getSize() {
		long size = 0;
		for (IDRange range : idRanges) {
			size += range.getSize();
		}
		return size;
	}

	public IDSet takeIDs(long idSetSize) {
		validateNotReadOnly(this);
		List<IDRange> takenRanges;
		synchronized(this) {
			if (logger.isTraceEnabled()) logger.trace("Taking {} ID's from ID set: {}", Long.toString(idSetSize), toString());
			long size = getSize();
			if (size == 0) {
				throw new NoSuchElementException("Can't take id's; IDSet is empty");
			}
			if (idSetSize > size) {
				logger.trace(
					"Requested 'take size' of {} exceeds current size of IDSet; using set's current size of {} instead",
					idSetSize,
					size
				);
				idSetSize = size;
			}
			long numTaken = 0;
			takenRanges = new LinkedList<IDRange>();
			while (numTaken < idSetSize) {
				long numToBeTaken = idSetSize - numTaken;
				IDRange firstRange = idRanges.first();
				long numToBeTakenFromRange = Math.min(numToBeTaken, firstRange.getSize());
				IDRange takenIDRange = firstRange.takeIDs(numToBeTakenFromRange);
				takenRanges.add(takenIDRange);
				numTaken += numToBeTakenFromRange;
				if (!firstRange.hasMoreIDs()) {
					takeFirstRange();
				}
			}
		}
		IDSet takenIDSet = new IDSet(category, takenRanges, INTERNAL);
		if (logger.isTraceEnabled()) logger.trace("Took ID's from set; ID's taken: {}, ID's remaining: {}", takenIDSet.toString(), toString());
		return takenIDSet;
	}

	public long takeID() {
		validateNotReadOnly(this);
		long takenID;
		synchronized(this) {
			if (logger.isTraceEnabled()) logger.trace("Taking single ID from ID set {}", toString());
			if (idRanges.size() == 0) {
				throw new NoSuchElementException("No more id's remaining in set");
			}
			IDRange firstRange = idRanges.first();
			takenID = firstRange.takeID();
			if (!firstRange.hasMoreIDs()) {
				takeFirstRange();
			}
		}
		if (logger.isTraceEnabled()) logger.trace("Took ID from set; ID taken: {}, ID's remaining: {}", Long.toString(takenID), toString());
		return takenID;
	}

	public long peekNextID() {
        if (idRanges.size() == 0) {
            throw new NoSuchElementException("No more id's remaining in set");
        }
		IDRange firstRange = idRanges.first();
		return firstRange.peekNextID();
	}

	public void pushIDs(IDSet pushedIDSet) {
		if (!pushedIDSet.category.equals(category)) {
			throw new IllegalArgumentException(
				"Can't push id set: "+pushedIDSet.toString()+" to id set: "+toString()+"; categories do not match"
			);
		}
		validateNotReadOnly(pushedIDSet);
		validateNotReadOnly(this);
		synchronized(pushedIDSet) {
			if (logger.isTraceEnabled()) logger.trace("Pushing ID's: {} to ID set: {}", pushedIDSet.toString(), toString());
			for (IDRange pushedIDRange : pushedIDSet.idRanges) {
				validateNoOverlap(pushedIDRange);
			}

			synchronized(this) {
    			while(pushedIDSet.hasMoreIDs()) {
    				IDRange pushedIDRange = pushedIDSet.takeFirstRange();
    	            addIDRange(pushedIDRange);
    			}
			}
			if (logger.isTraceEnabled()) logger.trace("Pushed ID's to set; ID's now remaining: {}", toString());
		}
	}

	public String toString() {
		StringBuilder buf = new StringBuilder();
		toString(buf);
		return buf.toString();
	}

	public void toString(StringBuilder buf) {
		buf.append(category.toString());
		buf.append(':');
		rangesToString(buf);
	}

	public void rangesToString(StringBuilder buf) {
		boolean firstRange = true;
		for (IDRange range : idRanges) {
			if (!firstRange) {
				buf.append(IDGeneratorConstants.RANGE_SEPARATOR);
			}
			range.toString(buf);
			firstRange = false;
		}
	}
	
	public void write(PrintWriter out) {
		for (IDRange range : idRanges) {
			range.write(out);
			out.println();
		}
	}

	public boolean hasMoreIDs() {
		if (idRanges.size() == 0) {
			return false;
		}
		IDRange firstRange = idRanges.first();
		return firstRange.hasMoreIDs();
	}

	public boolean isReadOnly() {
		return readOnly;
	}

	public Iterator<IDRange> peekRanges() {
		List<IDRange> peekingRanges = new ArrayList<IDRange>();
		for (IDRange range : idRanges) {
			peekingRanges.add(range.copy(true/*=readOnly*/));
		}
		return peekingRanges.iterator();
	}


	static void validateNotReadOnly(IDSet idSet) {
		if (idSet.isReadOnly()) {
			throw new IllegalStateException("IDSet is read-only: "+idSet.toString());
		}
	}

	void setReadOnly() {
		for (IDRange range : idRanges) {
			range.setReadOnly();
		}
        readOnly = true;
	}


	private IDRange takeFirstRange() {
		return idRanges.pollFirst();
	}

    private void addIDRange(IDRange newRange) {
        synchronized(idRanges) {
            boolean rangeMerged;
            do {
                rangeMerged = false;
                IDRange previousRange = idRanges.lower(newRange);
                if (previousRange != null) {
                    IDRange combinedNewRange = previousRange.tryMerge(newRange);
                    if (combinedNewRange != null) {
                        rangeMerged = true;
                        newRange = combinedNewRange;
                        idRanges.remove(previousRange);
                    }
                }
    
                IDRange nextRange = idRanges.higher(newRange);
                if (nextRange != null) {
                    IDRange combinedNewRange = newRange.tryMerge(nextRange);
                    if (combinedNewRange != null) {
                        rangeMerged = true;
                        newRange = combinedNewRange;
                        idRanges.remove(nextRange);
                    }
                }
            } while(rangeMerged);
    
            idRanges.add(newRange);
        }
    }

    private void validateNoOverlap(IDRange pushedRange) {
		if (idRanges.size() == 0) {
			return;
		}
		IDRange previousRange = idRanges.lower(pushedRange);
		if (previousRange != null) {
			if (previousRange.getEndID() > pushedRange.getStartID()) {
				throw new OverlappingRangeException(pushedRange, previousRange, this);
			}
		}
		else {
			IDRange firstRange = idRanges.first();
			if (firstRange == null) {
				return;
			}
			if (pushedRange.getEndID() >= firstRange.getStartID()) {
				throw new OverlappingRangeException(pushedRange, firstRange, this);
			}
		}
	}

	private static class OverlappingRangeException extends IllegalArgumentException {
        public OverlappingRangeException(IDRange pushedRange, IDRange existingRange, IDSet idSet) {
            super(
    			"Pushed range: "+pushedRange.toString()+
    			" overlaps with range: "+existingRange.toString()+
    			" in id set: "+idSet.toString()
    		);
        }
	}

	private static List<IDRange> toList(IDRange idRange) {
		List<IDRange> ranges = new ArrayList<IDRange>();
		if (idRange != null) {
			ranges.add(idRange);
		}
		return ranges;
	}

	private static final boolean INTERNAL = true;
	private static final boolean EXTERNAL = !INTERNAL;
	private IDCategory category;
	private Logger logger;
	private NavigableSet<IDRange> idRanges = new ConcurrentSkipListSet<IDRange>();
	private boolean readOnly = false;
}
