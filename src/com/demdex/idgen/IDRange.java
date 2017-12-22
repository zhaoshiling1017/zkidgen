package com.demdex.idgen;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A continguous range of ID numbers.  ID's can be "taken" from the range, as they are needed.
 * 
 * Note that given a call to <code>idRange.takeIDs(idRangeSize)</code>, if <code>idRangeSize > idRange.getSize()</code>,
 * the IDRange will *not* throw an exception, but rather will return an IDRange of size <code>idRange.getSize()</code>.
 * 
 * Note that an IDRange is threadsafe.
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

public class IDRange implements Comparable<IDRange>, Cloneable {

	public IDRange(long startID, long endID) {
		this(startID, endID, false);
	}

	public IDRange(long startID, long endID, boolean readOnly) {
		if (endID < startID) {
			throw new IllegalArgumentException("Invalid start/end values for range: "+startID+", "+endID);
		}
		this.currStartID = new AtomicLong(startID);
		this.endID = endID;
		this.readOnly = readOnly;
	}

	public long getSize() {
		return endID - currStartID.get() + 1;
	}

	public long getStartID() {
		return currStartID.get();
	}

	public long getEndID() {
		return endID;
	}

	public IDRange takeIDs(long idRangeSize) {
		validateNotReadOnly();
		long takenStartID;
		synchronized(this) {
			long size = getSize();
			if (size == 0) {
				throw new IllegalArgumentException("Can't take id's; IDRange is empty");
			}
			if (idRangeSize > size) {
				// if requesting more than maximum available amount of ID's, only return maximum
				idRangeSize = size;
			}
			takenStartID = currStartID.getAndAdd(idRangeSize);
		}
		return new IDRange(takenStartID, takenStartID + idRangeSize - 1);
	}

	public synchronized long takeID() {
		validateNotReadOnly();
		return currStartID.getAndIncrement();
	}

	public long peekNextID() {
		return currStartID.get();
	}

	public boolean hasMoreIDs() {
		return currStartID.get() <= endID;
	}

	public String toString() {
		StringBuilder buf = new StringBuilder();
		toString(buf);
		return buf.toString();
	}

	public void toString(StringBuilder buf) {
		buf.append(currStartID.toString());
		buf.append(IDGeneratorConstants.RANGE_VALUES_SEPARATOR);
		buf.append(Long.toString(endID));
	}

	public void write(PrintWriter out) {
		out.print(currStartID.toString());
		out.print(IDGeneratorConstants.RANGE_VALUES_SEPARATOR);
		out.print(endID);
	}

	public boolean equals(Object o) {
		if (o instanceof IDRange) {
			return equals((IDRange)o);
		}
		return false;
	}

	public boolean equals(IDRange r) {
		return
			endID == r.endID &&
			currStartID.equals(r.currStartID);
			
	}

	public int compareTo(IDRange other) {
		long diff = endID - other.endID;
		if (diff < 0) {
			return LESS_THAN;
		}
		if (diff > 0) {
			return GREATER_THAN;
		}
		return EQUAL;
	}

	public IDRange copy() {
		return copy(false);
	}

	public IDRange copy(boolean readOnly) {
		return new IDRange(currStartID.get(), endID, readOnly);
	}

	public static IDRange parse(String line) {
		int rangeSepLoc = line.indexOf(IDGeneratorConstants.RANGE_VALUES_SEPARATOR);
		if (rangeSepLoc == NOT_FOUND) {
			throw new IllegalArgumentException("Invalid IDRange format: "+line);
		}
		long startID = Long.parseLong(line.substring(0, rangeSepLoc)); 
		long endID = Long.parseLong(line.substring(rangeSepLoc + 1));
		IDRange range = new IDRange(startID, endID);
		return range;
    }

    public static List<IDRange> parseMultiple(String line) {
        List<IDRange> ranges = new ArrayList<IDRange>();
        String[] rangesStr = line.split(Character.toString(IDGeneratorConstants.RANGE_SEPARATOR));
        for (String rangeStr : rangesStr) {
            IDRange range = IDRange.parse(rangeStr);
            ranges.add(range);
        }
        return ranges;
	}

    public IDRange tryMerge(IDRange newRange) {
        if (!isAdjacent(newRange)) {
            return null;
        }
        return new IDRange(getStartID(), newRange.getEndID()); 
    }

    public boolean isAdjacent(IDRange newRange) {
        return getEndID() == newRange.getStartID() - 1;
    }


	void setReadOnly() {
		readOnly = true;
	}

	void validateNotReadOnly() {
		if (readOnly) {
			throw new IllegalStateException("IDRange is read-only: "+toString());
		}
	}

	private static final int LESS_THAN = -1;
	private static final int EQUAL = 0;
	private static final int GREATER_THAN = 1;
	private static final int NOT_FOUND = -1;
	private AtomicLong currStartID;
	private long endID;
	private boolean readOnly;
}
