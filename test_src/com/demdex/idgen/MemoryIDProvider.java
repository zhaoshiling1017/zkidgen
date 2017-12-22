package com.demdex.idgen;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicStampedReference;


/**
 * An IDProvider used for unit testing, which reads/writes ID generation state data to/from a hash map in memory.
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

public class MemoryIDProvider implements IDProvider {

	public MemoryIDProvider(IDCategory category) {
		this(category, "");
	}

	public MemoryIDProvider(IDCategory category, int minID, int maxID) {
		this(
			category,
			Integer.toString(minID) + IDGeneratorConstants.RANGE_VALUES_SEPARATOR + Integer.toString(maxID)
		);
	}

	private MemoryIDProvider(IDCategory category, String rangeStr) {
		String categoryName = category.getName();
		ids.put(categoryName, new IDData(category, rangeStr, 1));
	}


	public void open() {
	}

	public String getName() {
		return MemoryIDProvider.class.getSimpleName();
	}

	public RawIDSetData getData(IDCategory category) throws IDGeneratorException {
		String categoryName = category.getName();
		IDData idData = ids.get(categoryName);
		if (idData == null) {
			throw new IDGeneratorException("No id data found for category: "+categoryName);
		}
		return new RawIDSetData(idData.getVersion(), idData.getText().getBytes(IDGeneratorConstants.UTF8_CHARSET));
	}

	public void setData(IDCategory category, RawIDSetData idContents) throws IDGeneratorException {
		String categoryName = category.getName();
		IDData idData = ids.get(categoryName);
		idData.set(idContents.getVersion(), new String(idContents.getData(), IDGeneratorConstants.UTF8_CHARSET));
	}

	public void close() {
	}

	private static class IDData {

		public IDData(IDCategory category, String rangeStr, int i) {
			this.category = category;
			data = new AtomicStampedReference<String>(rangeStr, i);
		}

		public int getVersion() {
			return data.getStamp();
		}

		public String getText() {
			return data.getReference();
		}

		public void set(int expectedVersion, String newText) throws IDProviderVersionException {
			boolean success = data.compareAndSet(data.getReference(), newText, expectedVersion, expectedVersion + 1);
			if (!success) {
				throw new IDProviderVersionException(
					"Set data failed",
					category,
					expectedVersion,
					data.getStamp()
				);
			}
		}

		private IDCategory category;
		private AtomicStampedReference<String> data;
	}

	private Map<String,IDData> ids = new ConcurrentHashMap<String,IDData>();
}
