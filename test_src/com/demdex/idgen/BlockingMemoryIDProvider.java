package com.demdex.idgen;

import com.demdex.util.Blocker;

/**
 * An MemoryIDProvider used for concurrency testing, which blocks on writes. 
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

public class BlockingMemoryIDProvider extends MemoryIDProvider {

	public BlockingMemoryIDProvider(IDCategory category, int minID, int maxID) {
		super(category, minID, maxID);
	}

	public void setData(IDCategory category, RawIDSetData idContents) throws IDGeneratorException {
		try {
			blocker.startBlocking();
		}
		catch (Exception e) {
//			e.printStackTrace();
			throw new RuntimeException(e);
		}
		super.setData(category, idContents);
	}

	public void setBlockingEnabled(boolean enabled) {
		blocker.setBlockingEnabled(enabled);
	}

	public boolean isBlocking() {
		return blocker.isBlocking();
	}

	public void stopBlocking() {
		blocker.stopBlocking();
	}

	private Blocker blocker = new Blocker();
}