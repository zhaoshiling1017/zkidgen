package com.demdex.idgen;

import java.nio.charset.Charset;

/**
 * Miscellaneous constants used throughout the id generation process
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

public abstract class IDGeneratorConstants {
	public static final int DEFAULT_ID_SET_SIZE = 1000;
	public static final int DEFAULT_MAX_TRY_COUNT = 3;
    public static final char RANGE_SEPARATOR = ',';
	public static final char RANGE_VALUES_SEPARATOR = '-';
	public static final Charset UTF8_CHARSET = Charset.forName("UTF-8");
	public static final int ONE_KILO_BYTE = 1024;
	public static final int DEFAULT_BUFFER_SIZE = 16 * ONE_KILO_BYTE;
}
