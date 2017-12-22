package com.demdex.util.io;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;

/**
 * A collection of various static utility/helper methods for working with java.io Readers and Writers
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

public abstract class CharacterStreamUtils {

	public static IOException close(Reader in) {
	    if (in == null) {
	        return null;
	    }
	
		// no real use of throwing an IOException that occurs on Reader.close()
		// so if one occurs, return it instead of throwing it
	    // if a calling class cares, it can throw it itself
	    try {
	        in.close();
	        return null;
	    }
	    catch (IOException e) {
	        return e;
	    }
	}

	public static void close(Reader in, Logger logger, String streamType) {
	    if (in == null) {
	        return;
	    }
	
		// no real use of throwing an IOException that occurs on Reader.close()
		// so if one occurs, return it instead of throwing it
	    // if a calling class cares, it can pass in a logger and we'll log it
	    try {
	        in.close();
	    }
	    catch (IOException e) {
	    	if (logger != null) {
	    		logger.error("Non-fatal error occurred while closing "+streamType+" reader", e);
	    	}
	    }
	}

	public static void close(Logger logger, String streamTypes, Reader... readers) {
		for (Reader reader : readers) {
			close(reader, logger, streamTypes);
		}
	}

	public static void close(Collection<? extends Reader> readers, Logger logger, String streamTypes) {
		for (Reader reader : readers) {
			close(reader, logger, streamTypes);
		}
	}

	public static void flushAndClose(Writer out, Logger logger, String streamType) throws IOException {
	    if (out == null) {
	        return;
	    }
	
		// if an IOException occurs on Writer.flush(), then that's a potential data integrity problem
	    // so throw that IOException ...
	    out.flush();
	
		// ... but if the flush succeeded, then there's no real use of throwing an IOException that occurs on Writer.close()
		// so if one occurs, log it instead of throwing it
	    try {
	        out.close();
	    }
	    catch (IOException e) {
	    	if (logger != null) {
				logger.error("Non-fatal error occurred while closing "+streamType+" output stream", e);
	    	}
	    }
	}

	public static IOException flushAndClose(Writer out) throws IOException {
	    if (out == null) {
	        return null;
	    }
	
		// if an IOException occurs on Writer.flush(), then that's a potential data integrity problem
	    // so throw that IOException ...
	    out.flush();
	
		// ... but if the flush succeeded, then there's no real use of throwing an IOException that occurs on Writer.close()
		// so if one occurs, return it instead of throwing it
	    // if a calling class cares, it can throw it itself
	    try {
	        out.close();
	    }
	    catch (IOException e) {
	        return e;
	    }
	    return null;
	}

	public static IOException[] flushAndClose(Writer... writers) throws IOException {
		List<IOException> nonFatalExceptions = new ArrayList<IOException>();
		IOException fatalException = null;
		for (Writer writer : writers) {
			try {
				IOException nonFatalException = flushAndClose(writer);
				nonFatalExceptions.add(nonFatalException);
			}
			catch (IOException e) {
				if (fatalException  == null) {
					fatalException = e;
				}
			}
		}
		if (fatalException != null) {
			throw fatalException;
		}
		IOException[] nonFataExceptionArr = new IOException[nonFatalExceptions.size()];
		nonFatalExceptions.toArray(nonFataExceptionArr);
		return nonFataExceptionArr;
	}

	public static IOException[] flushAndClose(Collection<? extends Writer> writers) throws IOException {
		Writer[] writersArr = new Writer[writers.size()];
		writers.toArray(writersArr);
		return flushAndClose(writersArr);
	}

	public static void flushAndClose(Reader in, Writer out, Logger logger, String streamTypes) throws IOException {
		try {
			flushAndClose(out, logger, streamTypes);
		}
		finally {
			close(in, logger, streamTypes);
		}
	}

	public static BufferedReader toBufferedReader(Reader reader, int blockSize) {
		if (reader instanceof BufferedReader) {
			return (BufferedReader)reader;
		}
		return new BufferedReader(reader, blockSize);
	}

	public static BufferedWriter toBufferedWriter(Writer writer, int blockSize) {
		if (writer instanceof BufferedWriter) {
			return (BufferedWriter)writer;
		}
		return new BufferedWriter(writer, blockSize);
	}

	public static PrintWriter toBufferedPrintWriter(Writer writer, int blockSize) {
		return new PrintWriter(toBufferedWriter(writer, blockSize));
	}

	public static Reader openFileForRead(String dirName, String fileName) throws IOException {
		return new FileReader(new File(dirName, fileName));
	}

	public static Reader openFileForRead(File dir, String fileName) throws IOException {
		return new FileReader(new File(dir, fileName));
	}

	public static Writer openFileForWrite(String dirName, String fileName, boolean append) throws IOException {
		File file = new File(dirName, fileName);
		return openFileForWrite(file, append);
	}

	public static Writer openFileForWrite(File dir, String fileName, boolean append) throws IOException {
		File file = new File(dir, fileName);
		return openFileForWrite(file, append);
	}

	public static Writer openFileForWrite(File file, boolean append) throws IOException {
		return new FileWriter(file, append);
	}
}
