/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;

/**
 * Journal manager for the common case of edits files being written
 * to a storage directory.
 */
public class FileJournalManager implements JournalManager {

  private final StorageDirectory sd;
  private EditLogFileOutputStream currentStream;
  private int sizeOutputFlushBuffer = 512*1024;

  public FileJournalManager(StorageDirectory sd) {
    this.sd = sd;
  }
  
  @Override
  public void open() throws IOException {
    assert currentStream == null;
    File eFile = NNStorage.getEditFile(sd);
    
    currentStream = new EditLogFileOutputStream(
        eFile, sizeOutputFlushBuffer);    
  }

  @Override
  public void abort() throws IOException {
    if (currentStream != null) {
      currentStream.close();
      currentStream = null;
    }
  }

  @Override
  public void close() throws IOException {
    currentStream.setReadyToFlush();
    currentStream.flush();
    currentStream.close();
    currentStream = null;
  }
  
  @Override
  public void restore() throws IOException {
    assert currentStream == null :
      "Should have been aborted before restoring!" +
      "Current stream: " + currentStream;
    open();
  }
  

  @Override
  public void divertFileStreams(String dest) throws IOException {
    // close old stream
    close();
    // create new stream
    currentStream = new EditLogFileOutputStream(new File(sd.getRoot(), dest),
        sizeOutputFlushBuffer);
    currentStream.create();    
  }

  @Override
  public void revertFileStreams(String source) throws IOException {
    // close old stream
    close();
    
    // rename edits.new to edits
    File editFile = NNStorage.getEditFile(sd);
    File prevEditFile = new File(sd.getRoot(), source);
    if(prevEditFile.exists()) {
      if(!prevEditFile.renameTo(editFile)) {
        //
        // renameTo() fails on Windows if the destination
        // file exists.
        //
        if(!editFile.delete() || !prevEditFile.renameTo(editFile)) {
          throw new IOException("Rename failed for " + sd.getRoot());
        }
      }
    }
    // open new stream
    currentStream = new EditLogFileOutputStream(editFile, sizeOutputFlushBuffer);
  }

  @Override
  public StorageDirectory getStorageDirectory() {
    return sd;
  }

  @Override
  public EditLogOutputStream getCurrentStream() {
    return currentStream;
  }

  @Override
  public void setBufferCapacity(int size) {
    this.sizeOutputFlushBuffer = size;
  }

  void setCurrentStreamForTests(EditLogFileOutputStream injectedStream) {
    this.currentStream = injectedStream;
  }
}
