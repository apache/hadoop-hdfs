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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * Journal manager for the common case of edits files being written
 * to a storage directory.
 */
public class FileJournalManager implements JournalManager {

  private final StorageDirectory sd;
  private EditLogFileOutputStream currentStream;
  private int outputBufferCapacity = 512*1024;

  public FileJournalManager(StorageDirectory sd) {
    this.sd = sd;
  }
  
  private boolean isCurrentStreamClosed() {
    return currentStream == null || !currentStream.isOpen();
  }
  
  @Override
  public EditLogOutputStream createStream() throws IOException {
    Preconditions.checkState(isCurrentStreamClosed());
    File eFile = NNStorage.getEditFile(sd);
    
    currentStream = new EditLogFileOutputStream(
        eFile, outputBufferCapacity);
    return currentStream;
  }

  @Override
  public EditLogOutputStream createDivertedStream(String dest)
    throws IOException {

    // create new stream
    currentStream = new EditLogFileOutputStream(new File(sd.getRoot(), dest),
        outputBufferCapacity);
    currentStream.create();
    return currentStream;    
  }

  @Override
  public EditLogOutputStream createRevertedStream(String source)
      throws IOException {
    Preconditions.checkState(isCurrentStreamClosed());
    
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
    
    return createStream();
  }

  @VisibleForTesting
  public StorageDirectory getStorageDirectory() {
    return sd;
  }

  @Override
  public void setOutputBufferCapacity(int size) {
    this.outputBufferCapacity = size;
  }

}
