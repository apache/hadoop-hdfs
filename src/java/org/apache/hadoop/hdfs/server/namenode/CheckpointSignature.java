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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;

/**
 * A unique signature intended to identify checkpoint transactions.
 */
@InterfaceAudience.Private
public class CheckpointSignature extends StorageInfo 
                      implements WritableComparable<CheckpointSignature> {
  private static final String FIELD_SEPARATOR = ":";
  MD5Hash imageDigest = null;

  String blockpoolID = "";
  
  long lastCheckpointTxId;
  long lastLogRollTxId;

  public CheckpointSignature() {}

  CheckpointSignature(FSImage fsImage) {
    super(fsImage.getStorage());
    blockpoolID = fsImage.getBlockPoolID();
    
    lastCheckpointTxId = fsImage.getStorage().getCheckpointTxId();
    lastLogRollTxId = fsImage.getEditLog().getLastRollTxId();
    imageDigest = fsImage.getStorage().getImageDigest();
  }

  CheckpointSignature(String str) {
    String[] fields = str.split(FIELD_SEPARATOR);
    assert fields.length == 8 : "Must be 8 fields in CheckpointSignature";
    layoutVersion = Integer.valueOf(fields[0]);
    namespaceID = Integer.valueOf(fields[1]);
    cTime = Long.valueOf(fields[2]);
    lastCheckpointTxId  = Long.valueOf(fields[3]);
    lastLogRollTxId  = Long.valueOf(fields[4]);
    imageDigest = new MD5Hash(fields[5]);
    clusterID = fields[6];
    blockpoolID = fields[7];
  }

  /**
   * Get the MD5 image digest
   * @return the MD5 image digest
   */
  MD5Hash getImageDigest() {
    return imageDigest;
  }

  /**
   * Get the cluster id from CheckpointSignature
   * @return the cluster id
   */
  public String getClusterID() {
    return clusterID;
  }

  /**
   * Get the block pool id from CheckpointSignature
   * @return the block pool id
   */
  public String getBlockpoolID() {
    return blockpoolID;
  }

  /**
   * Set the block pool id of CheckpointSignature.
   * 
   * @param blockpoolID the new blockpool id
   */
  public void setBlockpoolID(String blockpoolID) {
    this.blockpoolID = blockpoolID;
  }
  
  public String toString() {
    return String.valueOf(layoutVersion) + FIELD_SEPARATOR
         + String.valueOf(namespaceID) + FIELD_SEPARATOR
         + String.valueOf(cTime) + FIELD_SEPARATOR
         + String.valueOf(lastCheckpointTxId) + FIELD_SEPARATOR
         + String.valueOf(lastLogRollTxId) + FIELD_SEPARATOR
         + imageDigest.toString() + FIELD_SEPARATOR
         + clusterID + FIELD_SEPARATOR
         + blockpoolID ;
  }

  void validateStorageInfo(FSImage si) throws IOException {
    if(layoutVersion != si.getStorage().layoutVersion
       || namespaceID != si.getStorage().namespaceID 
       || cTime != si.getStorage().cTime
       || !imageDigest.equals(si.getStorage().getImageDigest())
       || !clusterID.equals(si.getClusterID())
       || !blockpoolID.equals(si.getBlockPoolID())) {
      // checkpointTime can change when the image is saved - do not compare
      throw new IOException("Inconsistent checkpoint fields.\n"
          + "LV = " + layoutVersion + " namespaceID = " + namespaceID
          + " cTime = " + cTime
          + " ; imageDigest = " + imageDigest
          + " ; clusterId = " + clusterID
          + " ; blockpoolId = " + blockpoolID
          + ".\nExpecting respectively: "
          + si.getStorage().layoutVersion + "; " 
          + si.getStorage().namespaceID + "; " + si.getStorage().cTime
          + "; " + si.getStorage().getImageDigest()
          + "; " + si.getClusterID() + "; " 
          + si.getBlockPoolID() + ".");
    }
  }

  //
  // Comparable interface
  //
  public int compareTo(CheckpointSignature o) {
    return ComparisonChain.start()
      .compare(layoutVersion, o.layoutVersion)
      .compare(namespaceID, o.namespaceID)
      .compare(cTime, o.cTime)
      .compare(lastCheckpointTxId, o.lastCheckpointTxId)
      .compare(lastLogRollTxId, o.lastLogRollTxId)
      .compare(imageDigest, o.imageDigest)
      .compare(clusterID, o.clusterID)
      .compare(blockpoolID, o.blockpoolID)
      .result();
  }

  public boolean equals(Object o) {
    if (!(o instanceof CheckpointSignature)) {
      return false;
    }
    return compareTo((CheckpointSignature)o) == 0;
  }

  public int hashCode() {
    return layoutVersion ^ namespaceID ^
            (int)(cTime ^ lastCheckpointTxId ^ lastLogRollTxId)
            ^ clusterID.hashCode() ^ blockpoolID.hashCode()
            ^ imageDigest.hashCode();
  }

  /////////////////////////////////////////////////
  // Writable
  /////////////////////////////////////////////////
  public void write(DataOutput out) throws IOException {
    super.write(out);
    WritableUtils.writeString(out, blockpoolID);
    out.writeLong(lastCheckpointTxId);
    out.writeLong(lastLogRollTxId);
    imageDigest.write(out);
  }

  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    blockpoolID = WritableUtils.readString(in);
    lastCheckpointTxId = in.readLong();
    lastLogRollTxId = in.readLong();
    imageDigest = new MD5Hash();
    imageDigest.readFields(in);
  }
}
