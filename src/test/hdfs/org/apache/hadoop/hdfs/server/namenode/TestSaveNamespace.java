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

import static org.apache.hadoop.hdfs.server.common.Util.fileAsURI;

import static org.junit.Assert.*;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.FSConstants.SafeModeAction;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.log4j.Level;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Test various failure scenarios during saveNamespace() operation.
 * Cases covered:
 * <ol>
 * <li>Recover from failure while saving into the second storage directory</li>
 * <li>Recover from failure while moving current into lastcheckpoint.tmp</li>
 * <li>Recover from failure while moving lastcheckpoint.tmp into
 * previous.checkpoint</li>
 * <li>Recover from failure while rolling edits file</li>
 * </ol>
 */
public class TestSaveNamespace {
  static {
    ((Log4JLogger)FSImage.LOG).getLogger().setLevel(Level.ALL);
  }
  
  private static final Log LOG = LogFactory.getLog(TestSaveNamespace.class);

  private static class FaultySaveImage implements Answer<Void> {
    int count = 0;
    boolean exceptionType = true;

    // generate a RuntimeException
    public FaultySaveImage() {
      this.exceptionType = true;
    }

    // generate either a RuntimeException or IOException
    public FaultySaveImage(boolean etype) {
      this.exceptionType = etype;
    }

    public Void answer(InvocationOnMock invocation) throws Throwable {
      Object[] args = invocation.getArguments();
      StorageDirectory sd = (StorageDirectory)args[0];

      if (count++ == 1) {
        LOG.info("Injecting fault for sd: " + sd);
        if (exceptionType) {
          throw new RuntimeException("Injected fault: saveFSImage second time");
        } else {
          throw new IOException("Injected fault: saveFSImage second time");
        }
      }
      LOG.info("Not injecting fault for sd: " + sd);
      return (Void)invocation.callRealMethod();
    }
  }

  private enum Fault {
    SAVE_FSIMAGE,
    MOVE_CURRENT,
    MOVE_LAST_CHECKPOINT
  };

  private void saveNamespaceWithInjectedFault(Fault fault) throws IOException {
    Configuration conf = getConf();
    NameNode.initMetrics(conf, NamenodeRole.ACTIVE);
    DFSTestUtil.formatNameNode(conf);
    FSNamesystem fsn = new FSNamesystem(conf);

    // Replace the FSImage with a spy
    FSImage originalImage = fsn.dir.fsImage;
    NNStorage storage = originalImage.getStorage();
    storage.close(); // unlock any directories that FSNamesystem's initialization may have locked

    NNStorage spyStorage = spy(storage);
    originalImage.storage = spyStorage;

    FSImage spyImage = spy(originalImage);
    fsn.dir.fsImage = spyImage;

    // inject fault
    switch(fault) {
    case SAVE_FSIMAGE:
      // The spy throws a RuntimeException when writing to the second directory
      doAnswer(new FaultySaveImage()).
        when(spyImage).saveFSImage((StorageDirectory)anyObject(), anyLong());
      break;

    /*
     TODO: these two cases no longer make sense for HDFS-1073. Need to go
     through saveNamespace path and find other good points to inject errors
     for this test.
    case MOVE_CURRENT:
      // The spy throws a RuntimeException when calling moveCurrent()
      doThrow(new RuntimeException("Injected fault: moveCurrent")).
        when(spyStorage).moveCurrent((StorageDirectory)anyObject());
      break;
    case MOVE_LAST_CHECKPOINT:
      // The spy throws a RuntimeException when calling moveLastCheckpoint()
      doThrow(new RuntimeException("Injected fault: moveLastCheckpoint")).
        when(spyStorage).moveLastCheckpoint((StorageDirectory)anyObject());
      break;
      */
    }

    try {
      doAnEdit(fsn, 1);

      // Save namespace - this will fail because we inject a fault.
      fsn.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      try {
        fsn.saveNamespace();
      } catch (Exception e) {
        LOG.info("Test caught expected exception", e);
      }

      // Now shut down and restart the namesystem
      originalImage.close();
      fsn.close();      
      fsn = null;

      // Start a new namesystem, which should be able to recover
      // the namespace from the previous incarnation.
      fsn = new FSNamesystem(conf);

      // Make sure the image loaded including our edit.
      checkEditExists(fsn, 1);
    } finally {
      if (fsn != null) {
        fsn.close();
      }
    }
  }

  /**
   * Verify that a saveNamespace command brings faulty directories
   * in fs.name.dir and fs.edit.dir back online.
   */
  @Test
  public void testReinsertnamedirsInSavenamespace() throws Exception {
    // create a configuration with the key to restore error
    // directories in fs.name.dir
    Configuration conf = getConf();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_RESTORE_KEY, true);

    NameNode.initMetrics(conf, NamenodeRole.ACTIVE);
    DFSTestUtil.formatNameNode(conf);
    FSNamesystem fsn = new FSNamesystem(conf);

    // Replace the FSImage with a spy
    FSImage originalImage = fsn.dir.fsImage;
    NNStorage storage = originalImage.getStorage();

    FSImage spyImage = spy(originalImage);
    fsn.dir.fsImage = spyImage;
    
    File currentDir = storage.getStorageDir(0).getCurrentDir();
    currentDir.setExecutable(false);
    currentDir.setReadable(false);

    try {
      doAnEdit(fsn, 1);
      fsn.setSafeMode(SafeModeAction.SAFEMODE_ENTER);

      // Save namespace - should mark the first storage dir as faulty
      // since it's not traversable.
      LOG.info("Doing the first savenamespace.");
      fsn.saveNamespace();
      LOG.info("First savenamespace sucessful.");      
      
      assertTrue("Savenamespace should have marked one directory as bad." +
                 " But found " + storage.getRemovedStorageDirs().size() +
                 " bad directories.", 
                   storage.getRemovedStorageDirs().size() == 1);

      currentDir.setExecutable(true);
      currentDir.setReadable(true);

      // The next call to savenamespace should try inserting the
      // erroneous directory back to fs.name.dir. This command should
      // be successful.
      LOG.info("Doing the second savenamespace.");
      fsn.saveNamespace();
      LOG.warn("Second savenamespace sucessful.");
      assertTrue("Savenamespace should have been successful in removing " +
                 " bad directories from Image."  +
                 " But found " + storage.getRemovedStorageDirs().size() +
                 " bad directories.", 
                 storage.getRemovedStorageDirs().size() == 0);

      // Now shut down and restart the namesystem
      LOG.info("Shutting down fsimage.");
      originalImage.close();
      fsn.close();      
      fsn = null;

      // Start a new namesystem, which should be able to recover
      // the namespace from the previous incarnation.
      LOG.info("Loading new FSmage from disk.");
      fsn = new FSNamesystem(conf);

      // Make sure the image loaded including our edit.
      LOG.info("Checking reloaded image.");
      checkEditExists(fsn, 1);
      LOG.info("Reloaded image is good.");
    } finally {
      if (currentDir.exists()) {
        currentDir.setExecutable(true);
        currentDir.setReadable(true);
      }

      if (fsn != null) {
        try {
          fsn.close();
        } catch (Throwable t) {
          LOG.fatal("Failed to shut down", t);
        }
      }
    }
  }

  @Test
  public void testCrashWhileSavingSecondImage() throws Exception {
    saveNamespaceWithInjectedFault(Fault.SAVE_FSIMAGE);
  }

  @Test
  public void testCrashWhileMoveCurrent() throws Exception {
    saveNamespaceWithInjectedFault(Fault.MOVE_CURRENT);
  }

  @Test
  public void testCrashWhileMoveLastCheckpoint() throws Exception {
    saveNamespaceWithInjectedFault(Fault.MOVE_LAST_CHECKPOINT);
  }
 

  /**
   * Test case where savenamespace fails in all directories
   * and then the NN shuts down. Here we should recover from the
   * failed checkpoint by moving the directories back on next
   * NN start. This is a regression test for HDFS-1921.
   */
  @Test
  public void testFailedSaveNamespace() throws Exception {
    doTestFailedSaveNamespace(false);
  }

  /**
   * Test case where saveNamespace fails in all directories, but then
   * the operator restores the directories and calls it again.
   * This should leave the NN in a clean state for next start.
   */
  @Test
  public void testFailedSaveNamespaceWithRecovery() throws Exception {
    doTestFailedSaveNamespace(true);
  }

  /**
   * Injects a failure on all storage directories while saving namespace.
   *
   * @param restoreStorageAfterFailure if true, will try to save again after
   *   clearing the failure injection
   */
  public void doTestFailedSaveNamespace(boolean restoreStorageAfterFailure)
  throws Exception {
    Configuration conf = getConf();
    NameNode.initMetrics(conf, NamenodeRole.ACTIVE);
    DFSTestUtil.formatNameNode(conf);
    FSNamesystem fsn = new FSNamesystem(conf);

    // Replace the FSImage with a spy
    final FSImage originalImage = fsn.dir.fsImage;
    NNStorage storage = originalImage.getStorage();
    storage.close(); // unlock any directories that FSNamesystem's initialization may have locked

    NNStorage spyStorage = spy(storage);
    originalImage.storage = spyStorage;
    FSImage spyImage = spy(originalImage);
    fsn.dir.fsImage = spyImage;
    spyImage.storage.setStorageDirectories(
        FSNamesystem.getNamespaceDirs(conf), 
        FSNamesystem.getNamespaceEditsDirs(conf));

    doThrow(new IOException("Injected fault: saveFSImage")).
      when(spyImage).saveFSImage((StorageDirectory)anyObject(),
                                 Mockito.anyLong());

    try {
      doAnEdit(fsn, 1);

      // Save namespace
      fsn.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      try {
        fsn.saveNamespace();
        fail("saveNamespace did not fail even when all directories failed!");
      } catch (IOException ioe) {
        LOG.info("Got expected exception", ioe);
      }
      
      // Ensure that, if storage dirs come back online, things work again.
      if (restoreStorageAfterFailure) {
        Mockito.reset(spyImage);
        spyStorage.setRestoreFailedStorage(true);
        fsn.saveNamespace();
        checkEditExists(fsn, 1);
      }

      // Now shut down and restart the NN
      originalImage.close();
      fsn.close();
      fsn = null;

      // Start a new namesystem, which should be able to recover
      // the namespace from the previous incarnation.
      fsn = new FSNamesystem(conf);

      // Make sure the image loaded including our edits.
      checkEditExists(fsn, 1);
    } finally {
      if (fsn != null) {
        fsn.close();
      }
    }
  }

  @Test
  public void testSaveWhileEditsRolled() throws Exception {
    Configuration conf = getConf();
    NameNode.initMetrics(conf, NamenodeRole.ACTIVE);
    DFSTestUtil.formatNameNode(conf);
    FSNamesystem fsn = new FSNamesystem(conf);

    try {
      doAnEdit(fsn, 1);
      CheckpointSignature sig = fsn.rollEditLog();
      LOG.warn("Checkpoint signature: " + sig);
      // Do another edit
      doAnEdit(fsn, 2);

      // Save namespace
      fsn.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      fsn.saveNamespace();

      // Now shut down and restart the NN
      fsn.close();
      fsn = null;

      // Start a new namesystem, which should be able to recover
      // the namespace from the previous incarnation.
      fsn = new FSNamesystem(conf);

      // Make sure the image loaded including our edits.
      checkEditExists(fsn, 1);
      checkEditExists(fsn, 2);
    } finally {
      if (fsn != null) {
        fsn.close();
      }
    }
  }
  
  @Test
  public void testTxIdPersistence() throws Exception {
    Configuration conf = getConf();
    NameNode.initMetrics(conf, NamenodeRole.ACTIVE);
    DFSTestUtil.formatNameNode(conf);
    FSNamesystem fsn = new FSNamesystem(conf);

    try {
      // We have a BEGIN_LOG_SEGMENT txn to start
      assertEquals(1, fsn.getEditLog().getLastWrittenTxId());
      doAnEdit(fsn, 1);
      assertEquals(2, fsn.getEditLog().getLastWrittenTxId());
      
      fsn.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
      fsn.saveNamespace();

      // 2 more txns: END the first segment, BEGIN a new one
      assertEquals(4, fsn.getEditLog().getLastWrittenTxId());
      
      // Shut down and restart
      fsn.getFSImage().close();
      fsn.close();
      
      // 1 more txn to END that segment
      assertEquals(5, fsn.getEditLog().getLastWrittenTxId());
      fsn = null;
      
      fsn = new FSNamesystem(conf);
      // 1 more txn to start new segment on restart
      assertEquals(6, fsn.getEditLog().getLastWrittenTxId());
      
    } finally {
      if (fsn != null) {
        fsn.close();
      }
    }
  }
  
  private void doAnEdit(FSNamesystem fsn, int id) throws IOException {
    // Make an edit
    fsn.mkdirs(
      "/test" + id,
      new PermissionStatus("test", "Test",
          new FsPermission((short)0777)),
          true);
  }

  private void checkEditExists(FSNamesystem fsn, int id) throws IOException {
    // Make sure the image loaded including our edit.
    assertNotNull(fsn.getFileInfo("/test" + id, false));
  }

  private Configuration getConf() throws IOException {
    String baseDir = MiniDFSCluster.getBaseDirectory();
    String nameDirs = fileAsURI(new File(baseDir, "name1")) + "," + 
                      fileAsURI(new File(baseDir, "name2"));

    Configuration conf = new HdfsConfiguration();
    FileSystem.setDefaultUri(conf, "hdfs://localhost:0");
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, "0.0.0.0:0");
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, nameDirs);
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY, nameDirs);
    conf.set(DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY, "0.0.0.0:0");
    conf.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, false); 
    return conf;
  }
}
