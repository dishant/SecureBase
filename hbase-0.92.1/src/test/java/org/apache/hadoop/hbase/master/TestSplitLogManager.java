/**
 * Copyright 2011 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.master;

import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.master.SplitLogManager.Task;
import org.apache.hadoop.hbase.master.SplitLogManager.TaskBatch;
import org.apache.hadoop.hbase.regionserver.TestMasterAddressManager.NodeCreationListener;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog.TaskState;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestSplitLogManager {
  private static final Log LOG = LogFactory.getLog(TestSplitLogManager.class);
  static {
    Logger.getLogger("org.apache.hadoop.hbase").setLevel(Level.DEBUG);
  }

  private ZooKeeperWatcher zkw;
  private static boolean stopped = false;
  private SplitLogManager slm;
  private Configuration conf;

  private final static HBaseTestingUtility TEST_UTIL =
    new HBaseTestingUtility();

  static Stoppable stopper = new Stoppable() {
    @Override
    public void stop(String why) {
      stopped = true;
    }

    @Override
    public boolean isStopped() {
      return stopped;
    }

  };

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  @Before
  public void setup() throws Exception {
    TEST_UTIL.startMiniZKCluster();
    conf = TEST_UTIL.getConfiguration();
    zkw = new ZooKeeperWatcher(conf, "split-log-manager-tests", null);
    ZKUtil.deleteChildrenRecursively(zkw, zkw.baseZNode);
    ZKUtil.createAndFailSilent(zkw, zkw.baseZNode);
    assertTrue(ZKUtil.checkExists(zkw, zkw.baseZNode) != -1);
    LOG.debug(zkw.baseZNode + " created");
    ZKUtil.createAndFailSilent(zkw, zkw.splitLogZNode);
    assertTrue(ZKUtil.checkExists(zkw, zkw.splitLogZNode) != -1);
    LOG.debug(zkw.splitLogZNode + " created");

    stopped = false;
    resetCounters();
  }

  @After
  public void teardown() throws IOException, KeeperException {
    stopper.stop("");
    slm.stop();
    TEST_UTIL.shutdownMiniZKCluster();
  }

  private interface Expr {
    public long eval();
  }

  private void waitForCounter(final AtomicLong ctr, long oldval, long newval,
      long timems) {
    Expr e = new Expr() {
      public long eval() {
        return ctr.get();
      }
    };
    waitForCounter(e, oldval, newval, timems);
    return;
  }

  private void waitForCounter(Expr e, long oldval, long newval,
      long timems) {
    long curt = System.currentTimeMillis();
    long endt = curt + timems;
    while (curt < endt) {
      if (e.eval() == oldval) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException eintr) {
        }
        curt = System.currentTimeMillis();
      } else {
        assertEquals(newval, e.eval());
        return;
      }
    }
    assertTrue(false);
  }

  private String submitTaskAndWait(TaskBatch batch, String name)
  throws KeeperException, InterruptedException {
    String tasknode = ZKSplitLog.getEncodedNodeName(zkw, name);
    NodeCreationListener listener = new NodeCreationListener(zkw, tasknode);
    zkw.registerListener(listener);
    ZKUtil.watchAndCheckExists(zkw, tasknode);

    slm.installTask(name, batch);
    assertEquals(1, batch.installed);
    assertTrue(slm.findOrCreateOrphanTask(tasknode).batch == batch);
    assertEquals(1L, tot_mgr_node_create_queued.get());

    LOG.debug("waiting for task node creation");
    listener.waitForCreation();
    LOG.debug("task created");
    return tasknode;
  }

  /**
   * Test whether the splitlog correctly creates a task in zookeeper
   * @throws Exception
   */
  @Test
  public void testTaskCreation() throws Exception {
    LOG.info("TestTaskCreation - test the creation of a task in zk");

    slm = new SplitLogManager(zkw, conf, stopper, "dummy-master", null);
    slm.finishInitialization();
    TaskBatch batch = new TaskBatch();

    String tasknode = submitTaskAndWait(batch, "foo/1");

    byte[] data = ZKUtil.getData(zkw, tasknode);
    LOG.info("Task node created " + new String(data));
    assertTrue(TaskState.TASK_UNASSIGNED.equals(data, "dummy-master"));
  }

  @Test
  public void testOrphanTaskAcquisition() throws Exception {
    LOG.info("TestOrphanTaskAcquisition");

    String tasknode = ZKSplitLog.getEncodedNodeName(zkw, "orphan/test/slash");
    zkw.getRecoverableZooKeeper().create(tasknode,
        TaskState.TASK_OWNED.get("dummy-worker"), Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);

    int to = 1000;
    conf.setInt("hbase.splitlog.manager.timeout", to);
    conf.setInt("hbase.splitlog.manager.timeoutmonitor.period", 100);
    to = to + 2 * 100;


    slm = new SplitLogManager(zkw, conf, stopper, "dummy-master", null);
    slm.finishInitialization();
    waitForCounter(tot_mgr_orphan_task_acquired, 0, 1, 100);
    Task task = slm.findOrCreateOrphanTask(tasknode);
    assertTrue(task.isOrphan());
    waitForCounter(tot_mgr_heartbeat, 0, 1, 100);
    assertFalse(task.isUnassigned());
    long curt = System.currentTimeMillis();
    assertTrue((task.last_update <= curt) &&
        (task.last_update > (curt - 1000)));
    LOG.info("waiting for manager to resubmit the orphan task");
    waitForCounter(tot_mgr_resubmit, 0, 1, to + 100);
    assertTrue(task.isUnassigned());
    waitForCounter(tot_mgr_rescan, 0, 1, to + 100);
  }

  @Test
  public void testUnassignedOrphan() throws Exception {
    LOG.info("TestUnassignedOrphan - an unassigned task is resubmitted at" +
        " startup");
    String tasknode = ZKSplitLog.getEncodedNodeName(zkw, "orphan/test/slash");
    //create an unassigned orphan task
    zkw.getRecoverableZooKeeper().create(tasknode,
        TaskState.TASK_UNASSIGNED.get("dummy-worker"), Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);
    int version = ZKUtil.checkExists(zkw, tasknode);

    slm = new SplitLogManager(zkw, conf, stopper, "dummy-master", null);
    slm.finishInitialization();
    waitForCounter(tot_mgr_orphan_task_acquired, 0, 1, 100);
    Task task = slm.findOrCreateOrphanTask(tasknode);
    assertTrue(task.isOrphan());
    assertTrue(task.isUnassigned());
    // wait for RESCAN node to be created
    waitForCounter(tot_mgr_rescan, 0, 1, 500);
    Task task2 = slm.findOrCreateOrphanTask(tasknode);
    assertTrue(task == task2);
    LOG.debug("task = " + task);
    assertEquals(1L, tot_mgr_resubmit.get());
    assertEquals(1, task.incarnation);
    assertEquals(0, task.unforcedResubmits);
    assertTrue(task.isOrphan());
    assertTrue(task.isUnassigned());
    assertTrue(ZKUtil.checkExists(zkw, tasknode) > version);
  }

  @Test
  public void testMultipleResubmits() throws Exception {
    LOG.info("TestMultipleResbmits - no indefinite resubmissions");

    int to = 1000;
    conf.setInt("hbase.splitlog.manager.timeout", to);
    conf.setInt("hbase.splitlog.manager.timeoutmonitor.period", 100);
    to = to + 2 * 100;

    conf.setInt("hbase.splitlog.max.resubmit", 2);
    slm = new SplitLogManager(zkw, conf, stopper, "dummy-master", null);
    slm.finishInitialization();
    TaskBatch batch = new TaskBatch();

    String tasknode = submitTaskAndWait(batch, "foo/1");
    int version = ZKUtil.checkExists(zkw, tasknode);

    ZKUtil.setData(zkw, tasknode, TaskState.TASK_OWNED.get("worker1"));
    waitForCounter(tot_mgr_heartbeat, 0, 1, 1000);
    waitForCounter(tot_mgr_resubmit, 0, 1, to + 100);
    int version1 = ZKUtil.checkExists(zkw, tasknode);
    assertTrue(version1 > version);
    ZKUtil.setData(zkw, tasknode, TaskState.TASK_OWNED.get("worker2"));
    waitForCounter(tot_mgr_heartbeat, 1, 2, 1000);
    waitForCounter(tot_mgr_resubmit, 1, 2, to + 100);
    int version2 = ZKUtil.checkExists(zkw, tasknode);
    assertTrue(version2 > version1);
    ZKUtil.setData(zkw, tasknode, TaskState.TASK_OWNED.get("worker3"));
    waitForCounter(tot_mgr_heartbeat, 1, 2, 1000);
    waitForCounter(tot_mgr_resubmit_threshold_reached, 0, 1, to + 100);
    Thread.sleep(to + 100);
    assertEquals(2L, tot_mgr_resubmit.get());
  }

  @Test
  public void testRescanCleanup() throws Exception {
    LOG.info("TestRescanCleanup - ensure RESCAN nodes are cleaned up");

    conf.setInt("hbase.splitlog.manager.timeout", 1000);
    conf.setInt("hbase.splitlog.manager.timeoutmonitor.period", 100);
    slm = new SplitLogManager(zkw, conf, stopper, "dummy-master", null);
    slm.finishInitialization();
    TaskBatch batch = new TaskBatch();

    String tasknode = submitTaskAndWait(batch, "foo/1");
    int version = ZKUtil.checkExists(zkw, tasknode);

    ZKUtil.setData(zkw, tasknode, TaskState.TASK_OWNED.get("worker1"));
    waitForCounter(tot_mgr_heartbeat, 0, 1, 1000);
    waitForCounter(new Expr() {
      @Override
      public long eval() {
        return (tot_mgr_resubmit.get() + tot_mgr_resubmit_failed.get());
      }
    }, 0, 1, 5*60000); // wait long enough
    if (tot_mgr_resubmit_failed.get() == 0) {
      int version1 = ZKUtil.checkExists(zkw, tasknode);
      assertTrue(version1 > version);
      byte[] taskstate = ZKUtil.getData(zkw, tasknode);
      assertTrue(Arrays.equals(TaskState.TASK_UNASSIGNED.get("dummy-master"),
          taskstate));
      
      waitForCounter(tot_mgr_rescan_deleted, 0, 1, 1000);
    } else {
      LOG.warn("Could not run test. Lost ZK connection?");
    }

    return;
  }

  @Test
  public void testTaskDone() throws Exception {
    LOG.info("TestTaskDone - cleanup task node once in DONE state");

    slm = new SplitLogManager(zkw, conf, stopper, "dummy-master", null);
    slm.finishInitialization();
    TaskBatch batch = new TaskBatch();
    String tasknode = submitTaskAndWait(batch, "foo/1");
    ZKUtil.setData(zkw, tasknode, TaskState.TASK_DONE.get("worker"));
    synchronized (batch) {
      while (batch.installed != batch.done) {
        batch.wait();
      }
    }
    waitForCounter(tot_mgr_task_deleted, 0, 1, 1000);
    assertTrue(ZKUtil.checkExists(zkw, tasknode) == -1);
  }

  @Test
  public void testTaskErr() throws Exception {
    LOG.info("TestTaskErr - cleanup task node once in ERR state");

    conf.setInt("hbase.splitlog.max.resubmit", 0);
    slm = new SplitLogManager(zkw, conf, stopper, "dummy-master", null);
    slm.finishInitialization();
    TaskBatch batch = new TaskBatch();

    String tasknode = submitTaskAndWait(batch, "foo/1");
    ZKUtil.setData(zkw, tasknode, TaskState.TASK_ERR.get("worker"));
    synchronized (batch) {
      while (batch.installed != batch.error) {
        batch.wait();
      }
    }
    waitForCounter(tot_mgr_task_deleted, 0, 1, 1000);
    assertTrue(ZKUtil.checkExists(zkw, tasknode) == -1);
    conf.setInt("hbase.splitlog.max.resubmit", ZKSplitLog.DEFAULT_MAX_RESUBMIT);
  }

  @Test
  public void testTaskResigned() throws Exception {
    LOG.info("TestTaskResigned - resubmit task node once in RESIGNED state");

    slm = new SplitLogManager(zkw, conf, stopper, "dummy-master", null);
    slm.finishInitialization();
    TaskBatch batch = new TaskBatch();
    String tasknode = submitTaskAndWait(batch, "foo/1");
    ZKUtil.setData(zkw, tasknode, TaskState.TASK_RESIGNED.get("worker"));
    int version = ZKUtil.checkExists(zkw, tasknode);

    waitForCounter(tot_mgr_resubmit, 0, 1, 1000);
    int version1 = ZKUtil.checkExists(zkw, tasknode);
    assertTrue(version1 > version);

    byte[] taskstate = ZKUtil.getData(zkw, tasknode);
    assertTrue(Arrays.equals(taskstate,
        TaskState.TASK_UNASSIGNED.get("dummy-master")));
  }

  @Test
  public void testUnassignedTimeout() throws Exception {
    LOG.info("TestUnassignedTimeout - iff all tasks are unassigned then" +
        " resubmit");

    // create an orphan task in OWNED state
    String tasknode1 = ZKSplitLog.getEncodedNodeName(zkw, "orphan/1");
    zkw.getRecoverableZooKeeper().create(tasknode1,
        TaskState.TASK_OWNED.get("dummy-worker"), Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);

    int to = 1000;
    conf.setInt("hbase.splitlog.manager.timeout", to);
    conf.setInt("hbase.splitlog.manager.unassigned.timeout", 2 * to);
    conf.setInt("hbase.splitlog.manager.timeoutmonitor.period", 100);


    slm = new SplitLogManager(zkw, conf, stopper, "dummy-master", null);
    slm.finishInitialization();
    waitForCounter(tot_mgr_orphan_task_acquired, 0, 1, 100);


    // submit another task which will stay in unassigned mode
    TaskBatch batch = new TaskBatch();
    submitTaskAndWait(batch, "foo/1");

    // keep updating the orphan owned node every to/2 seconds
    for (int i = 0; i < (3 * to)/100; i++) {
      Thread.sleep(100);
      ZKUtil.setData(zkw, tasknode1,
          TaskState.TASK_OWNED.get("dummy-worker"));
    }

    // since we have stopped heartbeating the owned node therefore it should
    // get resubmitted
    LOG.info("waiting for manager to resubmit the orphan task");
    waitForCounter(tot_mgr_resubmit, 0, 1, to + 500);

    // now all the nodes are unassigned. manager should post another rescan
    waitForCounter(tot_mgr_resubmit_unassigned, 0, 1, 2 * to + 500);
  }

  @Test
  public void testDeadWorker() throws Exception {
    LOG.info("testDeadWorker");

    conf.setLong("hbase.splitlog.max.resubmit", 0);
    slm = new SplitLogManager(zkw, conf, stopper, "dummy-master", null);
    slm.finishInitialization();
    TaskBatch batch = new TaskBatch();

    String tasknode = submitTaskAndWait(batch, "foo/1");
    int version = ZKUtil.checkExists(zkw, tasknode);

    ZKUtil.setData(zkw, tasknode, TaskState.TASK_OWNED.get("worker1"));
    waitForCounter(tot_mgr_heartbeat, 0, 1, 1000);
    slm.handleDeadWorker("worker1");
    waitForCounter(tot_mgr_resubmit, 0, 1, 1000);
    waitForCounter(tot_mgr_resubmit_dead_server_task, 0, 1, 1000);

    int version1 = ZKUtil.checkExists(zkw, tasknode);
    assertTrue(version1 > version);
    byte[] taskstate = ZKUtil.getData(zkw, tasknode);
    assertTrue(Arrays.equals(TaskState.TASK_UNASSIGNED.get("dummy-master"),
        taskstate));
    return;
  }

  @Test
  public void testEmptyLogDir() throws Exception {
    LOG.info("testEmptyLogDir");
    slm = new SplitLogManager(zkw, conf, stopper, "dummy-master", null);
    slm.finishInitialization();
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path emptyLogDirPath = new Path(fs.getWorkingDirectory(),
        UUID.randomUUID().toString());
    fs.mkdirs(emptyLogDirPath);
    slm.splitLogDistributed(emptyLogDirPath);
    assertFalse(fs.exists(emptyLogDirPath));
  }

  @Test
  public void testVanishingTaskZNode() throws Exception {
    LOG.info("testVanishingTaskZNode");
    conf.setInt("hbase.splitlog.manager.unassigned.timeout", 0);
    slm = new SplitLogManager(zkw, conf, stopper, "dummy-master", null);
    slm.finishInitialization();
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    final Path logDir = new Path(fs.getWorkingDirectory(),
        UUID.randomUUID().toString());
    fs.mkdirs(logDir);
    Path logFile = new Path(logDir, UUID.randomUUID().toString());
    fs.createNewFile(logFile);
    new Thread() {
      public void run() {
        try {
          // this call will block because there are no SplitLogWorkers
          slm.splitLogDistributed(logDir);
        } catch (Exception e) {
          LOG.warn("splitLogDistributed failed", e);
          fail();
        }
      }
    }.start();
    waitForCounter(tot_mgr_node_create_result, 0, 1, 10000);
    String znode = ZKSplitLog.getEncodedNodeName(zkw, logFile.toString());
    // remove the task znode
    ZKUtil.deleteNode(zkw, znode);
    waitForCounter(tot_mgr_get_data_nonode, 0, 1, 30000);
    waitForCounter(tot_mgr_log_split_batch_success, 0, 1, 1000);
    assertTrue(fs.exists(logFile));
    fs.delete(logDir, true);
  }
}
