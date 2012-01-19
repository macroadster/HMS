/*
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

package org.apache.hms.controller.rest;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hms.common.conf.CommonConfigurationKeys;
import org.apache.hms.common.entity.Status;
import org.apache.hms.common.entity.action.Action;
import org.apache.hms.common.entity.action.ActionDependency;
import org.apache.hms.common.entity.action.DaemonAction;
import org.apache.hms.common.entity.action.PackageAction;
import org.apache.hms.common.entity.action.ScriptAction;
import org.apache.hms.common.entity.cluster.MachineState.StateEntry;
import org.apache.hms.common.entity.cluster.MachineState.StateType;
import org.apache.hms.common.entity.manifest.ConfigManifest;
import org.apache.hms.common.util.ExceptionUtil;
import org.apache.hms.common.util.JAXBUtil;
import org.apache.hms.common.util.ZookeeperUtil;
import org.apache.hms.controller.Controller;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;


@Path("config")
public class ConfigManager {
  private static Log LOG = LogFactory.getLog(ConfigManager.class);
  
  @GET
  @Path("manifest/create-hadoop-cluster")
  public ConfigManifest getSample(@Context UriInfo uri) {
    return Examples.createHadoopCluster;
  }
  

  @GET
  @Path("manifest/delete-cluster")
  public ConfigManifest getDestroyCluster() {    
    return Examples.deleteHadoopCluster;
  }

  @GET
  @Path("manifest/delete-hadoop-cluster")
  public ConfigManifest getDeleteCluster() {
    List<StateEntry> states = new LinkedList<StateEntry>();
    Set<String> roles = new HashSet<String>();
    List<Action> actions = new ArrayList<Action>();
    // Stop Tasktrackers
    DaemonAction taskTrackerAction = new DaemonAction();
    taskTrackerAction.setDaemonName("hadoop-tasktracker");
    taskTrackerAction.setActionType("stop");
    taskTrackerAction.setRole("tasktracker");
    // Setup expected result
    List<StateEntry> expectedTasktrackerResults = new LinkedList<StateEntry>();
    expectedTasktrackerResults.add(new StateEntry(StateType.DAEMON, "hadoop-tasktracker", Status.STOPPED));
    taskTrackerAction.setExpectedResults(expectedTasktrackerResults);
    actions.add(taskTrackerAction);

    // Stop Jobtracker
    DaemonAction jobTrackerAction = new DaemonAction();
    jobTrackerAction.setDaemonName("hadoop-jobtracker");
    jobTrackerAction.setActionType("stop");
    jobTrackerAction.setRole("jobtracker");
    // Setup tasktracker stop dependencies
    List<ActionDependency> dep = new LinkedList<ActionDependency>();
    roles = new HashSet<String>();
    states = new LinkedList<StateEntry>();
    states.add(new StateEntry(StateType.DAEMON, "hadoop-jobtracker", Status.STARTED));
    roles.add("jobtracker");
    dep.add(new ActionDependency(roles, states));
    jobTrackerAction.setDependencies(dep);
    // Setup expected result
    List<StateEntry> expectedJobtrackerResults = new LinkedList<StateEntry>();
    expectedJobtrackerResults.add(new StateEntry(StateType.DAEMON, "hadoop-jobtracker", Status.STOPPED));
    jobTrackerAction.setExpectedResults(expectedJobtrackerResults);
    actions.add(jobTrackerAction);

    // Stop Datanodes
    DaemonAction datanodeAction = new DaemonAction();
    datanodeAction.setDaemonName("hadoop-datanode");
    datanodeAction.setActionType("stop");
    datanodeAction.setRole("datanode");
    // Setup datanode stop dependencies
    dep = new LinkedList<ActionDependency>();
    roles = new HashSet<String>();
    states = new LinkedList<StateEntry>();
    states.add(new StateEntry(StateType.DAEMON, "hadoop-datanode", Status.STARTED));
    roles.add("datanode");
    dep.add(new ActionDependency(roles, states));
    datanodeAction.setDependencies(dep);
    // Setup expected result
    List<StateEntry> expectedDatanodeResults = new LinkedList<StateEntry>();
    expectedDatanodeResults.add(new StateEntry(StateType.DAEMON, "hadoop-datanode", Status.STOPPED));
    datanodeAction.setExpectedResults(expectedDatanodeResults);
    actions.add(datanodeAction);

    // Stop Namenode
    DaemonAction namenodeAction = new DaemonAction();
    namenodeAction.setDaemonName("hadoop-namenode");
    namenodeAction.setActionType("stop");
    namenodeAction.setRole("namenode");
    // Setup namenode stop dependencies
    dep = new LinkedList<ActionDependency>();
    roles = new HashSet<String>();
    states = new LinkedList<StateEntry>();
    states.add(new StateEntry(StateType.DAEMON, "hadoop-namenode", Status.STARTED));
    roles.add("namenode");
    dep.add(new ActionDependency(roles, states));
    namenodeAction.setDependencies(dep);
    // Setup expected result
    List<StateEntry> expectedNamenodeResults = new LinkedList<StateEntry>();
    expectedNamenodeResults.add(new StateEntry(StateType.DAEMON, "hadoop-namenode", Status.STOPPED));
    namenodeAction.setExpectedResults(expectedNamenodeResults);
    actions.add(namenodeAction);
    ConfigManifest cm = new ConfigManifest();
    cm.setActions(actions);
    return cm;
  }
  
  @POST
  @Path("blueprint/{name}")
  public Response createBlueprint(@Context UriInfo uri, 
      @PathParam("name") String name, ConfigManifest blueprint) {
    Response res;
    try {
      ZooKeeper zk = Controller.getInstance().getZKInstance();
      blueprint.setUrl(uri.getAbsolutePath().toURL());
      byte[] data = JAXBUtil.write(blueprint);
      Stat stat = zk.exists(CommonConfigurationKeys.ZOOKEEPER_CONFIG_BLUEPRINT_PATH_DEFAULT+'/'+name, false);
      if(stat==null) {
        zk.create(CommonConfigurationKeys.ZOOKEEPER_CONFIG_BLUEPRINT_PATH_DEFAULT+'/'+name, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      } else {
        throw new WebApplicationException(409);
      }
      res = Response.created(uri.getAbsolutePath()).build();
      return res;
    } catch(WebApplicationException e) {
      throw e;
    } catch(Exception e) {
      LOG.error(ExceptionUtil.getStackTrace(e));
      throw new WebApplicationException(500);
    }
  }
  
  @PUT
  @Path("blueprint/{name}")
  public Response updateBlueprint(@Context UriInfo uri, 
      @PathParam("name") String oldName, ConfigManifest blueprint) {
    Response res;
    try {
      ZooKeeper zk = Controller.getInstance().getZKInstance();
      String newName = oldName;
      if(blueprint.getUrl()!=null) {
        newName = ZookeeperUtil.getBaseURL(blueprint.getUrl().toString());
      } else {
        blueprint.setUrl(uri.getAbsolutePath().toURL());
      }
      byte[] data = JAXBUtil.write(blueprint);
      Stat stat = zk.exists(CommonConfigurationKeys.ZOOKEEPER_CONFIG_BLUEPRINT_PATH_DEFAULT+'/'+newName, false);
      if(stat!=null && oldName.equals(newName)) {
        // Update existing blueprint
        String path = CommonConfigurationKeys.ZOOKEEPER_CONFIG_BLUEPRINT_PATH_DEFAULT+'/'+oldName; 
        zk.delete(path, stat.getVersion());
        zk.create(CommonConfigurationKeys.ZOOKEEPER_CONFIG_BLUEPRINT_PATH_DEFAULT+'/'+newName, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      } else if(stat!=null) {
        // Conflict in name change
        throw new WebApplicationException(409);
      } else {
        // Create new blueprint
        try {
          zk.create(CommonConfigurationKeys.ZOOKEEPER_CONFIG_BLUEPRINT_PATH_DEFAULT+'/'+newName, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch(KeeperException.NodeExistsException e) {
          throw new WebApplicationException(409);
        }
      }
      res = Response.noContent().build();
      return res;
    } catch(WebApplicationException e) {
      throw e;
    } catch(Exception e) {
      LOG.error(ExceptionUtil.getStackTrace(e));
      throw new WebApplicationException(500);
    }
  }
  
  @GET
  @Path("blueprint/{name}")
  public ConfigManifest getBlueprint(@PathParam("name") String name) {
    try {
      ZooKeeper zk = Controller.getInstance().getZKInstance();
      Stat current = new Stat();
      String path = ZookeeperUtil.getConfigManifestPath(name);
      byte[] data = zk.getData(path, false, current);
      ConfigManifest res = JAXBUtil.read(data, ConfigManifest.class);
      return res;
    } catch(Exception e) {
      LOG.error(ExceptionUtil.getStackTrace(e));
      throw new WebApplicationException(500);      
    }
  }
  
  @GET
  @Path("blueprint")
  public List<ConfigManifest> getList() {
    List<ConfigManifest> list = new ArrayList<ConfigManifest>();
    try {
      ZooKeeper zk = Controller.getInstance().getZKInstance();
      List<String> blueprints = zk.getChildren(CommonConfigurationKeys.ZOOKEEPER_CONFIG_BLUEPRINT_PATH_DEFAULT, false);
      Stat current = new Stat();
      for(String blueprint : blueprints) {
        byte[] data = zk.getData(ZookeeperUtil.getConfigManifestPath(blueprint), false, current);
        ConfigManifest x = JAXBUtil.read(data, ConfigManifest.class);
        list.add(x);
      }
    } catch(Exception e) {
      LOG.error(ExceptionUtil.getStackTrace(e));
      throw new WebApplicationException(500);
    }
    return list;
  }
  
  @DELETE
  @Path("blueprint/{name}")
  public Response deleteStack(@PathParam("name") String name) {
    ZooKeeper zk = Controller.getInstance().getZKInstance();
    try {
      String path = ZookeeperUtil.getConfigManifestPath(name);
      Stat current = zk.exists(path, false);
      zk.delete(path, current.getVersion());
    } catch(Exception e) {
      LOG.error(ExceptionUtil.getStackTrace(e));
      throw new WebApplicationException(500);      
    }
    Response res = Response.noContent().build();
    return res;
  }
}
