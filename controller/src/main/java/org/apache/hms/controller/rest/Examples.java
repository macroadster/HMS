package org.apache.hms.controller.rest;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.hms.controller.Controller;

public class Examples {
  public static final ConfigManifest createHadoopCluster = new ConfigManifest();
  public static final ConfigManifest deleteHadoopCluster = new ConfigManifest();
  private static Log LOG = LogFactory.getLog(Examples.class);
  public static String HOSTNAME;
  public static String DEFAULT_URL;
  
  static {
    initDefaultURL();
    initCreateHadoopCluster();
    initDeleteHadoopCluster();
  }
  
  private static void initDefaultURL() {
    InetAddress addr;
    try {
      addr = InetAddress.getLocalHost();
      HOSTNAME = addr.getHostName();
    } catch (UnknownHostException e) {
      HOSTNAME = "localhost";
    }
    StringBuilder buffer = new StringBuilder();
    buffer.append("http://");
    buffer.append(HOSTNAME);
    buffer.append(":");
    buffer.append(Controller.CONTROLLER_PORT);
    buffer.append("/");
    buffer.append(Controller.CONTROLLER_PREFIX);
    DEFAULT_URL = buffer.toString();
  }
  
  private static void initCreateHadoopCluster() {
    List<Action> actions = new ArrayList<Action>();

    // Install software
    PackageAction install = new PackageAction();
    install.setActionType("install");
    actions.add(install);
    List<StateEntry> expectedInstallResults = new LinkedList<StateEntry>();
    expectedInstallResults.add(new StateEntry(StateType.PACKAGE, "hadoop", Status.INSTALLED));
    install.setExpectedResults(expectedInstallResults);
    
    // Generate Hadoop configuration
    ScriptAction setupConfig = new ScriptAction();
    setupConfig.setScript("/usr/sbin/hadoop-setup-conf.sh");
    int i=0;
    String[] parameters = new String[14];
    parameters[i++] = "--namenode-host=${namenode}";
    parameters[i++] = "--jobtracker-host=${jobtracker}";
    parameters[i++] = "--secondarynamenode-host=${secondary-namenode}";
    parameters[i++] = "--datanodes=${datanode}";
    parameters[i++] = "--tasktrackers=${tasktracker}";
    parameters[i++] = "--conf-dir=/etc/hadoop";
    parameters[i++] = "--hdfs-dir=/var/lib/hdfs";
    parameters[i++] = "--namenode-dir=/var/lib/hdfs/namenode";
    parameters[i++] = "--mapred-dir=/var/lib/mapreduce";
    parameters[i++] = "--datanode-dir=/var/lib/hdfs/data";
    parameters[i++] = "--log-dir=/var/log/hadoop";
    parameters[i++] = "--hdfs-user=hdfs";
    parameters[i++] = "--mapreduce-user=mapred";
    parameters[i++] = "--auto";
    setupConfig.setParameters(parameters);
    List<StateEntry> expectedConfigResults = new LinkedList<StateEntry>();
    expectedConfigResults.add(new StateEntry(StateType.PACKAGE, "hadoop-config", Status.INSTALLED));
    setupConfig.setExpectedResults(expectedConfigResults);
    actions.add(setupConfig);
    
    // Format HDFS
    ScriptAction setupHdfs = new ScriptAction();
    setupHdfs.setRole("namenode");
    setupHdfs.setScript("/usr/sbin/hadoop-setup-hdfs.sh");
    String[] hdfsParameters = new String[3];
    hdfsParameters[0]="--format";
    hdfsParameters[1]="--hdfs-user=hdfs";
    hdfsParameters[2]="--mapreduce-user=mapred";
    setupHdfs.setParameters(hdfsParameters);
    // Setup dependencies       
    List<ActionDependency> dep = new LinkedList<ActionDependency>();
    Set<String> roles = new HashSet<String>();
    List<StateEntry> states = new LinkedList<StateEntry>();
    states.add(new StateEntry(StateType.PACKAGE, "hadoop-config", Status.INSTALLED));
    roles.add("namenode");
    roles.add("datanode");
    roles.add("jobtracker");
    roles.add("tasktracker");
    dep.add(new ActionDependency(roles, states));
    setupHdfs.setDependencies(dep);
    // Setup expected result
    List<StateEntry> expectedFormatResults = new LinkedList<StateEntry>();
    expectedFormatResults.add(new StateEntry(StateType.DAEMON, "hadoop-namenode", Status.STARTED));
    setupHdfs.setExpectedResults(expectedFormatResults);
    actions.add(setupHdfs);
    
    // Start Datanodes
    DaemonAction dataNodeAction = new DaemonAction();
    dataNodeAction.setDaemonName("hadoop-datanode");
    dataNodeAction.setActionType("start");
    dataNodeAction.setRole("datanode");
    // Setup namenode started dependencies
    dep = new LinkedList<ActionDependency>();
    roles = new HashSet<String>();
    states = new LinkedList<StateEntry>();
    states.add(new StateEntry(StateType.DAEMON, "hadoop-namenode", Status.STARTED));
    roles.add("namenode");
    dep.add(new ActionDependency(roles, states));
    dataNodeAction.setDependencies(dep);
    // Setup expected result
    List<StateEntry> expectedDatanodeResults = new LinkedList<StateEntry>();
    expectedDatanodeResults.add(new StateEntry(StateType.DAEMON, "hadoop-datanode", Status.STARTED));
    dataNodeAction.setExpectedResults(expectedDatanodeResults);
    actions.add(dataNodeAction);
  
    // Start Jobtracker
    DaemonAction jobTrackerAction = new DaemonAction();
    jobTrackerAction.setDaemonName("hadoop-jobtracker");
    jobTrackerAction.setActionType("start");
    jobTrackerAction.setRole("jobtracker");
    // Setup datanode started dependencies
    dep = new LinkedList<ActionDependency>();
    roles = new HashSet<String>();
    states = new LinkedList<StateEntry>();
    states.add(new StateEntry(StateType.DAEMON, "hadoop-datanode", Status.STARTED));
    roles.add("datanode");
    dep.add(new ActionDependency(roles, states));
    jobTrackerAction.setDependencies(dep);
    // Setup expected result
    List<StateEntry> expectedJobtrackerResults = new LinkedList<StateEntry>();
    expectedJobtrackerResults.add(new StateEntry(StateType.DAEMON, "hadoop-jobtracker", Status.STARTED));
    jobTrackerAction.setExpectedResults(expectedJobtrackerResults);
    actions.add(jobTrackerAction);
    
    // Start Tasktrackers
    DaemonAction taskTrackerAction = new DaemonAction();
    taskTrackerAction.setDaemonName("hadoop-tasktracker");
    taskTrackerAction.setActionType("start");
    taskTrackerAction.setRole("tasktracker");
    // Setup tasktracker started dependencies
    dep = new LinkedList<ActionDependency>();
    roles = new HashSet<String>();
    states = new LinkedList<StateEntry>();
    states.add(new StateEntry(StateType.DAEMON, "hadoop-jobtracker", Status.STARTED));
    roles.add("jobtracker");
    dep.add(new ActionDependency(roles, states));
    taskTrackerAction.setDependencies(dep);
    // Setup expected result
    List<StateEntry> expectedTasktrackerResults = new LinkedList<StateEntry>();
    expectedTasktrackerResults.add(new StateEntry(StateType.DAEMON, "hadoop-tasktracker", Status.STARTED));
    taskTrackerAction.setExpectedResults(expectedTasktrackerResults);
    actions.add(taskTrackerAction);
    
    createHadoopCluster.setActions(actions);
    URL url;
    try {
      url = new URL(DEFAULT_URL+"/config/blueprint/create-hadoop-1.0-cluster");
      createHadoopCluster.setUrl(url);
    } catch (MalformedURLException e) {
      LOG.error(ExceptionUtil.getStackTrace(e));
    }
  }
  
  private static void initDeleteHadoopCluster() {
    List<Action> actions = new ArrayList<Action>();
    ScriptAction nuke = new ScriptAction();
    nuke.setScript("ps");
    int i=0;

    DaemonAction nameNodeAction = new DaemonAction();
    nameNodeAction.setDaemonName("hadoop-namenode");
    nameNodeAction.setActionType("stop");
    nameNodeAction.setRole("namenode");
    actions.add(nameNodeAction);

    DaemonAction dataNodeAction = new DaemonAction();
    dataNodeAction.setDaemonName("hadoop-datanode");
    dataNodeAction.setActionType("stop");
    dataNodeAction.setRole("datanode");
    actions.add(dataNodeAction);

    DaemonAction jobTrackerAction = new DaemonAction();
    jobTrackerAction.setDaemonName("hadoop-jobtracker");
    jobTrackerAction.setActionType("stop");
    jobTrackerAction.setRole("jobtracker");
    actions.add(jobTrackerAction);

    DaemonAction taskTrackerAction = new DaemonAction();
    taskTrackerAction.setDaemonName("hadoop-tasktracker");
    taskTrackerAction.setActionType("stop");
    taskTrackerAction.setRole("tasktracker");
    actions.add(taskTrackerAction);

    ScriptAction nuke2 = new ScriptAction();
    nuke2.setScript("killall");
    i=0;
    String[] jsvcParameters = new String[3];
    jsvcParameters[i++] = "-9";
    jsvcParameters[i++] = "jsvc";
    jsvcParameters[i++] = "|| exit 0";
    nuke2.setParameters(jsvcParameters);
    nuke2.setRole("datanode");
    actions.add(nuke2);

    ScriptAction nukePackages = new ScriptAction();
    nukePackages.setScript("rpm");
    i=0;
    String[] packagesParameters = new String[8];
    packagesParameters[i++] = "-e";
    packagesParameters[i++] = "hadoop";
    packagesParameters[i++] = "||";
    packagesParameters[i++] = "rpm";
    packagesParameters[i++] = "-e";
    packagesParameters[i++] = "hadoop-mapreduce";
    packagesParameters[i++] = "hadoop-hdfs";
    packagesParameters[i++] = "hadoop-common || rm -rf /home/hms/apps/*";
    nukePackages.setParameters(packagesParameters);
    actions.add(nukePackages);
    ScriptAction scrub = new ScriptAction();
    scrub.setScript("rm");
    String[] scrubParameters = new String[3];
    scrubParameters[0] = "-rf";
    scrubParameters[1] = "/var/lib/hdfs";
    scrubParameters[2] = "/var/lib/mapreduce";
    scrub.setParameters(scrubParameters);
    actions.add(scrub);
    
    deleteHadoopCluster.setActions(actions);
    URL url;
    try {
      url = new URL(DEFAULT_URL+"/config/blueprint/delete-hadoop-1.0-cluster");
      deleteHadoopCluster.setUrl(url);
    } catch (MalformedURLException e) {
      LOG.error(ExceptionUtil.getStackTrace(e));
    }
  }
}
