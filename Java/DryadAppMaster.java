/*
Copyright (c) Microsoft Corporation

All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
compliance with the License.  You may obtain a copy of the License 
at http://www.apache.org/licenses/LICENSE-2.0   


THIS CODE IS PROVIDED *AS IS* BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, EITHER 
EXPRESS OR IMPLIED, INCLUDING WITHOUT LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF 
TITLE, FITNESS FOR A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.  


See the Apache Version 2.0 License for specific language governing permissions and 
limitations under the License. 

*/

package com.microsoft.research;

//import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import java.io.File;
import java.io.IOException;
import java.lang.StringBuilder;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.logging.Log; 
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.security.client.ClientRMSecurityInfo;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.ProtoUtils;
import org.apache.hadoop.yarn.util.Records; 

public class DryadAppMaster 
{
    private Log log;
    private YarnConfiguration yarnConf;
    private YarnRPC rpc;
    private AMRMProtocol resourceManager;
    private ApplicationAttemptId appAttemptID;
    private String appMasterHostname;
    public final String xcResources;
    public final String vertexCmdLine;
    public final String jniClassPath;
    public final String dryadHome;
    private ScheduledExecutorService heartbeatExec;
    private ScheduledFuture<?> heartbeatHandle;
    private AtomicBoolean shuttingDown;
    private AtomicBoolean scheduleProcesses;
    private AtomicInteger responseId;
    private AtomicInteger nextVertexId;
    private Map<ContainerId, VertexInfo> runningContainers;
    private List<ContainerId> containersToReturn;
    private List<ResourceRequest> resourceRequests;
    private int clusterNodeCount = -1;
    private final int minMemory;
    private final int maxMemory;

    private final int minNodes;
    private final int maxNodes;

    private static int YTS_NA = 0;
    private static int YTS_Scheduling = 1;
    private static int YTS_Running = 2;
    private static int YTS_Completed = 3;
    private static int YTS_Failed = 4;

    private native void SendVertexState(int vertexId, int state, String nodeName); 

    private class VertexInfo 
    {
	public final int vertexId;
	public final String nodeName;

	public VertexInfo(int vid, String node) {
	    vertexId = vid;
	    nodeName = node;
	}
    }

    static {
	Log slog = LogFactory.getLog("DryadAppMaster");
	slog.info("About to load DryadYarnBridge library");
	System.loadLibrary("DryadYarnBridge");
	slog.info("Loaded DryadYarnBridge library");
    }

    public DryadAppMaster() throws YarnRemoteException, IOException
    {
	log = LogFactory.getLog("DryadAppMaster");
	log.info("In DryadAppMaster constructor");
	shuttingDown = new AtomicBoolean(false);
	scheduleProcesses = new AtomicBoolean(true);
	responseId = new AtomicInteger();
	nextVertexId = new AtomicInteger(2); //first vertex id is 2 to map to Dryad Vertex Scheduler
	runningContainers =  new HashMap<ContainerId, VertexInfo>();

	containersToReturn = Collections.synchronizedList(new ArrayList<ContainerId>());
	resourceRequests = Collections.synchronizedList(new ArrayList<ResourceRequest>());

	Map<String, String> envs = System.getenv();
	String containerIdString = envs.get(Environment.CONTAINER_ID.name());
	    
	if (containerIdString == null) {
	    // container id should always be set in the env by the framework 
	    StringBuilder sb = new StringBuilder(4096);
	    for(Map.Entry<String, String> entry : envs.entrySet())
	    {
		sb.append("\n\tKey: '");
		sb.append(entry.getKey());
		sb.append("'\tValue: '");
		sb.append(entry.getValue());
		sb.append("'");
	    }
	    
	    log.error("Couldn't find container id in environment strings.  Environment: " + sb); 
	    throw new IllegalArgumentException("ContainerId not set in the environment");
	}
	appMasterHostname = envs.get("COMPUTERNAME");  // WINDOWS ONLY
	if (appMasterHostname == null) {
	    throw new IllegalArgumentException(
					       "COMPUTERNAME not set in the environment");
	}
	xcResources = envs.get("XC_RESOURCEFILES");
	jniClassPath = envs.get("JNI_CLASSPATH");
	dryadHome = envs.get("DRYAD_HOME");

	ContainerId containerId = ConverterUtils.toContainerId(containerIdString);
	appAttemptID = containerId.getApplicationAttemptId();
	
	minNodes = Integer.parseInt(envs.get("MINIMUM_COMPUTE_NODES"));
	maxNodes = Integer.parseInt(envs.get("MAXIMUM_COMPUTE_NODES"));

	File vertexExecutable = new File(envs.get("DRYAD_HOME"), "DryadVertexService.exe");
	vertexCmdLine = vertexExecutable.getAbsolutePath();
	
	yarnConf = new YarnConfiguration();
	String dest = yarnConf.get(YarnConfiguration.RM_SCHEDULER_ADDRESS,YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS);
	log.warn("Configuration says to connect to ResourceManager at " + dest);
	// Connect to the Scheduler of the ResourceManager. 
	InetSocketAddress rmAddress = NetUtils.createSocketAddr(dest);           
	log.info("Connecting to ResourceManager at " + rmAddress);

	rpc = YarnRPC.create(yarnConf);
	resourceManager = (AMRMProtocol) rpc.getProxy(AMRMProtocol.class, rmAddress, (Configuration)yarnConf);

	heartbeatExec = Executors.newScheduledThreadPool(1);

	String historyUrl = "http://localhost/foo"; // NYI JobHistoryUtils.getHistoryUrl((Configuration)yarnConf, 
	    //				  appAttemptID.getApplicationId());
	log.info("History url is " + historyUrl);

	RegisterApplicationMasterRequest appMasterRequest = 
	    Records.newRecord(RegisterApplicationMasterRequest.class);
	appMasterRequest.setApplicationAttemptId(appAttemptID);     
	appMasterRequest.setHost(appMasterHostname);
	// NYI - for now, until we learn that these are necessary, use dummy values for URL and rpc port
	appMasterRequest.setRpcPort(0);
	appMasterRequest.setTrackingUrl(historyUrl);
	log.info("Registering AppMaster");
	RegisterApplicationMasterResponse response = 
	    resourceManager.registerApplicationMaster(appMasterRequest);
	log.info("AppMaster registered");
	minMemory = response.getMinimumResourceCapability().getMemory();
	maxMemory = response.getMaximumResourceCapability().getMemory();
	
	// setup the heartbeat to the RM
	Runnable heartbeatObj = new Runnable() {
	    public void run() { heartbeat(); }
	};

	long hbInterval = yarnConf.getInt(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, 
					  YarnConfiguration.DEFAULT_RM_AM_EXPIRY_INTERVAL_MS);
	// For now, just heartbeat every  second, so we learn about failures
	hbInterval = 1000; //(hbInterval * 3) / 4; 
	log.info("Sending heartbeats to the RM every " + hbInterval + " ms.");

	// send the first heartbeat immediately, so we learn how many nodes are in the cluster
	heartbeatHandle = heartbeatExec.scheduleAtFixedRate(heartbeatObj, 0, hbInterval, TimeUnit.MILLISECONDS);
    }

    private float getProgress()
    {
	return 0.01f; // NYI
    }

    private void heartbeat()
    {
	// check to see if we should cancel the heartbeat
	if (shuttingDown.get()) {
	    log.info("Cancelling heartbeat"); 
	    heartbeatHandle.cancel(true);
	} else {
	    log.info("Sending heartbeat to the RM"); 
	    AllocateResponse response = sendAllocateRequest();
	    if (response != null) {
		int oldNodeCount = clusterNodeCount;
		clusterNodeCount = response.getNumClusterNodes();
		if (clusterNodeCount != oldNodeCount) {
		    log.info("There are now " + clusterNodeCount + " available nodes on the cluster.");
		}
		processResponse(response);
	    }
	}
    }
    
    private void launchContainer(Container container, ContainerManager cm)
    {
	ContainerLaunchContext ctx = 
	    Records.newRecord(ContainerLaunchContext.class);

	VertexInfo vi = new VertexInfo(nextVertexId.getAndIncrement(), 
				       container.getNodeId().getHost());

	// set the environment variable to enable vertex debugging if desired
	// also set the CCP_DRYADPROCID and XC_JOBMANAGER variables so the 
	// vertex knows how to find the GM and knows what its id is
	Map<String, String> vertexEnv = new HashMap<String, String>();
	//vertexEnv.put("HPCQUERY_DEBUGVERTEXHOST", "HPCQUERY_DEBUGVERTEXHOST");
	//vertexEnv.put("CCP_SCHEDULERTYPE", "LOCAL");
	//vertexEnv.put("HPCQUERY_DEBUGVERTEXHOST", "DEBUG");
	vertexEnv.put("XCJOBMANAGER", appMasterHostname); 
	vertexEnv.put("CCP_JOBID", appAttemptID.getApplicationId().getId() + "");
	vertexEnv.put("CCP_TASKID", container.getId().getId() + "");
	vertexEnv.put("XC_RESOURCEFILES", xcResources);
	
	vertexEnv.put("JNI_CLASSPATH", jniClassPath);
	vertexEnv.put("DRYAD_HOME", dryadHome);
	ctx.setEnvironment(vertexEnv);

	String commandLine = vertexCmdLine
	    + " 1>stdout-fromcm.txt"
	    + " 2>stderr-fromcm.txt";
	log.info("Launching a container with command line '" +  
		 vertexCmdLine + "'" + " for vertex " + vi.vertexId +
		 " on host " + vi.nodeName);

	List<String> commands = new ArrayList<String>();
	commands.add(commandLine);
	ctx.setCommands(commands);

	runningContainers.put(container.getId(), vi);

	//SendVertexState(command.vertexId, DPS_Starting); // no need to send this when starting task

	// Send the start request to the ContainerManager
	StartContainerRequest startReq = Records.newRecord(StartContainerRequest.class);
	startReq.setContainerLaunchContext(ctx);
	startReq.setContainer(container);
	try {
	    cm.startContainer(startReq);	   
	} catch (YarnRemoteException|IOException e) {
	    log.info("Error launching the container: " + e.getMessage());
	}
	try {
	    GetContainerStatusRequest conStatusReq = Records.newRecord(GetContainerStatusRequest.class);
	    conStatusReq.setContainerId(container.getId());
	    ContainerStatus status = cm.getContainerStatus(conStatusReq).getStatus();
	    log.info("Container " + status.getContainerId() + " is in the " +  status.getState() + " state");
	    if (status.getState() == ContainerState.RUNNING) {
		log.debug("Calling SendVertexState()");
		SendVertexState(vi.vertexId, YTS_Running, vi.nodeName);
 		log.debug("Returned from SendVertexState()");
	    } else {
		log.warn("May not send running state");
	    }
	} catch (YarnRemoteException|IOException e) {
	    log.info("Error getting container state: " + e.getMessage());
	}
    }

    private void processResponse(AllocateResponse response) 
    {
	// is this the first allocation?	
	if (scheduleProcesses.compareAndSet(true, false)) {
	    //don't schedule a process where the graph manager is running
	    int numProcessesToStart = Math.max(response.getNumClusterNodes() - 1, maxNodes);
	    log.info("There are " + response.getNumClusterNodes() + " nodes in the cluster. maxNodes = " + maxNodes);
	    scheduleProcess(numProcessesToStart);
	}
	
	boolean shouldReboot = response.getReboot();
	List<Container> newContainers = response.getAllocatedContainers();
	List<ContainerStatus> finishedContainers = response.getCompletedContainersStatuses(); 
	List<NodeReport> updatedNodes = response.getUpdatedNodes();
	int returnedResponseId = response.getResponseId(); // TODO - how should this be tracked?
	log.info(String.format("Response id %d reboot %b containing %d new containers, %d finished containers, and %d updated nodes", 
			       returnedResponseId, shouldReboot, newContainers.size(), 
			       finishedContainers.size(), updatedNodes.size()));

	for (ContainerStatus containerStatus : finishedContainers) {                               
	    ContainerId cid = containerStatus.getContainerId();
	    log.info("Got container status for containerID= " 
		     + cid + ", state=" + containerStatus.getState()     
		     + ", exitStatus=" + containerStatus.getExitStatus() 
		     + ", diagnostics=" + containerStatus.getDiagnostics());
	    
	    // Need to notify graph manager of current state
	    VertexInfo vi = runningContainers.remove(cid);
	    //only send events up the stack when we are not shutting down
	    if (vi != null && !shuttingDown.get()) {  
		int containerState = 0;
		if (containerStatus.getState() == ContainerState.COMPLETE) {
		    if (containerStatus.getExitStatus() == 0) {
			containerState = YTS_Completed;
		    } else {
			containerState = YTS_Failed;			
		    } 
		    SendVertexState(vi.vertexId, containerState, vi.nodeName);
		} else {
		    log.error("Container finished without a COMPLETE status. containerID=" + cid);
		}
	    }
	}
	    
	startContainers(newContainers);
    }

    public void scheduleProcess(int vertexId, String name, String commandLine)
    {
	log.info(String.format("scheduleProcess called (external) for vertex %1$d name: '%2$s' commandLine: '%3$s'",  
			       vertexId, name, commandLine));
	
    }

    public void scheduleProcess(int numProcesses) 
    {
	log.info("Scheduling " + numProcesses + " processes.");

	ResourceRequest resourceRequest =  Records.newRecord(ResourceRequest.class);

	resourceRequest.setHostName("*");
	
	Resource capability = Records.newRecord(Resource.class);
	capability.setMemory(maxMemory);
	resourceRequest.setCapability(capability);
	Priority priority = Records.newRecord(Priority.class);
	priority.setPriority(1);
	resourceRequest.setPriority(priority);

	resourceRequest.setNumContainers(numProcesses);
	synchronized(resourceRequests) {
	    resourceRequests.add(resourceRequest);
	}
    }

    private AllocateResponse sendAllocateRequest() 
    {
	AllocateRequest request = Records.newRecord(AllocateRequest.class);
	int idToSend = responseId.getAndIncrement();
	request.setResponseId(idToSend);
	request.setProgress(getProgress());
	request.setApplicationAttemptId(appAttemptID);
	int numReleases = 0;
	
	List<ContainerId> localContainersToReturn = new ArrayList<ContainerId>();
	synchronized(containersToReturn) {
	    if (containersToReturn.size() > 0) {
		numReleases = containersToReturn.size();
		localContainersToReturn.addAll(containersToReturn);
		request.setReleaseList(localContainersToReturn);
		containersToReturn.clear();
	    }
	}

	synchronized (resourceRequests) {
	    if (resourceRequests.size() > 0) {
		request.setAskList(resourceRequests);
	    }
	    log.info("Sending request to RM requesting " + resourceRequests.size() 
		     + " nodes and releasing " + numReleases + " nodes."); 

	    AllocateResponse response = null;
	    try {
		response = resourceManager.allocate(request);
		resourceRequests.clear();
		log.info("Received reponse from RM - " + response.getNumClusterNodes() + 
			 " nodes available in cluster");	
		return response;
	    } catch (YarnRemoteException|IOException e) {
		log.error("Error communicating with RM: " + e.getMessage() , e);
		// TODO - retry communication
		return null;
	    } 
	}
    }

    public void shutdown(boolean immediateShutdown, boolean success)
    { 
	shuttingDown.set(true);
	heartbeatHandle.cancel(immediateShutdown); // if we are shutting down, we can just interrupt the running thread, if necessary
	log.info("Shutdown heartbeats to RM");

	// send the shutdown message to the RM
	FinishApplicationMasterRequest request = Records.newRecord(FinishApplicationMasterRequest.class);
	request.setAppAttemptId(appAttemptID);
	if (success) {
	    request.setFinishApplicationStatus(FinalApplicationStatus.SUCCEEDED);  
	} else {
	    request.setFinishApplicationStatus(FinalApplicationStatus.FAILED);
	}
	try {
	    //response is currently an empty class
	    FinishApplicationMasterResponse response = resourceManager. finishApplicationMaster(request);  
	} catch (YarnRemoteException|IOException e) {
	    log.error("Error communicating with RM: " + e.getMessage() , e);
	}
	log.info("FinishApplicationMasterRequest sent");
    }

    private void startContainers(List<Container> newContainers) 
    {
	// DCF TODO: Cache the connections to the cm
	for (final Container container : newContainers) { 
	    // Connect to ContainerManager on the allocated container 
	    String cmIpPortStr = container.getNodeId().getHost() + ":" 
		+ container.getNodeId().getPort();              
	    final InetSocketAddress cmAddress = NetUtils.createSocketAddr(cmIpPortStr);               
	    log.debug("The allocated container contains a resource memory capactity of " + 
		      container.getResource().getMemory());
	    log.debug("The allocated container contains a container ID of " + container.getId());

	    // UGI example from DistributedShell AM
	    UserGroupInformation ugi =
		UserGroupInformation.createRemoteUser(container.getId().toString());
	    Token<ContainerTokenIdentifier> token =
		ProtoUtils.convertFromProtoFormat(container.getContainerToken(),
						  cmAddress);
	    ugi.addToken(token);
	    ContainerManager cm = ugi.doAs(new PrivilegedAction<ContainerManager>() {
	        @Override
		public ContainerManager run() {
		    return ((ContainerManager) rpc.getProxy(ContainerManager.class,
							    cmAddress, yarnConf));
		}
	    });
	    launchContainer(container, cm);
	}
	

    } 
}
