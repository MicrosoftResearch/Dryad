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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.Integer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath; 
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.apache.commons.logging.Log; 
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.client.ClientRMSecurityInfo;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class DryadLinqYarnApp 
{


    public static void main( String[] args ) throws YarnRemoteException, InterruptedException, 
						    IOException, URISyntaxException,
						    ParserConfigurationException, SAXException,
						    XPathExpressionException
    {
	Log log = LogFactory.getLog("DryadLinqYarnClient");
	if (args.length != 1)
	{
	    log.error("Incorrect number of arguments.");
	    System.exit(1);
	}
	// the queryplan xml file is in args[0]
	log.info("Reading query plan from file " + args[0]);
	DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
	Document queryPlan = builder.parse(new File(args[0]));
	
	XPath xpath = XPathFactory.newInstance().newXPath();

	XPathExpression nameExpr = xpath.compile("/Query/ClusterName");
	//TODO - the cluster name might not be useful
	String clusterName = (String) nameExpr.evaluate(queryPlan, XPathConstants.STRING); 

	XPathExpression resExpr = xpath.compile("/Query/Resources/Resource");
	NodeList resourceList = (NodeList) resExpr.evaluate(queryPlan, XPathConstants.NODESET);

	String[] localResourcePaths = new String[resourceList.getLength() + 1];
	for (int i = 0; i < resourceList.getLength(); i++)
	{
	    localResourcePaths[i] = resourceList.item(i).getTextContent();
	    //System.out.println(localResourcePaths[i]);
	} 
	XPathExpression appNameExpr = xpath.compile("/Query/QueryName");
	String queryName = (String) appNameExpr.evaluate(queryPlan, XPathConstants.STRING); 

	XPathExpression minNodesExpr = xpath.compile("/Query/MinimumComputeNodes");
	int minComputeNodes = Integer.parseInt((String) minNodesExpr.evaluate(queryPlan, XPathConstants.STRING)); 

	XPathExpression maxNodesExpr = xpath.compile("/Query/MaximumComputeNodes");
	int maxComputeNodes = Integer.parseInt((String) maxNodesExpr.evaluate(queryPlan, XPathConstants.STRING)); 
	

	File queryPlanFile =  new File(args[0]);
	String queryPlanLeafName = queryPlanFile.getName();

	File[] srcDirs = new File[2];
	srcDirs[0] = queryPlanFile.getParentFile();
	srcDirs[1] = new File(System.getProperty("user.dir"));

	// add the query plan to the resources
	localResourcePaths[localResourcePaths.length - 1] = queryPlanLeafName;

	ClientRMProtocol applicationsManager; 
	YarnConfiguration yarnConf = new YarnConfiguration();

	String dest = yarnConf.get(YarnConfiguration.RM_ADDRESS, YarnConfiguration.DEFAULT_RM_ADDRESS);
	log.info("Connecting to dest " + dest); 
	InetSocketAddress rmAddress = NetUtils.createSocketAddr(dest);
	YarnRPC rpc = YarnRPC.create(yarnConf);
	applicationsManager = ((ClientRMProtocol) rpc.getProxy(
	    ClientRMProtocol.class, rmAddress, (Configuration)yarnConf));    

	if (maxComputeNodes == -1) {
	    // find the max number of nodes in the cluster
	    GetClusterMetricsResponse metricsResponse = applicationsManager.getClusterMetrics(
				       Records.newRecord(GetClusterMetricsRequest.class));	
	    maxComputeNodes = metricsResponse.getClusterMetrics().getNumNodeManagers();
	    log.info("Set maxComputeNodes to " + maxComputeNodes);
	}

	GetNewApplicationRequest request = 
	    Records.newRecord(GetNewApplicationRequest.class);              
	GetNewApplicationResponse response = 
	    applicationsManager.getNewApplication(request);

	ApplicationId appId = response.getApplicationId();
	log.info("Got new ApplicationId=" + appId);
	log.info("Min Resource Capability: " + response.getMinimumResourceCapability().getMemory()); 
	log.info("Max Resource Capability: " + response.getMaximumResourceCapability().getMemory()); 

	Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
	
	// copy the files to hdfs under a job directory
	// and add them to local resources
	// TODO: Use content based hashing to avoid copying files
	FileSystem fs = FileSystem.get(yarnConf);
	Path homeDir = fs.getHomeDirectory();

	Path resourceHdfsDir = new Path(homeDir, "dlbin/" + appId);
	FsPermission execPerm = new FsPermission("755");
	fs.mkdirs(resourceHdfsDir, execPerm);
	StringBuilder resourceString = new StringBuilder();
	resourceString.append(resourceHdfsDir);

	for(int i = 0; i < localResourcePaths.length; i++)
	{
	    boolean sourceFound = false;
	    File resourceFile = new File(localResourcePaths[i]);
	    resourceString.append(',');
	    resourceString.append(localResourcePaths[i]);
	    if (!resourceFile.exists())
	    {
		for(int j = 0; j < srcDirs.length && !sourceFound; j++)
		{
		    resourceFile = new File(srcDirs[j], localResourcePaths[i]);
		    if (resourceFile.exists())
		    {
			sourceFound = true;
		    }
		}
	    } 
	    else 
	    {
		sourceFound = true;
	    }
	    if (!sourceFound)
	    {
		throw new FileNotFoundException("Unable to find local resource: " + localResourcePaths[i]); 	
	    }
	    Path srcPath = new Path(resourceFile.toURI());
	    String leafName = new File(localResourcePaths[i]).getName().toLowerCase();
	    Path remotePath = new Path(resourceHdfsDir, leafName);
	    log.info("Copying file '" +  leafName + "' to '" + remotePath + "'"); 
	    fs.copyFromLocalFile(srcPath, remotePath);

	    if (leafName.endsWith(".exe") || leafName.endsWith(".dll") ||
		leafName.endsWith(".pdb")) {
		fs.setPermission(remotePath, execPerm);
	    }
	    

	    FileStatus remoteStatus = fs.getFileStatus(remotePath);
	    LocalResource amResource = Records.newRecord(LocalResource.class);
	    amResource.setType(LocalResourceType.FILE);
	    amResource.setVisibility(LocalResourceVisibility.APPLICATION);   
	    amResource.setResource(ConverterUtils.getYarnUrlFromPath(remotePath)); 
	    amResource.setTimestamp(remoteStatus.getModificationTime());
	    log.info("Set file modification time to " + 
		     remoteStatus.getModificationTime());
	    amResource.setSize(remoteStatus.getLen());
	    log.info("Set file length to " + remoteStatus.getLen());
	    localResources.put(remotePath.getName(),  amResource);    
	}
	
	int amMemory = response.getMaximumResourceCapability().getMemory(); 
	// request the min amount of memory, which should schedule the am on its own node
	//int amMemory = response.getMaximumResourceCapability().getMemory();
	log.info("Set amMemory=" + amMemory);
	log.info("Creating the ApplicationSubmissionContext");
	// Create a new ApplicationSubmissionContext
	ApplicationSubmissionContext appContext = Records.newRecord(ApplicationSubmissionContext.class);
	log.info("Setting the ApplicationId");
	// set the ApplicationId 
	appContext.setApplicationId(response.getApplicationId());
	// set the application name
	appContext.setApplicationName(queryName);
	// set the queue to the default queue
	appContext.setQueue("default");
	log.info("Getting a ContainerLaunchContext");
	// Create a new container launch context for the AM's container
	ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
	log.info("Got a ContainerLaunchContext");

	// Set the local resources into the launch context    
	amContainer.setLocalResources(localResources);
	
	// get a copy of the local environment variables
	Map<String, String> envs = System.getenv();

	// The environment for the am
	Map<String, String> env = new HashMap<String, String>();    
	env.put("CCP_JOBID", appId.getId() + "");
	env.put("CCP_DRYADPROCID", "1");
	env.put("XC_RESOURCEFILES", resourceString.toString());
	env.put("MINIMUM_COMPUTE_NODES", minComputeNodes + "");
	env.put("MAXIMUM_COMPUTE_NODES", maxComputeNodes + "");
	env.put("DRYAD_HOME", envs.get("DRYAD_HOME"));
	env.put("JNI_CLASSPATH", envs.get("JNI_CLASSPATH"));
	//log.info("DRYAD_HOME env variable is '" + envs.get("DRYAD_HOME") + "'");
	amContainer.setEnvironment(env);
	
	File jmExecutable = new File(envs.get("DRYAD_HOME"), "LinqToDryadJM_managed.exe");
	String jmCmdLine = jmExecutable.getAbsolutePath();

	// Construct the command to be executed on the launched container 
	String command = jmCmdLine  + " " + 
	    queryPlanLeafName + " " +  
	    //" --break " +
	    " 1>stdout.txt" +
	    " 2>stderr.txt";                     
	
	List<String> commands = new ArrayList<String>();
	commands.add(command);
	amContainer.setCommands(commands);

	Resource capability = Records.newRecord(Resource.class);
	capability.setMemory(amMemory);
	appContext.setResource(capability);
	appContext.setAMContainerSpec(amContainer);	

	// Create the request to send to the ApplicationsManager 
	SubmitApplicationRequest appRequest = 
	    Records.newRecord(SubmitApplicationRequest.class);
	appRequest.setApplicationSubmissionContext(appContext);
	applicationsManager.submitApplication(appRequest);

	System.out.println(appId);
     }
 }
