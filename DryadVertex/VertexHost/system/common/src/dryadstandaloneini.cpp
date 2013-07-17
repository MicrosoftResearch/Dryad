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

#include "DrCommon.h"
#include "dryadstandaloneini.h"
#include "dryadnativeport.h"
#include "dryaderrordef.h"
#include "dryadmetadata.h"

#pragma unmanaged

DryadNativePort* g_dryadNativePort;

static XDRESSIONHANDLE s_session;
static XCPROCESSHANDLE s_processHandle;

static bool g_initialized = false;

//
// Initialize the XCompute layer
//
DrError DryadInitializeXCompute(const char* netLibName, const char* iniFileName,
                              int argc, char* argv[], int* pNOpts)
{
    DrError err = DrError_OK; 
    *pNOpts = 0;

    //
    // Only initialize XCompute Once
    //
    if (g_initialized)
    {
        return DryadError_AlreadyInitialized;
    }

    g_initialized = true;

    // todo: why is this code commented / should it be removed?
/* JC
    if (iniFileName == NULL) {
        iniFileName = s_configFileName;
    }
*/    
    
    //
    // If library name not set, set it to DryadApplication.X 
    // where X = process id
    // otherwise, copy name into "component" string
    //
    char component[32];
    if (netLibName == NULL)
    {
        err = ::StringCbPrintfA(component,
                               sizeof(component),
                               "DryadApplication.%u",
                               ::GetCurrentProcessId());
        LogAssert(SUCCEEDED(err));
        netLibName = component;
    }
    else
    {
        strcpy(component, netLibName);
    }

    //
    // Initialize Xcompute, providing this semi-unique 
    // component name and any config file specified
    //
    err = XcInitialize(iniFileName, component);
    if (err != DrError_OK)
    {
        //
        // If initialization fails, report and exit
        //
        DrLogE( "DryadConfigurationManager - XcInitialize failed, error=%s",
            DRERRORSTRING(err));
        goto exit;
    }
    
    //
    // Open an Xcompute session, using default session settings
    //
    err = XcOpenSession(NULL, &s_session, NULL);
    if (err != DrError_OK)
    {
        //
        // If opening session failed, report and exit
        //
        DrLogE( "DryadConfigurationManager - XcOpenSession failed, error=%s",
            DRERRORSTRING(err));
        goto exit;
    }
    
    //
    // Get handle to session process
    //
    err = XcOpenCurrentProcessHandle(s_session, &s_processHandle);
    if (err == DrError_UnknownProcess && s_processHandle == INVALID_XCPROCESSHANDLE)
    {
        // todo: remove commented code
//JC        //Job manager not running under a CN will have to do the following initialization
//JC        CreateGlobalJob();

        //
        // If process cannot be found, report and exit
        //
        DrLogE( "DryadConfigurationManager - XcOpenCurrentProcessHandle failed (because not running on a compute node?), error=%s",
            DRERRORSTRING(err));
        goto exit;
    }
    else if(err != DrError_OK)
    {
        //
        // If failure other than unknown process, report and exit
        //
        DrLogE( "DryadConfigurationManager - XcOpenCurrentProcessHandle failed, error=%s",
            DRERRORSTRING(err));
        goto exit;
    }

    //
    // Initialize the dryad metadata and start listening for IO completion events
    //
    DryadInitialize();
    err = DrError_OK;

exit:

    return err;
}

//
// Shut down the completion port and xcompute session
//
DrError DryadShutdownXCompute()
{
    DrError err = DrError_OK;

    //
    // Get the number of outstanding requests remaining
    // todo: do we want this log? if so, use logging framework 
    //
    UInt32 n = g_dryadNativePort->GetOutstandingRequests();
//VS    fprintf(stdout, "DryadShutdownXCompute:  %u outstanding requests\n", n);
    while (n > 1)  // There will always be 1 outstanding request for the next vertex command
    {
        //
        // While their are requests remaining beyond 1, wait 10 seconds and try again
        // todo: do we want this log? probably not since this could be indefinite
        // todo: is 10 seconds appropriate?
        //
        Sleep(10);
        n = g_dryadNativePort->GetOutstandingRequests();
//VS        fprintf(stdout, "DryadShutdownXCompute:  %u outstanding requests\n", n);
    }
    
    //
    // Close the xcompute session
    //
    err = XcCloseSession(s_session);
    return err;
}

//
// Initialize the dryad metadata and start up a completion port waiting for events
//
void DryadInitialize()
{
    // todo: check whether this commented out code matters
//JC    DryadInitPropertyTable();
//JC    DryadInitTagTable();
//JC     DryadInitErrorTable();

    //
    // Initialize the tables defining the metadata
    //
    DryadInitMetaDataTable();

    //
    // Create a port and start it
    //
    g_dryadNativePort = new DryadNativePort(4, 2);
    g_dryadNativePort->Start();
}


XDRESSIONHANDLE GetSessionHandle()
{
    return s_session;
}

XCPROCESSHANDLE GetProcessHandle()
{
    return s_processHandle;
}

//JC
#if 0
#include <dryadpropertydumper.h>
#include <IConfiguration.h>
#include <Configuration.h>
#include <ConfigurationMap.h>
#include <ConfigurationManager.h>
#include <dryadnativeport.h>
#include <dryadmetadata.h>
#include <dryaderrordef.h>


static char* s_configFileName = "cosmos.ini";

#if 0
static char* s_cosmosOptionPrefix = "--inioption:";
#endif

static char* s_verbosePrefix = "--verbose";
static char* s_popupPrefix = "--popup";
static char* s_debugBreakPrefix = "--debugbreak";
static char* s_assertHandlerPrefix = "--asserthandler";
static char* s_disableProfiler = "--disableProfiler";

static const char* s_defaultParameters[] = {
    "Counters", "NoCountersFile", "1",
    "Dryad", "Cluster", "{!machinename}",
    "Dryad", "DumpAllSentMessages", "false",
    "Dryad", "DumpAllReceivedMessages", "false",

    "stdout", "LogSourceInfo", "1",

    "LogRules", "Rule1", "*,*,*,localLog",
    "LogRules", "Rule120", "*,A,*,terminate",
    "LogRules", "Rule121", "*,SEAW,*,stdout",

    "localLog", "FileNameBase", "local\\Log",
    "localLog", "MaxFiles", "100",
    "localLog", "MaxFileSize", "10000000",
    "localLog", "BufferSize", "10000",

    NULL
};

static const char* s_dryadProfilerLogRule = "Rule122";

class DryadConfigurationManager : public ConfigurationManager
{
private:
    DrStr64 m_strNetLibName;
    DrStr128 m_strIniFileName;
    int m_argc;
    char** m_argv; // heap allocated copy
    int m_nOpts; // number of command line arguments consumed
        
public:
    DryadConfigurationManager(
        const char* netLibName,
        const char* iniFileName,
        int argc,
        char* argv[])
    {
        m_strNetLibName = netLibName;
        m_strIniFileName = iniFileName;
        m_argc = argc;
        m_nOpts = 0;
        if (argc == 0) {
            m_argv = NULL;
        } else {
            LogAssert(argv != NULL);
            m_argv = new char *[argc];
            LogAssert(m_argv != NULL);
            for (int i = 0; i < argc; i++) {
                if (argv[i] == NULL) {
                    m_argv[i] = NULL;
                } else {
                    size_t len = strlen(argv[i])+1;
                    m_argv[i] = new char[len];
                    LogAssert(m_argv[i] != NULL);
                    memcpy(m_argv[i], argv[i], len);
                }
            }
        }
    }

    ~DryadConfigurationManager()
    {
        for (int i = 0; i < m_argc; i++) {
            if (m_argv[i] != NULL) {
                delete[] m_argv[i];
            }
        }
        if (m_argv != NULL) {
            delete[] m_argv;
        }
    }

    int GetNumOptsConsumed()
    {
        return m_nOpts;
    }

    DrError ApplyProfilerConfig(IMutableConfiguration *cosmosFile) {
        DrError err = DrError_OK;
        cosmosFile->SetParameter("LogRules",
                                 s_dryadProfilerLogRule, "DryadProfiler,*,*,collectorDryadProfiler");
        cosmosFile->SetParameter("collectorDryadProfiler",
                                 "FileNameBase", "collector\\dryadProfiler");
        cosmosFile->SetParameter("collectorDryadProfiler",
                                 "MaxFiles", "0");
        cosmosFile->SetParameter("collectorDryadProfiler",
                                 "MaxFileSize", "10000000");
        cosmosFile->SetParameter("collectorDryadProfiler",
                                 "BufferSize", "10000");
        return err;
    }

    // returns the number of arguments consumed in *pNOpts
    DrError ApplyDryadConfigOverrides(
        int argc, // Number of command line arguments eligible for parsing
        char* argv[], // Command line arguments eligible for parsing
        int* pNOpts, // Returned # of command line arguments consumed
        IMutableConfiguration *cosmosFile) // Returned autopilot.ini contents
    {
        DrError err = DrError_OK;
        bool disableProfiler = false;

        *pNOpts = 0;

        ++argv;
        --argc;
        while (argc > 0)
        {
#if 0
            if (::_strnicmp(argv[0], s_cosmosOptionPrefix,
                            ::strlen(s_cosmosOptionPrefix)) == 0)
            {
                if (argc < 2)
                {
                    err = DrError_InvalidParameter;
                    goto exit;
                }

                if (::strcmp(argv[1], "-") == 0)
                {
                    cosmosFile->RemoveSectionFromArg(argv[0]);
                }
                else if (::strchr(argv[1], '=') == NULL)
                {
                    cosmosFile->RemoveParameterFromArg(argv[0], argv[1]);
                }
                else
                {
                    cosmosFile->SetParameterFromArg(argv[0], argv[1]);
                }

                argv += 2;
                argc -= 2;
                (*pNOpts) += 2;
            }
            else 
#endif                
                if (::_strnicmp(argv[0], s_verbosePrefix,
                                 ::strlen(s_verbosePrefix)) == 0)
            {
                cosmosFile->SetParameter("Dryad",
                                         "DumpAllSentMessages", "true");
                cosmosFile->SetParameter("Dryad",
                                         "DumpAllReceivedMessages", "true");
                cosmosFile->SetParameter("Dryad",
                                         "MessageDumpFile",
                                         "messages.{!component}.{!nodename}.txt");

                cosmosFile->SetParameter("LogRules",
                                         "Rule121", "*,ISEAW,*,stdout");

                argv += 1;
                argc -= 1;
                (*pNOpts) += 1;
            }
            else if (::_strnicmp(argv[0], s_popupPrefix,
                                 ::strlen(s_popupPrefix)) == 0)
            {
                cosmosFile->SetParameter("LogRules",
                                         "Rule120", "*,A,*,popup");

                argv += 1;
                argc -= 1;
                (*pNOpts) += 1;
            }
            else if (::_strnicmp(argv[0], s_assertHandlerPrefix,
                                 ::strlen(s_assertHandlerPrefix)) == 0)
            {
                cosmosFile->SetParameter("LogRules",
                                         "Rule122", "*,A,*,applicationcallback");

                argv += 1;
                argc -= 1;
                (*pNOpts) += 1;
            }
            else if (::_strnicmp(argv[0], s_debugBreakPrefix,
                                 ::strlen(s_debugBreakPrefix)) == 0)
            {
                argv += 1;
                argc -= 1;
                (*pNOpts) += 1;

                ::DebugBreak();
            }
            else if (::_strnicmp(argv[0], s_disableProfiler,
                                 ::strlen(s_disableProfiler)) == 0)
            {
                argv += 1;
                argc -= 1;
                (*pNOpts) += 1;

                disableProfiler = true;
            }            
            else
            {
                break;
            }
        }

        err = DrError_OK;
        if (!disableProfiler) {
            err = ApplyProfilerConfig(cosmosFile);
        }
        
        return err;
    }
    
    
    bool DoInitialize()
    {  
        if (m_valid) {
            // We don't want perf counter files
            Counters::SetInitNoPerfFiles();
        }
        
        // Register ourselves as the singleton config manager
        if (!m_valid || !Configuration::PreInitialize(this)) {
            DrLogA( "DryadConfigurationManager",
                "Failed to preinitialize dryad configuration manager");
            m_valid = false;
        }

        if (m_valid) {
            if (!CommonInit(m_strIniFileName.GetString(), 0, -1 )) {
                DrLogA( "DryadConfigurationManager",
                    "Failed to initialize dryad configuration manager");
                m_valid = false;
            }
        }

        return m_valid;
    }

        /**
         * Called at the start of InitBootstrapConfiguration, this method is a last chance for a subclass to mess with the bootstrap configuration before it is used.
         *
         * On entry, bootstrapConfigPathname is the fully qualified name of the bootstrap file.
         *                bootstrapConfiguration is the raw (no macro expansion or override collapsing) configuration, or NULL if the configuration was not found.
         *
         * On exit, bootstrapConfiguration is the final raw (no macro expansion or override collapsing) bootstrap configuration. If NULL, initialization will fail.
         *
         * The default implementation does nothing.
         *
         * Returns false if initialization should fail.
         */
        virtual bool PreprocessBootstrapConfiguration(const char *bootstrapConfigPathname, Ptr<const IConfiguration>& bootstrapConfiguration)
        {
            if (bootstrapConfiguration == NULL) {
                // The bootstrap config file is missing -- build a default one
                DrStr64 strDataDirLocation;
                DrStr32 strRelDataDirLocation;
                strRelDataDirLocation.SetF(".\\DataDir.%u",  GetCurrentProcessId());
                DrError err = DrCanonicalizeFilePath(strDataDirLocation, strRelDataDirLocation);
                if (err != DrError_OK) {
                    DrLogE( "DryadConfigurationManager",
                        "Failed to canonicalize data directory name %s error=%s",
                        strRelDataDirLocation.GetString(), DRERRORSTRING(err));
                    return false;
                }
                
                Ptr<IMutableConfiguration> cfg = Configuration::GenerateDefaultBootstrapConfig(
                    strDataDirLocation.GetString(),
                    "...",
                    "default",
                    NULL);
                if (cfg == NULL) {
                    DrLogE( "DryadConfigurationManager",
                        "Failed to create default bootstrap file");
                    return false;
                }

                bootstrapConfiguration = cfg;
            }
            
            return true;
        }

        /**
         * Called immediately after attempted loading of the default configuration, this method is a last chance for a subclass to
         * mess with the default configuration before it is used.
         *
         * On entry, defaultConfigPathname is the fully qualified name of the default configuration.
         *                "configuration" is the default filtered view of the configuration, or NULL if the configuration was not found
         *                 rawConfiguration is the raw (no macro expansion or override collapsing) configuration, or NULL if the configuration was not found.
         *
         * On exit, rawConfiguration is the final raw (no macro expansion or override collapsing) default configuration. If NULL, initialization will fail.
         *
         * The default implementation does nothing.
         *
         * Returns false if initialization should fail.
         */
        virtual bool PreprocessDefaultConfiguration(
            const char *defaultConfigPathname,
            const IConfiguration *configuration,
            Ptr<const IConfiguration>& rawConfiguration)
        {
            // Create an editable version of the configuration
            Ptr<IMutableConfiguration> newConfig;
            if (configuration == NULL) {
                // The config file is missing -- build a default one
                newConfig = Configuration::GenerateDefaultConfig();
                if (newConfig == NULL) {
                    DrLogE( "DryadConfigurationManager",
                        "Failed to create default config file");
                    return false;
                }
                for (const char **ppDefaults = s_defaultParameters; *ppDefaults != NULL; ppDefaults += 3) {
                    const char *section = ppDefaults[0];
                    const char *param = ppDefaults[1];
                    LogAssert(param != NULL);
                    const char *value = ppDefaults[2];
                    LogAssert(value != NULL);
                    newConfig->SetParameter(section, param, value);
                }
            } else {
                newConfig = new ConfigurationMap(configuration);
                if (newConfig == NULL) {
                    DrLogE( "DryadConfigurationManager",
                        "Failed to create copy of config file");
                    return false;
                }
            }

            // process the command line to override values
            DrError err = ApplyDryadConfigOverrides(m_argc, m_argv, &m_nOpts, newConfig);
            if (err != DrError_OK) {
                DrLogE( "DryadConfigurationManager",
                    "Failed to apply command line overrides to config file: %s", err);
                return false;
            }

            // replace the configuration with the edited one. Note that overrides, etc. have already been applied and removed.
            rawConfiguration = newConfig;
            return true;
        }

};



DrJobTicket* CreateGlobalJob()
{    
    DrError hr = S_OK;
    DrServiceDescriptor sd;
    DrRef<DrJobTicket> jobTicket = g_pDryadConfig->GetDefaultJobTicket();    
    
    sd.Set("xcps", g_pDryadConfig->GetDefaultClusterName(), NULL, "rd.RDRBasic.XComputeProcessScheduler_0");        
    
    DrRef<XcPsCreateJobRequest> msg;
    msg.Attach(new XcPsCreateJobRequest());
    msg->SetCreateJobTicket(jobTicket);    
    XcJobConstraint& constraint = msg->CreateJobConstraint();
    constraint.SetMaxConcurrentProcesses(999);
    constraint.SetMaxExecutionTime(DrTimeInterval_Hour);
    g_pDrClient->SendTo(msg, sd);    
    msg->WaitForResponse( &hr );
    if (hr != DrError_OK)
    {
        DrLogE( "DryadConfigurationManager",
            "CreateJob failied, error=%s",
            DRERRORSTRING(hr));
        LogAssert(false);
    }
    return jobTicket.Detach();    
}



#endif // if 0
