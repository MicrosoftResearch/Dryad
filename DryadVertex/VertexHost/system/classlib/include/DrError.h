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

//JC Check this file for redundant information

// DrError.h
//
// This file must contain *only* DEFINE_DR_ERROR directives!
//
//
// It is included multiple times with different macro definitions.

#ifndef COMMON_DR_ERRORS_DEFINED

// The operation succeeded
DEFINE_DR_ERROR (DrError_OK,                        S_OK,                   "The operation succeeded")

// The operation failed
DEFINE_DR_ERROR (DrError_Fail,                      E_FAIL,                 "The operation failed")

#endif

// Out of memory
DEFINE_DR_ERROR (DrError_OutOfMemory,               DR_ERROR (0x0001), "Out of memory")

// The operation has not yet completed
DEFINE_DR_ERROR (DrError_IncompleteOperation,       DR_ERROR (0x0002), "The operation has not yet completed")

// Out of Range
DEFINE_DR_ERROR (DrError_OutOfRange,                DR_ERROR (0x0003), "Out of Range")

#ifndef COMMON_DR_ERRORS_DEFINED

// Invalid Parameter
DEFINE_DR_ERROR (DrError_InvalidParameter,          DR_ERROR (0x0004), "Invalid Parameter")

#endif

// Operation timed out
DEFINE_DR_ERROR (DrError_Timeout,                   DR_ERROR (0x0005), "Operation timed out")

// Remote end of the connection disconnected
DEFINE_DR_ERROR (DrError_RemoteDisconnected,        DR_ERROR (0x0006), "Remote end of the connection disconnected")

// Local end of the connection disconnected
DEFINE_DR_ERROR (DrError_LocalDisconnected,         DR_ERROR (0x0007), "Local end of the connection disconnected")

// Listen operation was stopped
DEFINE_DR_ERROR (DrError_ListenStopped,             DR_ERROR (0x0008), "Listen operation was stopped ")

// Stream already exists
DEFINE_DR_ERROR (DrError_StreamAlreadyExists,       DR_ERROR (0x0009), "Stream already exists")

// Stream not found
DEFINE_DR_ERROR (DrError_StreamNotFound,            DR_ERROR (0x000a), "Stream not found")

#ifndef COMMON_DR_ERRORS_DEFINED

// End of stream
DEFINE_DR_ERROR (DrError_EndOfStream,               DR_ERROR (0x000b), "End of stream")

// Invalid property
DEFINE_DR_ERROR (DrError_InvalidProperty,           DR_ERROR (0x000c), "Invalid property")

#endif

// Extent not found
DEFINE_DR_ERROR (DrError_ExtentNotFound,            DR_ERROR (0x000d), "Extent not found")

#ifndef COMMON_DR_ERRORS_DEFINED

// Unable to peek this far forward
DEFINE_DR_ERROR (DrError_PeekTooFar,                DR_ERROR (0x000e), "Unable to peek this far forward")

// String is too long
DEFINE_DR_ERROR (DrError_StringTooLong,             DR_ERROR (0x000f), "String is too long")

#endif

// operation aborted
DEFINE_DR_ERROR (DrError_Aborted,                   DR_ERROR (0x0010), "The operation was aborted")

// Directory does not exist
DEFINE_DR_ERROR (DrError_DirectoryDoesNotExist,     DR_ERROR (0x0011), "Directory does not exist")

// Invalid version
DEFINE_DR_ERROR (DrError_InvalidVersion,            DR_ERROR (0x0012), "Invalid version")

// Host not found or name resolution error
DEFINE_DR_ERROR (DrError_HostNotFound,              DR_ERROR (0x0013), "Host not found")

// Parameter not found
DEFINE_DR_ERROR (DrError_ParameterNotFound,         DR_ERROR (0x0014), "Parameter not found")

// Bad offset
DEFINE_DR_ERROR (DrError_BadOffset,                 DR_ERROR (0x0015), "Bad offset")

// No available extent instances
DEFINE_DR_ERROR (DrError_NoExtentInstances,         DR_ERROR (0x0016), "No available extent instances")

// Invalid pathname
DEFINE_DR_ERROR (DrError_InvalidPathname,           DR_ERROR (0x0017), "Invalid pathname")

// Not implemented
DEFINE_DR_ERROR (DrError_NotImplemented,            DR_ERROR (0x0018), "Not implemented")

// Improper protocol response received
DEFINE_DR_ERROR (DrError_ImproperProtocolResponse,  DR_ERROR (0x0019), "Improper Protocol Response Received")

// The connection failed
DEFINE_DR_ERROR (DrError_ConnectionFailed,          DR_ERROR (0x001a), "The connection failed")

// Extent cannot be appended as it has reached its size limit
DEFINE_DR_ERROR(DrError_ExtentFull,                 DR_ERROR (0x001b), "The extent is full")

// Extent has been sealed
DEFINE_DR_ERROR(DrError_ExtentSealed,               DR_ERROR (0x001c), "The extent is sealed")

// Extent failed to seal after reaching its size limit on an append
DEFINE_DR_ERROR(DrError_ExtentSealFailOnFull,       DR_ERROR (0x001d), "The extent failed to seal as full")

// Extent operation that can only be done on primary attempted on a non-primary EN
DEFINE_DR_ERROR(DrError_EnPrimaryOnly,               DR_ERROR (0x001e), "The extent instance is not the primary instance")

// Timeout waiting for a protocol response
DEFINE_DR_ERROR(DrError_ResponseTimeout,            DR_ERROR (0x001f), "The Dryad server did not respond to the request")

// Timeout waiting for a protocol send
DEFINE_DR_ERROR (DrError_SendTimeout,               DR_ERROR (0x0020), "Unable to reach the Dryad node")

// Name server unavailable
DEFINE_DR_ERROR (DrError_NameServerUnavailable,     DR_ERROR (0x0021), "Dryad Name server unavailable")

#ifndef COMMON_DR_ERRORS_DEFINED

// Line Too Long
DEFINE_DR_ERROR (DrError_LineTooLong,               DR_ERROR (0x0022), "Line is too long")

#endif

// Process Already Exists
DEFINE_DR_ERROR (DrError_ProcessAlreadyExists,      DR_ERROR (0x0023), "Process already exists")

// Extent operation that can only be done only on non-primary attempted on primary
DEFINE_DR_ERROR (DrError_EnNonPrimaryOnly,          DR_ERROR (0x0024), "The extent instance is the primary instance")

// Process Already Exists
DEFINE_DR_ERROR (DrError_UnknownProcess,            DR_ERROR (0x0025), "Unknown process")

// Extent is incomplete (and therefore whatever operation was being atttempted is invalid)
DEFINE_DR_ERROR (DrError_ExtentIncomplete,          DR_ERROR (0x0026), "The extent is incomplete")

// Length specified is invalid
DEFINE_DR_ERROR (DrError_InvalidLength,             DR_ERROR (0x0028), "Invalid length")

// Retry limit reached (client lib append)
DEFINE_DR_ERROR (DrError_RetryLimit,                DR_ERROR (0x0029), "Retry limit reached")

// Previous operation in queue failed (client lib fixed extent read/write queues)
DEFINE_DR_ERROR (DrError_PreviousFailed,            DR_ERROR (0x002a), "Previous operation in queue failed")

// This handle is not open for read
DEFINE_DR_ERROR (DrError_HandleInvalidModeRead,     DR_ERROR (0x002b), "This handle is not open for read")

// This handle is not open for append
DEFINE_DR_ERROR (DrError_HandleInvalidModeAppend,   DR_ERROR (0x002c), "This handle is not open for append")

// client lib Initialization failed
DEFINE_DR_ERROR (DrError_InitializationFailed,      DR_ERROR (0x002d), "Initialization failed")

// invalid handle
DEFINE_DR_ERROR (DrError_InvalidHandle,             DR_ERROR (0x002e), "Invalid handle value")

// Disconnect waiting for a protocol response
DEFINE_DR_ERROR(DrError_ResponseDisconnect,         DR_ERROR (0x002f), "Connection lost while waiting for a response")

// File-based stream allows only 0 extent index.
DEFINE_DR_ERROR(DrError_InvalidFileExtentIndex,     DR_ERROR (0x0030), "Local file-based Dryad stream allows only 0 extent index")

// Attempt to append beyond an unsealed extent in a stream
DEFINE_DR_ERROR (DrError_OffsetIsUnsealed,          DR_ERROR (0x0031), "Offset is unsealed")

// Request/call/operation unexpected
DEFINE_DR_ERROR(DrError_Unexpected,                 DR_ERROR (0x0032), "Unexpected call or request")

#ifndef COMMON_DR_ERRORS_DEFINED

// Invalid time interval
DEFINE_DR_ERROR(DrError_InvalidTimeInterval,        DR_ERROR (0x0033), "Invalid time interval")

#endif

// Too many redirects
DEFINE_DR_ERROR(DrError_TooManyRedirects,           DR_ERROR (0x0034), "Too many redirects")

// Incompatible rendezvous parties
DEFINE_DR_ERROR(DrError_IncompatibleRendezvousParties, DR_ERROR (0x0035), "Incompatible rendezvous parties")

// No matching rendezvous part
DEFINE_DR_ERROR(DrError_NoMatchingRendezvousParty,  DR_ERROR (0x0036), "No matching rendezvous party")

// Failed to seal an extent (used in failure mode sealing)
DEFINE_DR_ERROR (DrError_FailedToSeal,              DR_ERROR (0x0037), "Failed to seal extent")

//Stream option id is invalid
DEFINE_DR_ERROR(DrError_InvalidOption,              DR_ERROR (0x0038), "Stream option id is invalid")

//Stream option value size is invalid
DEFINE_DR_ERROR(DrError_InvalidOptionSize,          DR_ERROR (0x0039), "Stream option value size is invalid")

//Item not found in lookup table, typically converted to specific item code
DEFINE_DR_ERROR(DrError_ItemNotFound,               DR_ERROR (0x003a), "Item not found")

// The pipe was redirected
DEFINE_DR_ERROR(DrError_InternalPipeRedirected,     DR_ERROR (0x003b), "The pipe was redirected (internal)")

// request did not succeed, please talk to the primary Server
DEFINE_DR_ERROR(DrError_TalkToPrimaryServer,        DR_ERROR (0x003c), "Please talk to primary server for this request")

// Invalid pipe URI
DEFINE_DR_ERROR(DrError_InvalidPipeUri,             DR_ERROR (0x003d), "Invalid Pipe URI")

// Artificial fault error
DEFINE_DR_ERROR(DrError_ArtificialError,            DR_ERROR (0x003e), "Artificial test error; Please test again")

// Both streams should be on the same volume.
DEFINE_DR_ERROR(DrError_SameVolumeRequired,         DR_ERROR (0x003f), "Both streams should be on the same volume")

// Append block cannot be larger than the extent size
DEFINE_DR_ERROR(DrError_AppendBlockTooLarge,        DR_ERROR (0x0040), "Append block is larger then the local limit for append or extent size")

// Message was inconsistent
DEFINE_DR_ERROR(DrError_InconsistentMessage,        DR_ERROR (0x0041), "Inconsistent message")

// Message was inconsistent
DEFINE_DR_ERROR(DrError_OutOfLimits,                DR_ERROR (0x0042), "The value was out of given limits")

// Client specified a volume ID on the DRM that does not match the DRM's volume ID
DEFINE_DR_ERROR(DrError_InvalidVolumeId,            DR_ERROR (0x0043), "The volume id was invalid")

// Unsupported version
DEFINE_DR_ERROR(DrError_UnsupportedVersion,         DR_ERROR (0x0044), "This version is not supported")

// Client lib is not initizalized
DEFINE_DR_ERROR(DrError_ClientNotInit,              DR_ERROR (0x0045), "Dryad Client Library is not initialized")

// Inconsistent configuration
DEFINE_DR_ERROR(DrError_InconsistentConfig,         DR_ERROR (0x0046), "Inconsistent INI Configuration")

// One of hosts in chain failed with network error
DEFINE_DR_ERROR(DrError_ChainedNetworkError,        DR_ERROR (0x0047), "One of hosts in chain failed with network error")

// Process Property Version Mismatch
DEFINE_DR_ERROR(DrError_ProcessPropertyVersionMismatch,        DR_ERROR (0x0048), "Process property version mismatch")

// Current unsealed extent read from primary failed
DEFINE_DR_ERROR(DrError_PrimaryFailed,              DR_ERROR (0x0049), "Current unsealed extent read from primary failed")

// Async operation already completed
DEFINE_DR_ERROR(DrError_AlreadyCompleted,           DR_ERROR (0x004a), "Async operation already completed")

// Response message is invalid.
DEFINE_DR_ERROR(DrError_InvalidReply,               DR_ERROR (0x004b), "Response message is invalid")

// Internal error: required metadata is missing.
DEFINE_DR_ERROR(DrError_MissingMetadata,            DR_ERROR (0x004c), "Internal error: required metadata is missing")

// Internal error: required metadata is in inconsistent state.
DEFINE_DR_ERROR(DrError_CorruptMetadata,            DR_ERROR (0x004d), "Internal error: required metadata is in inconsistent state")

// Unable to seek over unsealed extent.
DEFINE_DR_ERROR(DrError_NoSeekOverUnsealed,         DR_ERROR (0x004e), "Unable to seek over unsealed extent")

// Too many files being retrieved by the cache manager
DEFINE_DR_ERROR(DrError_TooManyFilesBeingRetrieved, DR_ERROR (0x004f), "Too many files being retrieved by the cache manager")

// Primary extent instance is not present in retrieved metadata
DEFINE_DR_ERROR(DrError_NoPrimary,                  DR_ERROR (0x0050), "Primary extent instance is not present in retrieved metadata")

// Stream is read-only
DEFINE_DR_ERROR(DrError_StreamReadOnly,             DR_ERROR (0x0051), "Stream is read-only")

// Async operation was queued up successfully and it's now pending execution
DEFINE_DR_ERROR(DrError_Pending,                    DR_ERROR (0x0052), "Operation is in progress")

// Attempted operation is invalid in the given context
DEFINE_DR_ERROR(DrError_InvalidOperation,           DR_ERROR (0x0053), "Executing operation is invalid in the given context")

// Extent is not sealed
DEFINE_DR_ERROR(DrError_ExtentNotSealed,            DR_ERROR (0x0054), "Extent is not sealed")

// Duplicated resources
DEFINE_DR_ERROR(DrError_DuplicatedResources,        DR_ERROR (0x0055), "This file exists; possible duplicated resources specified")

// Unhandled STL exception
DEFINE_DR_ERROR(DrError_InternalException,          DR_ERROR (0x0056), "Unhandled internal exception")

// Pn Queue is full and can not admit a new process
DEFINE_DR_ERROR(DrError_PnQueueFull,                DR_ERROR (0x0057), "Pn Queue Full")

// Append request not at end of data
DEFINE_DR_ERROR(DrError_AppendNotAtEnd,             DR_ERROR (0x0058), "Append request not at end of data")

// Supplied password for Pn accounts is not correct
DEFINE_DR_ERROR(DrError_IncorrectPnPassword,        DR_ERROR (0x0059), "Incorrect PN password")

// Extent is not in active state. It's either deleted, recycled, incomplete or corrupt.
DEFINE_DR_ERROR(DrError_ExtentNotActive,            DR_ERROR (0x0060), "Extent is not in active state; it is either deleted, recycled, incomplete or corrupt")

// Seal operation is in progress.
DEFINE_DR_ERROR(DrError_SealInProgress,             DR_ERROR (0x0061), "Seal operation is in progress")

// Extent needs failure mode seal.
DEFINE_DR_ERROR(DrError_ExtentNeedsSeal,            DR_ERROR (0x0062), "Extent needs failure mode seal")

// Extent needs failure mode seal.
DEFINE_DR_ERROR(DrError_UnknownMethod,              DR_ERROR (0x0063), "Unknown request or method")

// not authorized
DEFINE_DR_ERROR(DrError_NotAuthorized,              DR_ERROR (0x0064), "Client request cannot be authorized")

// Syntax error
DEFINE_DR_ERROR(DrError_SyntaxError,                DR_ERROR (0x0065), "Syntax error")

// Attempt to suspend a task that was already suspended
DEFINE_DR_ERROR(DrError_TaskAlreadySuspended,       DR_ERROR (0x0066), "Task already suspended")

// Attempt to unsuspend a task that was already unsuspended
DEFINE_DR_ERROR(DrError_TaskAlreadyUnsuspended,     DR_ERROR (0x0067), "Task already unsuspended")

// Task does not exist
DEFINE_DR_ERROR(DrError_TaskNotFound,               DR_ERROR (0x0068), "Task does not exist")

// Task exists but it is in the deleted state
DEFINE_DR_ERROR(DrError_TaskDeleted,                DR_ERROR (0x0069), "Task is in the deleted state")

// Task already completed (e.g. you tried to suspend it when it was already completed)
DEFINE_DR_ERROR(DrError_TaskAlreadyCompleted,       DR_ERROR (0x006A), "Task already completed")

// Scheduler already paused
DEFINE_DR_ERROR(DrError_SchedulerAlreadyPaused,     DR_ERROR (0x006B), "Scheduler already paused")

// Scheduler already unpaused
DEFINE_DR_ERROR(DrError_SchedulerAlreadyUnpaused,   DR_ERROR (0x006C), "Scheduler already unpaused")

// DrServiceDescriptor name resolving errors
DEFINE_DR_ERROR(DrError_ClusterNotFound,            DR_ERROR (0x006D), "Cluster not found")
DEFINE_DR_ERROR(DrError_ServiceTypeNotFound,        DR_ERROR (0x006E), "Service type not found")
DEFINE_DR_ERROR(DrError_NamespaceNotFound,          DR_ERROR (0x006F), "Namespace/volume not found")
DEFINE_DR_ERROR(DrError_ServiceInstanceNotFound,    DR_ERROR (0x0070), "Service instance not found")

// Job ticket is invalid
DEFINE_DR_ERROR(DrError_JobTicketInvalid,           DR_ERROR (0x0071), "Job ticket is invalid")

// Job ticket has expired
DEFINE_DR_ERROR(DrError_JobTicketExpired,           DR_ERROR (0x0072), "Job ticket has expired")

// Another authentication is in process
DEFINE_DR_ERROR(DrError_AuthenticationInProcess,    DR_ERROR (0x0073), "Another authentication is in process")


// i-th readahead request was satisfied with (i-1)-th
DEFINE_DR_ERROR(DrError_ReadaheadAleadySatisfied,   DR_ERROR (0x0074), "Readahead satisfied by previous request")

//
// Cache service errors
//
DEFINE_DR_ERROR(DrError_CacheItemFetchIncomplete,   DR_ERROR (0x0075), "Cache Item is still being fetched from source")
DEFINE_DR_ERROR(DrError_CacheItemFetchFailed,       DR_ERROR (0x0076), "Failed to fetch the cache item from source")
DEFINE_DR_ERROR(DrError_CacheItemSourceInvalid,     DR_ERROR (0x0077), "The source of cache item is invalid")

// PN process states
DEFINE_DR_ERROR(DrError_PnProcessInitializing,     DR_ERROR (0x0078), "The process is being initialized")
DEFINE_DR_ERROR(DrError_PnProcessCreated,     DR_ERROR (0x0079), "The process has been created")
DEFINE_DR_ERROR(DrError_PnProcessRunning,     DR_ERROR (0x007A), "The process is running")
DEFINE_DR_ERROR(DrError_PnProcessCreateFailed,     DR_ERROR (0x007B), "Process create failed")

// VCWS errors
DEFINE_DR_ERROR(DrError_VcwsVirtualClusterNotFound, DR_ERROR (0x007C), "Virtual cluster not found")
DEFINE_DR_ERROR(DrError_VcwsMountPointNotFound,     DR_ERROR (0x007D), "Mount point not found")
DEFINE_DR_ERROR(DrError_VcwsPermissionDenied,        DR_ERROR (0x007E), "Permission denied")
DEFINE_DR_ERROR(DrError_VcwsInvalidMountPoint,        DR_ERROR (0x007F), "Invalid mount point")
DEFINE_DR_ERROR(DrError_VcwsInvalidVirtualCluster,    DR_ERROR (0x0080), "Invalid virtual cluster")
DEFINE_DR_ERROR(DrError_VcwsIncompleteMountPoint,  DR_ERROR (0x0081), "Incomplete mount point")
DEFINE_DR_ERROR(DrError_DmRequestFailed,  DR_ERROR (0x0082), "Failed to send command to DM")
DEFINE_DR_ERROR(DrError_CjsNoMaster,  DR_ERROR (0x0083), "No master of CJS machines from DM")

//PN process states continued
DEFINE_DR_ERROR(DrError_PnProcessSystemProcessRunning,     DR_ERROR (0x0084), "Could not create process due to System priority process.")

DEFINE_DR_ERROR(DrError_XmlAttributeNotFound,     DR_ERROR (0x0085), "Attribute cannot be found in XML")
DEFINE_DR_ERROR(DrError_VcwsShareNameAlreadyExists,     DR_ERROR (0x0086), "Share name has already exists in the virtual cluster")
DEFINE_DR_ERROR(DrError_VcwsShareNotFound,     DR_ERROR (0x0087), "Share not found")
DEFINE_DR_ERROR(DrError_VcwsMultipleShareNames,     DR_ERROR (0x0088), "This directory has already been shared with another share name")
DEFINE_DR_ERROR(DrError_VcwsJobResourceTooLarge,     DR_ERROR (0x0089), "The size of some job resource is too large")
DEFINE_DR_ERROR(DrError_VcwsTooManyLogAppendRequestsInProgress,     DR_ERROR (0x008a), "There are too many log append requests in progress and please re-try later.")
DEFINE_DR_ERROR(DrError_VcwsInvalidVcPath,     DR_ERROR (0x008b), "The VC path format is invalid.")

//PN process states continued
DEFINE_DR_ERROR(DrError_PnDeploymentInProgress,     DR_ERROR (0x008C), "Could not create process due to deployment of cosmos code.")

// Mirroring
DEFINE_DR_ERROR (DrError_MirrorPolicyNotFound,     DR_ERROR (0x0090), "MirrorPolicyNotFound")

// Returned when you try to perform some operation on a mirrored object that is not allowed; e.g. deleting a mirrored
// stream or directory, or setting up mirroring for a directory that is already mirrored.
DEFINE_DR_ERROR (DrError_ObjectIsMirrored, DR_ERROR (0x0091), "ObjectIsMirrored")

DEFINE_DR_ERROR (DrError_DirectoryAlreadyMirrored, DR_ERROR (0x0092), "DirectoryAlreadyMirrored")
DEFINE_DR_ERROR (DrError_MirrorInstanceNotFound, DR_ERROR (0x0093), "MirrorInstanceNotFound")

// Returned when you try to perform an illegal operation on a mirror instance that is the master; e.g. trying to delete it
DEFINE_DR_ERROR (DrError_MirrorInstanceIsMaster, DR_ERROR (0x0094), "MirrorInstanceIsMaster")

DEFINE_DR_ERROR (DrError_MirrorInstanceAlreadyExists, DR_ERROR (0x0095), "MirrorInstanceAlreadyExists")
DEFINE_DR_ERROR (DrError_DirectoryNotMirrored, DR_ERROR (0x0096), "DirectoryNotMirrored")

// Returned when mirror policy name does not start with the "cluster.volume." prefix of the master
DEFINE_DR_ERROR (DrError_InvalidMirrorPolicyName, DR_ERROR (0x0097), "InvalidMirrorPolicyName")

DEFINE_DR_ERROR (DrError_MirrorPolicyAlreadyExists, DR_ERROR (0x0098), "MirrorPolicyAlreadyExists")

DEFINE_DR_ERROR (DrError_UnalbeToAllocateExtentInstances,         DR_ERROR (0x0099), "Unable to allocate extent instances")

// VCWS errors continued
DEFINE_DR_ERROR(DrError_VcwsDrJobTimeout,  DR_ERROR (0x009a), "Your stream request could not be completed in time because of unusually high latency to the volume of your requested resource. Please try again later.")
DEFINE_DR_ERROR(DrError_VcwsDrJobReject,  DR_ERROR (0x009b), "Your stream request is being rejected because too many timeouts to the volume of your requested resource have been observed. Please try again later.")

// network send abort: map the same error in DrNetlib
DEFINE_DR_ERROR (DrError_SendAborted,         DR_ERROR (0x009c), "Send to cosmos node aborted")

// PN memory, execution time and max execution time exceeded, speculative process killed
DEFINE_DR_ERROR (DrError_ExecutionQuotaExceeded,         DR_ERROR (0x009d), "Process exceeded execution time lease")
DEFINE_DR_ERROR (DrError_MaxExecutionQuotaExceeded,         DR_ERROR (0x009e), "Process execution time exceeded upper threshold")
DEFINE_DR_ERROR (DrError_MemoryQuotaExceeded,         DR_ERROR (0x009f), "Process exceeded memory quota")
DEFINE_DR_ERROR (DrError_SpeculativeProcessAborted,         DR_ERROR (0x0110), "Speculative process aborted")
DEFINE_DR_ERROR (DrError_ProcessTerminated,         DR_ERROR (0x0111), "Process terminated by client")

// service is too busy processing requests on the stream
DEFINE_DR_ERROR (DrError_StreamTooBusy,             DR_ERROR (0x0112), "Service is too busy processing requests of this stream")

// VCWS errors continued
DEFINE_DR_ERROR(DrError_VcwsInvalidShareName,     DR_ERROR (0x0120), "There are invalid characters in share name and the list of all invalid characters is \"ASCII 1-31, \", *, :, <, >, ?, \\, <space>, %%, [, ], (, ), &, ;, /, {, }, #\"")
DEFINE_DR_ERROR(DrError_VcwsPermissionDeniedByMountPointSecurityGroups,     DR_ERROR (0x0121), "You are not in the security groups associated with this mount point. Please ask your VC admin for specific information.")

// StreamSet errors
DEFINE_DR_ERROR(DrError_StreamSetTemplateEmpty,     DR_ERROR (0x0130), "[StreamSet:Template] in StreamSet.ini is of zero length.")
DEFINE_DR_ERROR(DrError_InvalidStreamSetHotSpot,     DR_ERROR (0x0131), "[StreamSet:HotSpot] in StreamSet.ini is invalid.")
DEFINE_DR_ERROR(DrError_InvalidStreamSetExpireAfter,     DR_ERROR (0x0132), "[StreamSet:ExpireAfter] in StreamSet.ini is invalid.")
DEFINE_DR_ERROR(DrError_MultipleStreamsForAppend,     DR_ERROR (0x0133), "A streamset being mapped to multiple streams is specified for appending.")

// OpenFromBinary error (clientlib)
DEFINE_DR_ERROR(DrError_CannotCreateFromBinary,     DR_ERROR (0x0134), "A stream cannot be created from binary stream info.")

// APX Client errors
DEFINE_DR_ERROR(DrError_ApxAddressNotFound,     DR_ERROR (0x0140), "No available Autopilot authentication proxy IP address could be found.")
DEFINE_DR_ERROR(DrError_ApxUnexpectedHttpStatusCode,     DR_ERROR (0x0141), "An unexpected HTTP status code received from Autopilot authentication proxy.")

// Dryad Partition Manager
DEFINE_DR_ERROR(DrError_InvalidPartitionEntry,      DR_ERROR (0x0150), "Invalid Dryad partition entry.")
DEFINE_DR_ERROR(DrError_InvalidPartitionTable,      DR_ERROR (0x0151), "Invalid Dryad partition table.")
DEFINE_DR_ERROR(DrError_PartitionNotFound,          DR_ERROR (0x0152), "Dryad partition not found.")
DEFINE_DR_ERROR(DrError_PartitionKeyNotFound,       DR_ERROR (0x0153), "Dryad partition key not found.")
DEFINE_DR_ERROR(DrError_PartitionServerUnavailable, DR_ERROR (0x0154), "Dryad Partition Manager unavailable.")

//
// Cache service errors
//
DEFINE_DR_ERROR(DrError_CacheItemSizeExceedCacheSize,   DR_ERROR (0x0160), "The size of the cache item requested is greater than the stable size of the cache")

//
// HPC Errors
//
DEFINE_DR_ERROR(DrError_InvalidJob,                 DR_ERROR (0x0170), "Invalid HPC Job ID.")


// The error that should never be returned!
// This is useful for initializing error variables so we can test if they
// got set correctly at some point.
// ! DONT RETURN THIS AS AN ERROR EVER !

// An error code was not initialized
DEFINE_DR_ERROR(DrError_Impossible,                 DR_ERROR (0xFFFF), "An error code was not initialized")





/* ------------------------------------ DrIo errors (should eventually go away) --------------------------------- */


// Internal Error
DEFINE_DR_ERROR (DrError_IoInternalError,           DR_ERROR (0x0100), "Internal Error")

// Extent already Exists
DEFINE_DR_ERROR (DrError_IoExtentAlreadyExists,     DR_ERROR (0x0101), "Extent already Exists")

// Extent instance already deleted
DEFINE_DR_ERROR (DrError_IoExtentAlreadyDeleted,    DR_ERROR (0x0102), "Extent instance already deleted")

// Extent instance is corrupted
DEFINE_DR_ERROR (DrError_IoExtentCorrupted,         DR_ERROR (0x0103), "Extent instance is corrupted")

// Cannot open extent instance
DEFINE_DR_ERROR (DrError_IoExtentCannotOpen,        DR_ERROR (0x0104), "Cannot open extent instance")

// The volume is full
DEFINE_DR_ERROR (DrError_IoVolumeIsFull,            DR_ERROR (0x0105), "The volume is full")

// Dryad volume is corrupted
DEFINE_DR_ERROR (DrError_IoVolumeCorrupted,         DR_ERROR (0x0106), "Dryad volume is corrupted")

// Unknown Dryad volume
DEFINE_DR_ERROR (DrError_IoVolumeUnknown,           DR_ERROR (0x0107), "Unknown Dryad volume")

// An I/O operation is pending
DEFINE_DR_ERROR (DrError_IoPending,                 DR_ERROR (0x0108), "An I/O operation is pending")

// An I/O error occurred
DEFINE_DR_ERROR (DrError_IoReadWriteError,          DR_ERROR (0x0109), "An I/O error occurred")

// CRC mismatch
DEFINE_DR_ERROR (DrError_IoCorruptedData,           DR_ERROR (0x010a), "CRC mismatch")

#ifndef COMMON_DR_ERRORS_DEFINED
// Extent instance not found
DEFINE_DR_ERROR (DrError_IoExtentNotFound,          DR_ERROR (0x010b), "Extent instance not found")
#endif

// Stream is sealed
DEFINE_DR_ERROR(DrError_StreamSealed,               DR_ERROR (0x0200), "Stream is sealed")

// Stream has changed
DEFINE_DR_ERROR(DrError_StreamChanged,          DR_ERROR (0x0201), "Stream ID or length has changed")


#ifndef COMMON_DR_ERRORS_DEFINED
#define COMMON_DR_ERRORS_DEFINED
#endif

