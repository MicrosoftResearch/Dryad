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

#pragma once

#pragma warning( push )
/* 'X' bytes padding added after member 'Y' */
#pragma warning( disable: 4820 )


#pragma pack( push, 8 )


#if !defined(_PCVOID_DEFINED)
typedef const void* PCVOID;
#define _PCVOID_DEFINED
#endif



#if defined(__cplusplus)
extern "C" {
#endif


#define DRYADAPI_EXT
#define DRYADAPI __stdcall


typedef struct tagDRHANDLE
{
    ULONG_PTR       Unused;
} *DRHANDLE, **PDRHANDLE;


/*++

DR_ASYNC_INFO structure

Each function that may be executed asynchronously takes pointer
to DR_ASYNC_INFO structure as last parameter.

If NULL is passed then function completes in synchronous manner
and error code is returned as return value.

If parameter is not NULL then operation is carried on in asynchronous manner.
If asynchronous operation has been successfully started then function terminates
immediately with HRESULT_FROM_WIN32(ERROR_IO_PENDING) return value.
Any other return value indicates that it was impossible to start asynchronous operation.

Fields:

    cbSize          Size of structure in bytes. Set to sizeof(DR_ASYNC_INFO).

    pOperationState Pointer to error code returned by completed operation.
                    While operation is in progress value is set to HRESULT_FROM_WIN32(ERROR_IO_PENDING).
                    Before completion is reported value is set to an error code of completed operation.
                    Cannot be NULL.

    Event           Handle to event. Event is set once operation is completed. May be NULL.

    IOCP            Handle to IO completion port. If not NULL then upon completion status is posted
                    to specified completion port.

    pOverlapped     Pointer to OVERLAPPED structure. Used in conjunction with IOCP parameter
                    to post status to IOCP.
                    Should be null if IOCP is NULL, cannot be NULL if IOCP is not NULL.

    CompletionKey   Used in conjunction with IOCP parameter to post status to IOCP.
                    Should be 0 if IOCP is NULL.

    unusedX         Fields reserved for future use.

Note that DR_ASYNC_INFO structure is not required to be available for the duration of asynchronous call
(for example, this allows to allocate DR_ASYNC_INFO structure on stack).
In contrast variable specified by pOperationState pointer is required to be available
for the duration of the asynchronous call.

--*/
typedef struct tagDR_ASYNC_INFO {
    SIZE_T          cbSize;

    DrError*        pOperationState;

    HANDLE          Event;

    HANDLE          IOCP;
    LPOVERLAPPED    pOverlapped;
    UINT_PTR        CompletionKey;

    UINT64          unused0;
    UINT64          unused1;
} DR_ASYNC_INFO, *PDR_ASYNC_INFO;
typedef const DR_ASYNC_INFO* PCDR_ASYNC_INFO;

//JC
#if 0
typedef struct tagDRSESSIONHANDLE
{
    ULONG_PTR       Unused;
} *DRSESSIONHANDLE, **PDRSESSIONHANDLE;

/*++

DR_ASYNC_INFO structure

Each function that may be executed asynchronously takes pointer
to DR_ASYNC_INFO structure as last parameter.

If NULL is passed then function completes in synchronous manner
and error code is returned as return value.

If parameter is not NULL then operation is carried on in asynchronous manner.
If asynchronous operation has been successfully started then function terminates
immediately with HRESULT_FROM_WIN32(ERROR_IO_PENDING) return value.
Any other return value indicates that it was impossible to start asynchronous operation.

Fields:

    cbSize          Size of structure in bytes. Set to sizeof(DR_ASYNC_INFO).

    pOperationState Pointer to error code returned by completed operation.
                    While operation is in progress value is set to HRESULT_FROM_WIN32(ERROR_IO_PENDING).
                    Before completion is reported value is set to an error code of completed operation.
                    Cannot be NULL.

    Event           Handle to event. Event is set once operation is completed. May be NULL.

    IOCP            Handle to IO completion port. If not NULL then upon completion status is posted
                    to specified completion port.

    pOverlapped     Pointer to OVERLAPPED structure. Used in conjunction with IOCP parameter
                    to post status to IOCP.
                    Should be null if IOCP is NULL, cannot be NULL if IOCP is not NULL.

    CompletionKey   Used in conjunction with IOCP parameter to post status to IOCP.
                    Should be 0 if IOCP is NULL.

    unusedX         Fields reserved for future use.

Note that DR_ASYNC_INFO structure is not required to be available for the duration of asynchronous call
(for example, this allows to allocate DR_ASYNC_INFO structure on stack).
In contrast variable specified by pOperationState pointer is required to be available
for the duration of the asynchronous call.

--*/
typedef struct tagDR_ASYNC_INFO {
    SIZE_T          cbSize;

    DrError*        pOperationState;

    HANDLE          Event;

    HANDLE          IOCP;
    LPOVERLAPPED    pOverlapped;
    UINT_PTR        CompletionKey;

    UINT64          unused0;
    UINT64          unused1;
} DR_ASYNC_INFO, *PDR_ASYNC_INFO;
typedef const DR_ASYNC_INFO* PCDR_ASYNC_INFO;


/*++

DR_INIT_PARAMS structure

Contains optional parameters for cosmos initialization.
Default value of all fields is zero.

Fields:

    cbSize                      Size of structure in bytes. Set to sizeof(DR_INIT_PARAMS).

    SuppressConsoleLogOutput    Set to TRUE to suppress autopilot init output to console.

--*/
typedef struct tagDR_INIT_PARAMS {
    SIZE_T          cbSize;

    BOOL            SuppressConsoleLogOutput;
    BOOL            fLogDisableMillisecondTimestamps;

    UINT64          Reserved0;
    UINT64          Reserved1;
    UINT64          Reserved2;
    UINT64          Reserved3;
    UINT64          Reserved4;
    UINT64          Reserved5;
    UINT64          Reserved6;
    UINT64          Reserved7;
} DR_INIT_PARAMS, *PDR_INIT_PARAMS;
typedef const DR_INIT_PARAMS* PCDR_INIT_PARAMS;


/*++

DR_STREAM_PROPERTIES structure

Used in DrOpenStream and DrSetStreamProperties.

Fields:

    cbSize          Size of structure in bytes. Set to sizeof(DR_STREAM_PROPERTIES).

    ExpirePeriod    Expiration period in 100-ns intervals. On server side at the time
                    of request execution stream expiration time is set to curent time
                    plus ExpirePeriod.

    ReadOffsetHint  Optimization hint. Currently not used.

    Flags           Specifies new values of modified stream flags.
                    Set to 0 for DrOpenStream.

    FlagsMask       Specifies stream flags to be modified.
                    Set to 0 for DrOpenStream.

--*/
typedef struct tagDR_STREAM_PROPERTIES {
    SIZE_T          cbSize;
    INT64           ExpirePeriod;
    UINT64          ReadOffsetHint;
    UINT            Flags;
    UINT            FlagsMask;
    UINT64          unused0;            // four attributes reserved for future extension
    UINT64          unused1;
    UINT64          unused2;
    UINT64          unused3;
} DR_STREAM_PROPERTIES, *PDR_STREAM_PROPERTIES;
typedef const DR_STREAM_PROPERTIES* PCDR_STREAM_PROPERTIES;


/*++

    Old name for DR_STREAM_PROPERTIES structure

--*/
typedef DR_STREAM_PROPERTIES DR_STREAM_PARAMS;
typedef PDR_STREAM_PROPERTIES PDR_STREAM_PARAMS;
typedef PCDR_STREAM_PROPERTIES PCDR_STREAM_PARAMS;


typedef struct tagDR_EXTENT_INSTANCE {
    SIZE_T          cbSize;

    UINT            Flags;
    PCSTR           ExtentNodeName;
    UINT64          unused0;            // two attributes reserved for future extension
    UINT64          unused1;
} DR_EXTENT_INSTANCE, *PDR_EXTENT_INSTANCE;


typedef struct tagDR_EXTENT {
    SIZE_T              cbSize;

    UINT                Flags;

    UINT64              ModificationTime;

    UINT64              Length;

    UINT64              Crc64;

    GUID                Id;

    UINT                NumberOfInstances;
    PDR_EXTENT_INSTANCE Instances;
    UINT64              unused0;        // three attributes reserved for future extension
    UINT64              unused1;
    UINT64              unused2;
    UINT64              unused3;
} DR_EXTENT, *PDR_EXTENT;

/*++
Fields:

    Flags       DR_OBJECT_STREAM or DR_OBJECT_DIRECTORY
--*/
typedef struct tagDR_STREAM {
    SIZE_T          cbSize;

    PCSTR           pName;

    UINT            Flags;

    UINT64          CreationTime;
    UINT64          ExpirationTime;
    UINT64          ModificationTime;

    UINT64          Length;
    UINT            TotalNumberOfExtents;

    GUID            Id;

    UINT            StartExtentIndex;
    UINT64          StartExtentOffset;
    UINT            ExtentsCount;
    PDR_EXTENT      pExtents;
    PCSTR           pPath;
    UINT64          unused0;              // four attributes reserved for future extension
    UINT64          unused1;
    UINT64          unused2;
    UINT64          unused3;
} DR_STREAM, *PDR_STREAM;


typedef struct tagDR_DIRECTORY {
    SIZE_T          cbSize;

    PDR_STREAM      pStreams;
    SIZE_T          StreamsCount;
    UINT64          unused0;              // four attributes reserved for future extension
    UINT64          unused1;
    UINT64          unused2;
    UINT64          unused3;
} DR_DIRECTORY, *PDR_DIRECTORY;


typedef struct tagDR_EXTENT_NODE {
    SIZE_T          cbSize;

    UINT            ExtentNodeVersion;
    PCSTR           pExtentNodeVersionText;

    PCSTR           ExtentNodeName;
    UINT            Flags;
    UINT64          StartupTime;
    UINT64          LastSyncTime;
    PCSTR           ReasonText;

    UINT            NumberOfSealedExtents;
    UINT64          SealedExtentsLength;
    UINT64          SealedExtentsSizeOnDisk;

    UINT            NumberOfUnsealedExtents;
    UINT64          UnsealedExtentsLength;
    UINT64          UnsealedExtentsSizeOnDisk;

    UINT            NumberOfUnvacatedExtents;
    UINT64          UnvacatedExtentsLength;

    UINT64          FreeSpaceSize;

    UINT16          ScaleUnit;
    UINT64          unused0;              // four attributes reserved for future extension
    UINT64          unused1;
    UINT64          unused2;
    UINT64          unused3;
} DR_EXTENT_NODE, *PDR_EXTENT_NODE;


typedef struct tagDR_HOST {
    PCSTR           HostName;
    UINT16          Port;
    PCSTR           PodName;
    UINT16          ScaleUnit;
} DR_HOST, *PDR_HOST;


typedef struct tagDR_VOLUME {
    SIZE_T          cbSize;

    UINT            DrmVersion;
    PCSTR           pDrmVersionText;

    UINT64          CurrentTime;
    UINT64          StartupTime;

    UINT64          NumberOfStreams;
    UINT64          StreamsLength;
    UINT64          NumberOfCreatedStreams;

    UINT64          NumberOfSealedExtents;
    UINT64          SealedExtentsLength;

    UINT64          NumberOfUnsealedExtents;
    UINT64          UnsealedExtentsLength;

    PCSTR           pPrimaryDRM;
    UINT            NumberOfDrmHosts;
    PDR_HOST        pDrmHostList;
    UINT64          DrmRslSequence;
    GUID            VolumeId;

    UINT64          NumberOfCreatedExtents;
    UINT            NumberOfExtentNodes;
    PDR_EXTENT_NODE pExtentNodes;
    UINT64          unused0;              // four attributes reserved for future extension
    UINT64          unused1;
    UINT64          unused2;
    UINT64          unused3;
} DR_VOLUME, *PDR_VOLUME;


typedef struct tagDR_STREAM_POSITION {
    UINT    ExtentIndex;
    UINT64  Offset;
} DR_STREAM_POSITION, *PDR_STREAM_POSITION;
typedef const DR_STREAM_POSITION* PCDR_STREAM_POSITION;

/*++

DR_MULTIMODIFY_EXTENT_RANGE structure

Used in DrMultiModifyStream to describe a contiguous range of extents to
be concatenated onto a target stream.

Fields:

    cbSize           Size of structure in bytes. Set to sizeof(DR_MULTIMODIFY_EXTENT_RANGE).

    MultiModifyStreamEntryIndex
                        The 0-based index of the DR_MULTIMODIFY_STREAM_ENTRY, provided in the call to DrMultiModifyStream, which describes
                        the source stream from which to copy extents.

    ExtentIndex
                        The index in the source stream of the first extent to be copied. 0 means beginning of stream.

    ExtentCount
                        The number of contiguous extents in the range.
                        If 0, no extents will be copied, but the operation will succeed.
                        If DR_ALL_EXTENTS, all remaining extents in the source stream will be concatenated (may be 0).
--*/

typedef struct tagDR_MULTIMODIFY_EXTENT_RANGE {
    SIZE_T      cbSize;
    UINT        MultiModifyStreamEntryIndex;
    UINT        ExtentIndex;
    UINT        ExtentCount;
} DR_MULTIMODIFY_EXTENT_RANGE, *PDR_MULTIMODIFY_EXTENT_RANGE;
typedef const DR_MULTIMODIFY_EXTENT_RANGE *PCDR_MULTIMODIFY_EXTENT_RANGE;

/*++

DR_MULTIMODIFY_STREAM_ENTRY structure

Used in DrMultiModifyStream to describe one of a set of
atomically applied stream modifications.

Combines the capabilities of DrDeleteStream, DrRenameStream, DrConcatenateStream, and
DrSetStreamProperties, with atomic compare/exchange semantics.

Note that it is explicitly possible to construct an entry that verifies the existence or size of a particular
stream without actually making any changes to the stream. This is particularly
useful when concatenating append ranges, since each source stream must be described
by a DR_MULTIMODIFY_STREAM_ENTRY even if it is not to be touched.

If this structure is set to all zeroes, and only pOldStream is initialized, the effect will be to
verify existence of stream pOldStream, but make no changes to it.

Fields:

    cbSize              Size of structure in bytes. Set to sizeof(DR_MULTIMODIFY_ENTRY).

    ModifyFlags         Options for the entry. Includes:
                        DR_MULTIMODIFY_ENTRY_SET_EXPIRE_PERIOD - the expire time is to be updated from pStreamProperties
                        DR_MULTIMODIFY_ENTRY_DELETE_STREAM -- the stream is to be deleted or recycled according to deleteFlags
                        DR_MULTIMODIFY_ENTRY_ENFORCE_EXTENT_COUNT -- fail the request with DrError_StreamChanged if requiredExtentCount does not match
                        DR_MULTIMODIFY_ENTRY_CREATE_STREAM  -- Create the new stream names in pNewStreamName

    pOldStreamName
                        Fully qualified stream existing to be verified or modified.

    pNewStreamName
                        If renaming or creating a new stream, the new fully qualified stream name. NULL otherwise. Must
                        be on the same volume.

    DeleteFlags
                        If deleting (DR_MULTIMODIFY_ENTRY_DELETE_STREAM), options for the delete (See DrDeleteStream). Must be
                        0 if not deleting.

    pStreamProperties
                        If setting stream properties (including updating expire time), the new
                        stream properties. NULL otherwise. See DrSetStreamProperties.

    RequiredStreamId
                        if not NULL_GUID, causes the entire request to fail with DrError_StreamChanged
                        if the stream does not have the specified stream ID.

    RequiredExtentCount
                        if DR_MULTIMODIFY_ENTRY_ENFORCE_EXTENT_COUNT is 1, causes the request to fail with DrError_StreamChanged if
                        the stream does not contain the specified number of extents (prior to conconcatenation).

    ConcatenateExtentRangeCount
                        The number of entries in pConcatenateExtentRanges. 0 If no concatenation is to be performed.

    pConcatenateExtentRanges
                        An array of extent range descriptors of length ConcatenateExtentRangeCount. Each entry describes a contiguous block
                        of extents to be appended onto the end of this stream, in order. NULL if no concatenation is to be performed.
                        See DrConcatenateStream.
--*/
typedef struct tagDR_MULTIMODIFY_STREAM_ENTRY {
    SIZE_T                      cbSize;
    UINT                        ModifyFlags;

    PCSTR                       pOldStreamName;
    PCSTR                       pNewStreamName;
    UINT                        DeleteFlags;

    PCDR_STREAM_PROPERTIES      pStreamProperties;
    GUID                        RequiredStreamId;
    UINT                        RequiredExtentCount;
    UINT                        ConcatenateExtentRangeCount;
    PCDR_MULTIMODIFY_EXTENT_RANGE pConcatenateExtentRanges;

    UINT64                      unused0;        // four attributes reserved for future extension
    UINT64                      unused1;
    UINT64                      unused2;
    UINT64                      unused3;
} DR_MULTIMODIFY_STREAM_ENTRY, *PDR_MULTIMODIFY_STREAM_ENTRY;
typedef const DR_MULTIMODIFY_STREAM_ENTRY *PCDR_MULTIMODIFY_STREAM_ENTRY;

//
// MultiModifyStream Entry flags
//

//
// DR_MULTIMODIFY_ENTRY_SET_EXPIRE_PERIOD  If 1, stream's expiration period will be reset from pStreamProperties
// Append, concatenate, delete, rename and shrink expiration time operations
// fail for such streams.
//
#define DR_MULTIMODIFY_ENTRY_SET_EXPIRE_PERIOD      0x00000001u

//
// DR_MULTIMODIFY_ENTRY_DELETE_STREAM  If 1, stream will be deleted according to deleteFlags
//
#define DR_MULTIMODIFY_ENTRY_DELETE_STREAM          0x00000002u

//
// DR_MULTIMODIFY_ENTRY_ENFORCE_EXTENT_COUNT  If 1, request will fail with DrError_StreamChanged if requiredExtentCount does not match current stream size
//
#define DR_MULTIMODIFY_ENTRY_ENFORCE_EXTENT_COUNT   0x00000004u

//
// DR_MULTIMODIFY_ENTRY_CREATE_STREAM  If 1, creates stream pNewStreamName. pOldStreamName should be NULL.
//
#define DR_MULTIMODIFY_ENTRY_CREATE_STREAM          0x00000008u


typedef struct tagDR_RANGE
{
    PCSTR   pStreamName;
    UINT    ExtentIndex;
    UINT    ExtentCount;
} DR_RANGE;
typedef DR_RANGE* PDR_RANGE;
typedef const DR_RANGE* PCDR_RANGE;


#define DR_CURRENT_VERSION      0x00000009u

//
// Stream open modes
//
#define DR_APPEND               0x00000001u
#define DR_READ                 0x00000002u

//
// Creation disposition
//
#define DR_OPEN                 0x00000010u
#define DR_CREATE               0x00000020u
#define DR_CREATE_OR_OPEN       (DR_OPEN | DR_CREATE)

//
// Metadata refresh policy flags for DrOpenStream and DrReadStream
// Refresh policy is set during DrOpenStream, and then may be overridden by DrReadStream
// If DrReadStream does not override, options from DrOpenStream affect DrReadStream as well
//
#define DR_REFRESH_AGGRESSIVE   0x10000000u
#define DR_REFRESH_PASSIVE      0x20000000u
#define DR_REFRESH_FROM_CACHE   0x30000000u
#define DR_REFRESH_NO_INSTANCES 0x40000000u   // permit to refresh instances (length can be refreshed always)

#define DR_REFRESH_MASK                (DR_REFRESH_AGGRESSIVE | DR_REFRESH_PASSIVE | DR_REFRESH_NO_INSTANCES) // all refresh bits
#define DR_REFRESH_MASK_MUST_HAVE      (DR_REFRESH_AGGRESSIVE | DR_REFRESH_PASSIVE) // at least one of these bits must be on
#define DR_REFRESH_DEFAULT             (DR_REFRESH_AGGRESSIVE) // default for DrOpenStream
#define DR_REFRESH_DEFAULT_STREAMINFO  (DR_REFRESH_AGGRESSIVE | DR_REFRESH_NO_INSTANCES) // default for DrOpenStreamFromStreamInfo

//
// Cache policy settings for DrOpenStream (possible future TODO: allow to be passed directly to DrReadStream)
//
#define DR_CACHE_SEQUENTIAL     0x01000000u
#define DR_CACHE_READ_AHEAD     0x02000000u   // same as DR_CACHE_SEQUENTIAL, but with optimistic read ahead in background
#define DR_CACHE_RANDOM_ACCESS  0x03000000u
#define DR_CACHE_NO_CACHE       0x04000000u   // ineffective if read boundaries do not exactly match append boundaries

#define DR_CACHE_DEFAULT        DR_CACHE_SEQUENTIAL
#define DR_CACHE_MASK           0x07000000u

//
// Object selection options
//
#define DR_OBJECT_STREAM        0x00000100u
#define DR_OBJECT_DIRECTORY     0x00000200u
#define DR_OBJECT_ANY           (DR_OBJECT_STREAM | DR_OBJECT_DIRECTORY)
#define DR_OBJECT_LOCAL         0x00000400u
#define DR_OBJECT_RECYCLED      0x00000800u

//
// Stream flags
//

//
// DR_STREAM_READ_ONLY  Stream is read-only.
// Append, concatenate, delete, rename and shrink expiration time operations
// fail for such streams.
//
#define DR_STREAM_READ_ONLY     0x00000008u

//
// DR_STREAM_SEALED  Stream is sealed.
// Append and concatenate operations
// fail for such streams. This bit cannot be cleared once set. Does not effect
// append operations to unsealed extents within the stream.
//
#define DR_STREAM_SEALED         0x00000010u

//
// Extent flags
//
#define DR_EXTENT_SEALED        0x00001000u
#define DR_EXTENT_NEEDS_SEAL    0x00002000u

//
// Extent node flags
//
#define DR_EXTENT_NODE_AVAILABLE    0x00000001u

//
// Stream append options
//
#define DR_FIXED_OFFSET_APPEND  0x01000000u
#define DR_PARTIAL_APPEND       0x02000000u
#define DR_SEAL                 0x04000000u
#define DR_UPDATE_DRM           0x08000000u

//
// Stream delete options
//
#define DR_DELETE_FORCE         0x00000001u

//
// Stream concatenation options
//
#define DR_CONCATENATE_ALLOW_CREATE_NEW     0x00100000u
#define DR_CONCATENATE_REQUIRE_CREATE_NEW   0x00200000u

#define DR_CONCATENATE_MASK                 (DR_CONCATENATE_ALLOW_CREATE_NEW | DR_CONCATENATE_REQUIRE_CREATE_NEW)

//
// Retrieve or append all extents
//
#define DR_ALL_EXTENTS          0xFFFFFFFFu

#define DR_INVALID_EXTENT_INDEX 0xFFFFFFFFu
#define DR_UNKNOWN_EXTENT_INDEX 0xFFFFFFFFu

#define DR_UNKNOWN_OFFSET       0xFFFFFFFFFFFFFFFFui64
#define DR_UNKNOWN_LENGTH       0xFFFFFFFFFFFFFFFFui64

#define DR_NEVER                0xFFFFFFFFFFFFFFFFui64
#define DR_INFINITE             0x7FFFFFFFFFFFFFFFi64

//
// Stream renaming flags
//
#define DR_RENAME_ALLOW_SUBSTITUTION    0x00000001u
#define DR_RENAME_REQUIRE_SUBSTITUTION  0x00000002u

#define DR_RENAME_MASK                  (DR_RENAME_ALLOW_SUBSTITUTION | DR_RENAME_REQUIRE_SUBSTITUTION)

//
// Get position flags
//
#define DR_ABSOLUTE_POSITION 0x00000001u

//
// Stream option constants
//

//
// DR_STREAM_OPTION_MAX_EXTENT_SIZE             UINT64
// Maximum extent size client passes to DRM and EN,
// if DRM returned smaller maximum size, the smaller
// size is used to pass to EN.
// Pass zero to use built-in default (DR_STREAM_OPTION_MAX_EXTENT_SIZE_FALLBACK).
// Set to value from the configuration file if there is any.
//
#define DR_STREAM_OPTION_MAX_EXTENT_SIZE                    1u
#define DR_STREAM_OPTION_MAX_EXTENT_SIZE_DEFAULT            0ui64
#define DR_STREAM_OPTION_MAX_EXTENT_SIZE_FALLBACK           0x6400000ui64 // 100 Mb
#define DR_STREAM_OPTION_MAX_EXTENT_SIZE_CONFIG             "ExtentSize"

//
// DR_STREAM_OPTION_MAX_PHYSICAL_EXTENT_SIZE    UINT64
// Maximum physical (compressed) extent size client passes to EN.
// Pass zero to use built-in default (DR_STREAM_OPTION_MAX_PHYSICAL_EXTENT_SIZE_FALLBACK).
// Set to value from the configuration file if there is any.
//
#define DR_STREAM_OPTION_MAX_PHYSICAL_EXTENT_SIZE           2u
#define DR_STREAM_OPTION_MAX_PHYSICAL_EXTENT_SIZE_DEFAULT   0ui64
#define DR_STREAM_OPTION_MAX_PHYSICAL_EXTENT_SIZE_FALLBACK  0xFFFFFFFFFFFFFFFFui64 // unlimited
#define DR_STREAM_OPTION_MAX_PHYSICAL_EXTENT_SIZE_CONFIG    "PhysicalExtentSize"


//
// DR_STREAM_OPTION_COMPRESSION_LEVEL           UINT
// Specifies compression level for data being appended.
// Value from 0 (fastest, no compression) to 5 (slowest, best compression ration).
// Set to value from the configuration file if there is any.
//
#define DR_STREAM_OPTION_COMPRESSION_LEVEL                  3u
#define DR_STREAM_OPTION_COMPRESSION_LEVEL_DEFAULT          2u
#define DR_STREAM_OPTION_COMPRESSION_LEVEL_MIN              0u
#define DR_STREAM_OPTION_COMPRESSION_LEVEL_MAX              5u
#define DR_STREAM_OPTION_COMPRESSION_LEVEL_CONFIG           "CompressionLevel"


//
// DR_STREAM_OPTION_MAX_APPEND_RETRY            UINT
// Specifies number of retries for unsuccessful stream append.
// Set to value from the configuration file if there is any.
//
#define DR_STREAM_OPTION_MAX_APPEND_RETRY                   4u
#define DR_STREAM_OPTION_MAX_APPEND_RETRY_DEFAULT           16u
#define DR_STREAM_OPTION_MAX_APPEND_RETRY_MIN               1u
#define DR_STREAM_OPTION_MAX_APPEND_RETRY_MAX               1023u
#define DR_STREAM_OPTION_MAX_APPEND_RETRY_CONFIG            "AppendRetry"


//
// DR_STREAM_OPTION_MAX_READ_RETRY              UINT
// Specifies number of retries for unsuccessful stream read.
// Set to value from the configuration file if there is any.
//
#define DR_STREAM_OPTION_MAX_READ_RETRY                     5u
#define DR_STREAM_OPTION_MAX_READ_RETRY_DEFAULT             16u
#define DR_STREAM_OPTION_MAX_READ_RETRY_MIN                 1u
#define DR_STREAM_OPTION_MAX_READ_RETRY_MAX                 1023u
#define DR_STREAM_OPTION_MAX_READ_RETRY_CONFIG              "ReadRetry"


//
// DR_STREAM_OPTION_FLAGS                       UINT
// Contains flags value for given handle.
// Read-only.
//
#define DR_STREAM_OPTION_FLAGS                              6u


//
// DR_STREAM_OPTION_MAX_APPEND_SIZE             UINT64
// Maxim block size for single append.
// Set to value from the configuration file if there is any.
//
#define DR_STREAM_OPTION_MAX_APPEND_SIZE                    7u
#define DR_STREAM_OPTION_MAX_APPEND_SIZE_DEFAULT            0x400000ui64 // 4 Mb
#define DR_STREAM_OPTION_MAX_APPEND_SIZE_MIN                1ui64
#define DR_STREAM_OPTION_MAX_APPEND_SIZE_MAX                0x2000000ui64 // 32 Mb
#define DR_STREAM_OPTION_MAX_APPEND_SIZE_CONFIG             "MaxAppendSize"


//
// DR_STREAM_OPTION_VERIFY_COMPRESSION          BOOL
// Set to TRUE to enable additional pass to verify compressed data
// for corruption on append.
// Should be either TRUE or FALSE.
// Set to value from the configuration file if there is any.
//
#define DR_STREAM_OPTION_VERIFY_COMPRESSION                 8u
#define DR_STREAM_OPTION_VERIFY_COMPRESSION_DEFAULT         FALSE
#define DR_STREAM_OPTION_VERIFY_COMPRESSION_CONFIG          "VerifyCompression"


//
// DR_STREAM_OPTION_GUID                        GUID
// Returns stream GUID for given handle.
// Read-only.
//
#define DR_STREAM_OPTION_GUID                               9u


//
// DR_STREAM_OPTION_SHORT_NAME                  String
// Returns stream name without path for given handle.
// Read-only.
//
#define DR_STREAM_OPTION_SHORT_NAME                         10u


//
// DR_STREAM_OPTION_PATH                        String
// Returns stream path for given handle.
// Read-only.
//
#define DR_STREAM_OPTION_PATH                               11u


//
// DR_STREAM_OPTION_VOLUME                      String
// Returns stream volume name for given handle.
// Read-only.
//
#define DR_STREAM_OPTION_VOLUME                             12u


//
// DR_STREAM_OPTION_CLUSTER                     String
// Returns stream cluster name for given handle.
// Read-only.
//
#define DR_STREAM_OPTION_CLUSTER                            13u


//
// DR_STREAM_OPTION_NAME                        String
// Returns complete stream name in URI form for given handle.
// Read-only.
//
#define DR_STREAM_OPTION_NAME                               14u


//
// DR_STREAM_OPTION_GUID_PATH                   String
// Returns .streamid stream path for given handle.
// Read-only.
//
#define DR_STREAM_OPTION_GUID_PATH                          15u


//
// DR_STREAM_OPTION_GUID_URI                    String
// Returns .streamid stream name in URI form for given handle.
// Read-only.
//
#define DR_STREAM_OPTION_GUID_URI                           16u


//
// DR_STREAM_OPTION_NAME_GUID_PATH              String
// Returns combined .streamid stream path and namespace location for given handle.
// Read-only.
//
#define DR_STREAM_OPTION_NAME_GUID_PATH                     17u


//
// DR_STREAM_OPTION_NAME_GUID_URI               String
// Returns combined .streamid stream name and namespace location in URI form for given handle.
// Read-only.
//
#define DR_STREAM_OPTION_NAME_GUID_URI                      18u

//
// DR_STREAM_OPTION_MAX_CONCURRENT_READS        UINT
// Maximum number of concurrent read request for single DrReadStream call.
// If first read attempt does not complete in DR_STREAM_OPTION_CONCURRENT_READ_TIMEOUT_1_MS
// time, second read request for the same data in sent to the different storage node.
// DR_STREAM_OPTION_CONCURRENT_READ_TIMEOUT_2_MS timeout is used for the second 
// and subsequent read attempts. If this option is set to 1 no concurrent read requests are
// performed. While this option limits number of concurrent retries, 
// DR_STREAM_OPTION_MAX_READ_RETRY option limits total number of retries for single API call.
// Aggressive sittings for the maximum concurrent reads number and timeouts may result in
// increased network load and cause load convoy effects during peak loads.
// Set to value from the configuration file if there is any.
//
#define DR_STREAM_OPTION_MAX_CONCURRENT_READS               19u
#define DR_STREAM_OPTION_MAX_CONCURRENT_READS_DEFAULT       2u
#define DR_STREAM_OPTION_MAX_CONCURRENT_READS_MIN           1u
#define DR_STREAM_OPTION_MAX_CONCURRENT_READS_MAX           3u
#define DR_STREAM_OPTION_MAX_CONCURRENT_READS_CONFIG        "MaxConcurrentReads"

//
// DR_STREAM_OPTION_CONCURRENT_READ_TIMEOUT_1_MS        UINT
// Wait time in milliseconds for making second concurrent read request
// if first read request is not complete. Maximum number of concurrent read 
// requests is limited by DR_STREAM_OPTION_MAX_CONCURRENT_READS option.
// Set to value from the configuration file if there is any.
//
#define DR_STREAM_OPTION_CONCURRENT_READ_TIMEOUT_1_MS               20u
#define DR_STREAM_OPTION_CONCURRENT_READ_TIMEOUT_1_MS_DEFAULT       10000u
#define DR_STREAM_OPTION_CONCURRENT_READ_TIMEOUT_1_MS_MIN           0u
#define DR_STREAM_OPTION_CONCURRENT_READ_TIMEOUT_1_MS_MAX           60000u
#define DR_STREAM_OPTION_CONCURRENT_READ_TIMEOUT_1_MS_CONFIG        "ConcurrentReadTimeout1Ms"

//
// DR_STREAM_OPTION_CONCURRENT_READ_TIMEOUT_2_MS        UINT
// Wait time in milliseconds after starting second read request for making third concurrent 
// read request if either first or second read request is not complete. Maximum number of 
// concurrent read requests is limited by DR_STREAM_OPTION_MAX_CONCURRENT_READS option.
// Set to value from the configuration file if there is any.
//
#define DR_STREAM_OPTION_CONCURRENT_READ_TIMEOUT_2_MS               21u
#define DR_STREAM_OPTION_CONCURRENT_READ_TIMEOUT_2_MS_DEFAULT       10000u
#define DR_STREAM_OPTION_CONCURRENT_READ_TIMEOUT_2_MS_MIN           0u
#define DR_STREAM_OPTION_CONCURRENT_READ_TIMEOUT_2_MS_MAX           60000u
#define DR_STREAM_OPTION_CONCURRENT_READ_TIMEOUT_2_MS_CONFIG        "ConcurrentReadTimeout2Ms"

//
// DR_STREAM_OPTION_MAX_OUTSTANDING_APPENDS              UINT
// Maximum number of append requests sent to network at same time per stream.
// Set to value from the configuration file if there is any.
//
#define DR_STREAM_OPTION_MAX_OUTSTANDING_APPENDS                    22u
#define DR_STREAM_OPTION_MAX_OUTSTANDING_APPENDS_DEFAULT            32u
#define DR_STREAM_OPTION_MAX_OUTSTANDING_APPENDS_MIN                1u
#define DR_STREAM_OPTION_MAX_OUTSTANDING_APPENDS_MAX                128u
#define DR_STREAM_OPTION_MAX_OUTSTANDING_APPENDS_CONFIG             "MaxOutstandingAppends"

//
// DR_STREAM_OPTION_MAX_OUTSTANDING_APPEND_SIZE          UINT64
// Maximum size of data contained in append requests sent to network at same time per stream.
// Set to value from the configuration file if there is any.
//
#define DR_STREAM_OPTION_MAX_OUTSTANDING_APPEND_SIZE                23u
#define DR_STREAM_OPTION_MAX_OUTSTANDING_APPEND_SIZE_DEFAULT        0x2000000ui64 // 32 Mb
#define DR_STREAM_OPTION_MAX_OUTSTANDING_APPEND_SIZE_MIN            0x200000ui64  // 2 Mb
#define DR_STREAM_OPTION_MAX_OUTSTANDING_APPEND_SIZE_MAX            0x4000000ui64 // 64 Mb
#define DR_STREAM_OPTION_MAX_OUTSTANDING_APPEND_SIZE_CONFIG         "MaxOutstandingAppendSize"

//
// DR_STREAM_OPTION_READ_AHEAD_BLOCK_SIZE                UINT64
// Read block size to be used if handle is opened with DR_CACHE_READ_AHEAD flag.
// Set to value from the configuration file if there is any.
//
#define DR_STREAM_OPTION_READ_AHEAD_BLOCK_SIZE                      24u
#define DR_STREAM_OPTION_READ_AHEAD_BLOCK_SIZE_DEFAULT              0x200000ui64  // 2 Mb
#define DR_STREAM_OPTION_READ_AHEAD_BLOCK_SIZE_MIN                  0x4000ui64    // 16 Kb
#define DR_STREAM_OPTION_READ_AHEAD_BLOCK_SIZE_MAX                  0x2000000ui64 // 32 Mb
#define DR_STREAM_OPTION_READ_AHEAD_BLOCK_SIZE_CONFIG               "ReadAheadBlockSize"

//
// DR_STREAM_OPTION_READ_AHEAD_OUTSTANDING_COUNT         UINT
// Maximum of simultaneous optimistic read requests to issue for handles opened with DR_CACHE_READ_AHEAD flag.
// Set to value from the configuration file if there is any.
//
#define DR_STREAM_OPTION_READ_AHEAD_OUTSTANDING_COUNT               25u
#define DR_STREAM_OPTION_READ_AHEAD_OUTSTANDING_COUNT_DEFAULT       8u
#define DR_STREAM_OPTION_READ_AHEAD_OUTSTANDING_COUNT_MIN           0u 
#define DR_STREAM_OPTION_READ_AHEAD_OUTSTANDING_COUNT_MAX           32u
#define DR_STREAM_OPTION_READ_AHEAD_OUTSTANDING_COUNT_CONFIG        "ReadAheadOutstandingCount"

//
// DR_STREAM_OPTION_CACHE_DEFAULT_MODE                    UINT
// Default cache operation mode for reads.
// Set to value from the configuration file if there is any.
// NOTE: this is global-only setting and it can't be changed for individual handles.
//
#define DR_STREAM_OPTION_CACHE_DEFAULT_MODE                         26u
#define DR_STREAM_OPTION_CACHE_DEFAULT_MODE_DEFAULT                 DR_CACHE_DEFAULT
#define DR_STREAM_OPTION_CACHE_DEFAULT_MODE_MIN                     DR_CACHE_SEQUENTIAL
#define DR_STREAM_OPTION_CACHE_DEFAULT_MODE_MAX                     DR_CACHE_NO_CACHE
#define DR_STREAM_OPTION_CACHE_DEFAULT_MODE_MASK                    DR_CACHE_MASK 
#define DR_STREAM_OPTION_CACHE_DEFAULT_MODE_CONFIG                  "CacheDefaultMode"

//
// DR_STREAM_OPTION_CACHE_SOFT_SIZE_LIMIT                 UINT64
// Soft limit for cache size measured in bytes of uncompressed data.
// After soft limit is reached sequential and read-ahead blocks are discarded
// immediately after use while random-read blocks are kept up to hard limit.
// Set to value from the configuration file if there is any.
// NOTE: this is global-only setting and it can't be changed for individual handles.
//
#define DR_STREAM_OPTION_CACHE_SOFT_SIZE_LIMIT                      27u
#define DR_STREAM_OPTION_CACHE_SOFT_SIZE_LIMIT_DEFAULT              0x10000000ui64  // 256 Mb
#define DR_STREAM_OPTION_CACHE_SOFT_SIZE_LIMIT_MIN                  0x0ui64
#define DR_STREAM_OPTION_CACHE_SOFT_SIZE_LIMIT_MAX                  MAX_UINT64
#define DR_STREAM_OPTION_CACHE_SOFT_SIZE_LIMIT_CONFIG               "CacheSoftSizeLimit"

//
// DR_STREAM_OPTION_CACHE_HARD_SIZE_LIMIT                 UINT64
// Hard limit for cache size measured in bytes of uncompressed data.
// Set to value from the configuration file if there is any.
// NOTE: this is global-only setting and it can't be changed for individual handles.
//
#define DR_STREAM_OPTION_CACHE_HARD_SIZE_LIMIT                      28u
#define DR_STREAM_OPTION_CACHE_HARD_SIZE_LIMIT_DEFAULT              0x20000000ui64  // 512 Mb
#define DR_STREAM_OPTION_CACHE_HARD_SIZE_LIMIT_MIN                  0x0ui64
#define DR_STREAM_OPTION_CACHE_HARD_SIZE_LIMIT_MAX                  MAX_UINT64
#define DR_STREAM_OPTION_CACHE_HARD_SIZE_LIMIT_CONFIG               "CacheHardSizeLimit"


//
// Init APIs
//

/*++

DrInitialize

This function behaves like DrInitializeEx with pInitParams set to NULL.
See description of DrInitializeEx below.

--*/
DRYADAPI_EXT
DrError
DRYADAPI
DrInitialize(
    IN  UINT    Version,
    IN  PCSTR   pDryadIniFile
    );


/*++

DrInitialize

Parameters:

    Version         Requested version number of the library. Pass DR_CURRENT_VERISON.

    pDryadIniFile  Pointer to string containing path and file name for cosmos ini file.
                    Pass NULL to use default. Default is cosmos.ini in current directory.

    pInitParams     Pointer to parametes block. May be NULL.

--*/
DRYADAPI_EXT
DrError
DRYADAPI
DrInitializeEx(
    IN  UINT                Version,
    IN  PCSTR               pDryadIniFile,
    IN  PCDR_INIT_PARAMS    pInitParams
    );


DRYADAPI_EXT
void
DRYADAPI
DrLogPreInitialize (
    void
);


DRYADAPI_EXT
DrError
DRYADAPI
DrUninitialize( void );


//
// Free for data structures allocated by Dryad client lib
//

DRYADAPI_EXT
DrError
DRYADAPI
DrFreeMemory(
    IN  PVOID           ptr
    );


/*++

DrOpenStream

Opens or creates cosmos or local file system stream.

Parameters:

    pStreamName     UTF8 encoded fully qualified cosmos URI, file URI, UNC name, or local file name.
                    Cannot be NULL.

    Flags           Specifies if stream should be open or created and open mode.
                    Combines next values:

                        DR_APPEND               open for append   | exclusive, required
                        DR_READ                 open for read     |
                                                (one of above should be specified)

                        DR_OPEN                 open existing stream     | can be combined, required
                        DR_CREATE               create new               |
                        DR_CREATE_OR_OPEN       create or open existing
                                                (defined as DR_OPEN | DR_CREATE)
                                                (at least one of above should be specified)

                        DR_REFRESH_AGGRESSIVE   (default) Refresh metadata from DRM if EOS  | exclusive, optional
                                                is detected.                                |
                        DR_REFRESH_PASSIVE      Do not refresh metadata from DRM if EOS     |
                                                is detected.                                |
                                                (refresh flags are optional)
                        DR_REFRESH_NO_INSTANCES Do not refresh instances in case of read error


                        DR_CACHE_SEQUENTIAL     (default) Optimize data cache for           | exclusive, optional
                                                sequential access. Reading same item twice  |
                                                is not efficient.                           |
                        DR_CACHE_READ_AHEAD     Optimize data cache for                     |
                                                sequential access and enable optimistic     |
                                                background read-ahead.                      |
                        DR_CACHE_RANDOM_ACCESS  Optimize data cache for random              |
                                                access. Reading same items twice is         |
                                                efficient.                                  |
                        DR_CACHE_NO_CACHE       Do not cache data. Consumes least memory.   |
                                                Could lead to data reread if read boundaries|
                                                do not exactly match append boundaries.     |
                                                (cache flags are optional)

    pStreamParams   Pointer to DR_STREAM_PROPERTIES structure. DR_STREAM_PROPERTIES structure
                    contains additional optional parameters.

    pStreamHandle   Pointer to variable that receives handle for opened stream.
                    NOTE: if operation is asynchronous, this location should be valid
                    until completion of operation is reported.

    pAsyncInfo      Pointer to DR_ASYNC_INFO structure.
                    See description of to DR_ASYNC_INFO structure.

Examples:

// create new or open existing cosmos stream for append synchronously
DRHANDLE h;
DrError ret = DrOpenStream( "cosmos://store/volume/dir1/dir2/stream1.ext",
                DR_CREATE_OR_OPEN | DR_APPEND, NULL, &h, NULL );

// asyncronously open existing stream for read
// use event for completion notification
DrError ret;
DRHANDLE h;
DrError asyncRet;
DR_ASYNC_INFO asyncInfo;
ZeroMemory( &asyncInfo, sizeof( asyncInfo ) );
asyncInfo.cbSize = sizeof( asyncInfo );
asyncInfo.pOperationState = &asyncRet;
asyncInfo.Event = CreateEvent( NULL, TRUE, FALSE, NULL );
ret = DrOpenStream( "cosmos://store/volume/dir1/dir2/stream2.ext",
                DR_OPEN | DR_READ, NULL, &h, &asyncInfo );
if ( ret == HRESULT_FROM_WIN32( ERROR_IO_PENDING ) ) {
    WaitForSingleObject( asyncInfo.Event, INFINITE );
    if ( SUCCEEDED( asyncRet ) ) {
        // Open completed successfully
    }
}

--*/
DRYADAPI_EXT
DrError
DRYADAPI
DrOpenStream(
    IN  PCSTR               pStreamName,
    IN  UINT                Flags,
    IN  PCDR_STREAM_PROPERTIES  pStreamParams,
    OUT PDRHANDLE           pStreamHandle,
    IN  PCDR_ASYNC_INFO     pAsyncInfo
    );

//
// Extended version of open stream which accept a session handle as parameter
//

DRYADAPI_EXT
DrError
DRYADAPI
DrOpenStreamEx(
    IN  DRSESSIONHANDLE     Session,
    IN  PCSTR               pStreamName,
    IN  UINT                Flags,
    IN  PCDR_STREAM_PROPERTIES  pStreamParams,
    OUT PDRHANDLE           pStreamHandle,
    IN  PCDR_ASYNC_INFO     pAsyncInfo
    );

/*++

DrOpenStreamFromStreamInfo

Binary verion of open stream: opening based on DR_STREAM 
Data structure has the same format as the data returned by DrGetStreamInformation
Upon return from DrOpenStreamFromStreamInfo that data structure may be freed.

Parameters:

    pStreamName     UTF8 encoded fully qualified cosmos URI, file URI, UNC name, or local file name.
                    Cannot be NULL.

    pStreamInfo     Pointer to DR_STREAM structure. Cannot be NULL

    Flags           Same as for DrOpenStream, except for DR_CREATE, which is unacceptable in this API
                    Note: DR_NO_REFRESH_INSTANCES is set by default (if no DR_REFRESH_ is provided)


    pStreamHandle   Pointer to variable that receives handle for opened stream.
                    NOTE: if operation is asynchronous, this location should be valid
                    until completion of operation is reported.

    pReserved       Reserved. Must be zero.

    pAsyncInfo      Pointer to DR_ASYNC_INFO structure.
                    See description of to DR_ASYNC_INFO structure.

Example:

DrError ret;

// retrieve streaminfo
PDR_STREAM streaminfo;
ret = DrGetStreamInformation("cosmos://store/volume/dir1/dir2/stream2.ext",
                             0, 0, DR_ALL_EXTENTS, &streaminfo, NULL);

// asyncronously open existing stream for read
// use event for completion notification
DRHANDLE h;
DrError asyncRet;
DR_ASYNC_INFO asyncInfo;
ZeroMemory( &asyncInfo, sizeof( asyncInfo ) );
asyncInfo.cbSize = sizeof( asyncInfo );
asyncInfo.pOperationState = &asyncRet;
asyncInfo.Event = CreateEvent( NULL, TRUE, FALSE, NULL );
ret = DrOpenStreamFromStreamInfo (NULL, "cosmos://store/volume/dir1/dir2/stream2.ext",
                                  streaminfo, DR_OPEN | DR_READ, NULL, &h, &asyncInfo);
if ( ret == HRESULT_FROM_WIN32( ERROR_IO_PENDING ) ) {
    WaitForSingleObject( asyncInfo.Event, INFINITE );
    if ( SUCCEEDED( asyncRet ) ) {
        // Open completed successfully
    }
}

// release stream info
DrFreeMemory (streaminfo);


--*/
DRYADAPI_EXT
DrError
DRYADAPI
DrOpenStreamFromStreamInfo (
    IN  DRSESSIONHANDLE     Session,          // may be NULL
    IN  PCSTR               pStreamName,
    IN  PDR_STREAM          pStreamInfo,
    IN  UINT                Flags,
    OUT PDRHANDLE           pStreamHandle,
    IN  PVOID               pReserved,        // for future extensions, currently NULL
    IN  PCDR_ASYNC_INFO     pAsyncInfo
    );

/*++
DrOpenSession

Remarks:

    This API lets user to create an empty session handle.

*/

DRYADAPI_EXT
DrError
DRYADAPI
DrOpenSession(
    OUT  PDRSESSIONHANDLE     pSession
    );

/*++
DrCloseSession

Remarks:

    This API lets user to close an existing session handle.

*/

DRYADAPI_EXT
DrError
DRYADAPI
DrCloseSession(
    IN  DRSESSIONHANDLE     Session
    );

/*++

DrReadStream

Parameters:

    StreamHandle    Stream handle returned by DrOpenStream.

    pBuffer         Pointer to the buffer that receives the data read from the stream.

    pBytesRead      Pointer to variable containing size of the buffer. On return this
                    variable receives number of bytes read.

    Flags           Optional, may be 0.

                        DR_REFRESH_AGGRESSIVE   (default) Refresh metadata from DRM if EOS
                                                is detected.
                        DR_REFRESH_PASSIVE      Do not refresh metadata from DRM if EOS
                                                is detected.

    pReadPosition   Pointer to DR_STREAM_POSITION structure. May be NULL.
                    Specifies position in the stream of the block to be read from.
                    If set to DR_INVALID_EXTENT_INDEX and DR_UNKNOWN_OFFSET then read is
                    performed from the current stream position and on return this variable
                    receives the position of the block read.

    pAsyncInfo      Pointer to DR_ASYNC_INFO structure.
                    See description of to DR_ASYNC_INFO structure.

Remarks:

    If pReadPosition is NULL, then current stream position is used as read position.
    Current stream position is advanced if read is successful.

    If pReadPosition is not NULL, extent index in position structure is set to DR_INVALID_EXTENT_INDEX
    and offset is set to DR_UNKNOWN_OFFSET, then current stream position is used as read position.
    If read is successfull, then read position is saved into variable specifed by pReadPosition and
    current stream position is advanced.

    If pReadPosition is not NULL, extent index in position structure is set valid extent index and
    and offset is set to valid offset, then current stream position is set to position specified by
    pReadPosition, then current read position is used as read position.
    Current stream position is advanced if read is successful.

    Queueing TODO

    If none of refresh flags is not specified in Flags, then value passed to DrOpenStream is used.

--*/
DRYADAPI_EXT
DrError
DRYADAPI
DrReadStream(
    IN  DRHANDLE                StreamHandle,
    OUT PVOID                   pBuffer,
    IN OUT  PSIZE_T             pBytesRead,
    IN  UINT                    Flags,
    IN OUT PDR_STREAM_POSITION  pReadPosition,
    IN  PCDR_ASYNC_INFO         pAsyncInfo
    );


/*++

DrAppendStream

Parameters:

    StreamHandle    Stream handle returned by DrOpenStream.

    pData           Pointer to the buffer containing data to be appended to the stream.

    DataSize        Number of bytes to be appened to the stream.

    Flags           Specifies if append should use fixed offset and if partials append is
                    allowed. Flags can be combined.


                        DR_FIXED_OFFSET_APPEND      append at current offset
                        //TODO: DR_SEAL, DR_UPDATE_DRM


    pAppendPosition Pointer to DR_STREAM_POSITION, may be NULL.
                    If not NULL and append succeeds, then variable receives offset and base extent
                    of appended data block.

    pBytesAppended  Pointer to variable to receive number of bytes appended to the stream.
                    May be NULL.

    pAsyncInfo      Pointer to DR_ASYNC_INFO structure.
                    See description of DR_ASYNC_INFO structure.

Remarks:

    If DR_FIXED_OFFSET_APPEND flag is not specified, then data block is appended at the end of stream,
    possibly more then one time. If there is a need to seal an extent during this process and
    pAppendPosition is NULL, than data block is appended to the next extent without waiting
    for the sealing of previous extent to be completed.

    If DR_FIXED_OFFSET_APPEND flag is specified, than data block is appended to the stream at offset set
    by DrSetStreamPostition. Operation fails if offset is not equal to current length of the stream
    since it's not possible to overwrite data appended to the stream or to write beyond end of stream.
    If append succeeds then current stream position is incremented by the number of bytes appended.

    If DR_PARTIAL_APPEND flag is specified, then partial append is enabled and possibly only first part of
    the specified data block is appended. Variable pointed at by pBytesAppended receives number of bytes
    actually appended to the stream.

    TODO: Queueing

    TODO: IN/OUT append pos

--*/
DRYADAPI_EXT
DrError
DRYADAPI
DrAppendStream(
    IN  DRHANDLE                StreamHandle,
    IN  PCVOID                  pData,
    IN  SIZE_T                  DataSize,
    IN  UINT                    Flags,
    IN OUT PDR_STREAM_POSITION  pAppendPosition,    // non-NULL pAppendPosition must be help around until completion
    OUT PSIZE_T                 pBytesAppended,     // non-NULL pBytesAppended must be held around until completion
    IN  PCDR_ASYNC_INFO         pAsyncInfo
    );

typedef struct tagDR_BUFFERLIST
{
    PCVOID  pData;
    SIZE_T  DataSize;
} DR_BUFFERLIST;
typedef DR_BUFFERLIST *PDR_BUFFERLIST;
typedef const DR_BUFFERLIST *PCDR_BUFFERLIST;

DRYADAPI_EXT
DrError
DRYADAPI
DrAppendStreamBufferList(
    IN  DRHANDLE                StreamHandle,
    IN  PCDR_BUFFERLIST         pBufferList,
    IN  SIZE_T                  BufferCount,
    IN  UINT                    Flags,
    IN OUT PDR_STREAM_POSITION  pAppendPosition,    // non-NULL pAppendPosition must be help around until completion
    OUT PSIZE_T                 pBytesAppended,     // non-NULL pBytesAppended must be held around until completion
    IN OUT void                *pReserved,          // must be set to NULL for now
    IN  PCDR_ASYNC_INFO         pAsyncInfo
    );


DRYADAPI_EXT
DrError
DRYADAPI
DrSetStreamPosition(
    IN  DRHANDLE            StreamHandle,
    IN  UINT                ExtentIndex,
    IN  UINT64              Offset
    );


DRYADAPI_EXT
DrError
DRYADAPI
DrGetStreamPosition(
    IN  DRHANDLE            StreamHandle,
    IN  UINT                Flags,
    OUT PDR_STREAM_POSITION pPosition
    );

/*++

DrTranslateStreamPosition

    Translates a stream position so that it is relative to another extent's base offset (or 0)

Parameters

    StreamHandle     Stream handle returned by DrOpenStream

    pSourcePosition  Position you want to translate

    pDestExtentIndex The extent index to translate relative to.  0 for offset relative to the stream.

    pDestOffset      Pointer to the offset that will be output.
                     Must not be NULL.

    Flags            Reserved. Must be 0.

Remarks:

    To translate into absolute stream offset pass 0 for DestExtentIndex.

    NOTE: this operation retrieves information from internal metadata cache. If you use this API to
    translate extent offset returned by DrAppendStream, then it should always return correct result
    since cache is up-to-date. In other scenarios in case if API fails you may need to refresh stream
    metadata by calling DrGetStreamLength with DR_REFRESH_AGGRESSIVE flag and retry DrTranslateStreamPosition
    after DrGetStreamLength succeeds.
--*/
DRYADAPI_EXT
DrError
DRYADAPI
DrTranslateStreamPosition(
    IN  DRHANDLE            StreamHandle,
    IN  PDR_STREAM_POSITION pSourcePosition,
    IN  UINT                DestExtentIndex,
    OUT PUINT64             pDestOffset,
    IN  UINT                Flags
    );


/*++

DrGetStreamLength

    Gets the length of the stream.

Parameters

    StreamHandle    Stream handle returned by DrOpenStream

    pLength         Pointer to the output length variable.
                    Must not be NULL.

    Flags           One of (or pass 0 for default value):

                        DR_REFRESH_AGGRESSIVE (default)
                            - visit server to find out latest known length
                        DR_REFRESH_PASSIVE
                            - return length from local cache if available otherwise
                              visit server to find out latest known length
                        DR_REFRESH_FROM_CACHE
                            - return length from local cache 
                              fail if not available

    pAsyncInfo      Pointer to DR_ASYNC_INFO structure.
                    See description of DR_ASYNC_INFO structure.

Remarks:

    Call with DR_REFRESH_FROM_CACHE always returns immediately.
    Note that if there are ongoing appends to this stream issue from this or 
    other client this this function may not return precise length.

--*/
DRYADAPI_EXT
DrError
DRYADAPI
DrGetStreamLength(
    IN  DRHANDLE            StreamHandle,
    OUT PUINT64             pLength,
    IN  UINT                Flags,
    IN  PCDR_ASYNC_INFO     pAsyncInfo
    );


DRYADAPI_EXT
DrError
DRYADAPI
DrCloseHandle(
    IN  DRHANDLE        StreamHandle
    );


/*++

DrDelete

    Deletes on recycles stream.

Parameters:

    pStreamName     Pointer to UTF8 name of the stream to be deleted.

    Flags           Specifies if stream should be deleted on recycled.

                        DR_DELETE_FORCE     delete stream

    pAsyncInfo      Pointer to DR_ASYNC_INFO structure.
                    See description of to DR_ASYNC_INFO structure.

Remarks:

    By default this operation recycles stream. Stream is renamed to
    streamName#versionNumber, where versionNumber is integer, and
    stream expiration time is set to recycle stream timeout. Recycled
    stream timeout is specified in DRM configuration file.

    To immediately delete stream set Flags to DR_DELETE_FORCE.

    @TODO: create DrDeleteEx API to return new recycled stream name.

--*/
DRYADAPI_EXT
DrError
DRYADAPI
DrDelete(
    IN  PCSTR           pStreamName,
    IN  UINT            Flags,
    IN  PCDR_ASYNC_INFO pAsyncInfo
    );

DRYADAPI_EXT
DrError
DRYADAPI
DrDeleteEx(
    IN  DRSESSIONHANDLE Session,
    IN  PCSTR           pStreamName,
    IN  UINT            Flags,
    IN  PCDR_ASYNC_INFO pAsyncInfo
    );

/*++

DrRename and DrRenameEx

Parameters:

    Session         Session handle. May be NULL.

    pOldName        Pointer to UTF8 name of the stream to be renamed to pNewName.

    pNewName        Pointer to new UTF8 name of the pOldName stream.

    Flags           A combination of next bit values or zero.

                        DR_RENAME_ALLOW_SUBSTITUTION    Delete pNewName stream if it exists
                                                        before renaming pOldName stream.

                        DR_RENAME_REQUIRE_SUBSTITUTION  Replace existing pNewStream stream
                                                        with pOldStream stream.

    pAsyncInfo      Pointer to DR_ASYNC_INFO structure.
                    See description of to DR_ASYNC_INFO structure.

Remarks:

    If DR_RENAME_ALLOW_SUBSTITUTION and DR_RENAME_REQUIRE_SUBSTITUTION flags are not set then
    operation fails if pNewName stream already exists.

    If either DR_RENAME_ALLOW_SUBSTITUTION or DR_RENAME_REQUIRE_SUBSTITUTION flag is set then
    this function atomically deletes pNewName stream and renames pOldName stream to pNewName.
    If DR_RENAME_REQUIRE_SUBSTITUTION flag is set and pNewName stream does not exist then
    operation fails with DrError_StreamNotFound.

    In substitution mode destination stream may be specified by GUID using .streamid name.
    In this case source stream is renamed to original destination stream name.

--*/
DRYADAPI_EXT
DrError
DRYADAPI
DrRename(
    IN  PCSTR           pOldName,
    IN  PCSTR           pNewName,
    IN  UINT            Flags,
    IN  PCDR_ASYNC_INFO pAsyncInfo
    );

DRYADAPI_EXT
DrError
DRYADAPI
DrRenameEx(
    IN  DRSESSIONHANDLE Session,
    IN  PCSTR           pOldName,
    IN  PCSTR           pNewName,
    IN  UINT            Flags,
    IN  PCDR_ASYNC_INFO pAsyncInfo
    );

/*++

DrMultiModifyStream

Parameters:

    pEntries        Array of DR_MULTIMODIFY_STREAM_ENTRY structures, one for each stream operation
                    to perform

    nEntries        Number of entries in pEntries

    Flags           Reserved. Must be 0.

    pAsyncInfo      Pointer to DR_ASYNC_INFO structure.
                    See description of to DR_ASYNC_INFO structure.

Remarks:

    This API allows you to atomically rename, delete, and set the properties of multiple
    streams on a single volume.

    It is allowed to rename streams over each other concurrently; e.g., A->B and B->A, or
    A->B, B->C, C->A.

    NOTE: in future cosmos versions, the partitioning of volumes will be less clear. To ensure
    compatibility with future versions, you should limit operations to a single directory.
--*/
DrError
DRYADAPI
DrMultiModifyStreamEx(
    IN  DRSESSIONHANDLE Session,
    IN  PCDR_MULTIMODIFY_STREAM_ENTRY pEntries,
    IN  UINT nEntries,
    IN  UINT Flags,
    IN  PCDR_ASYNC_INFO pAsyncInfo
    );

DrError
DRYADAPI
DrMultiModifyStream(
    IN  PCDR_MULTIMODIFY_STREAM_ENTRY pEntries,
    IN  UINT nEntries,
    IN  UINT Flags,
    IN  PCDR_ASYNC_INFO pAsyncInfo
    );

/*++

DrConcatenateStream

Parameters:

    pDestName       Pointer to UTF8 name of the stream to receive extents from source stream.

    pSrcName        Pointer to UTF8 name of the stream to share extents with the destination stream.

    ExtentIndex     Index of extent range to share. Pass 0 to start from the beginning of the stream.

    ExtentCount     Number of extents range to share. Pass DR_ALL_EXTENTS to share all extents from
                    the ExtentIndex to the end of the source stream.

    pDestExtentIndex    If not NULL, pointer to the expected extent index where the new extents will be appended to the destination
                    stream. If NULL, or if the value is DR_UNKOWN_EXTENT_INDEX, then the current end of the stream is used; otherwise,
                    the request fails with DrError_AppendNotAtEnd if the given value does not match the current length of the
                    stream. On return, if not NULL, the value is updated with the actual index at which the extents were appended.
                    Note: On older clusters, the returned value may be DR_UNKNOWN_EXTENT_INDEX if the DRM does not
                    support the feature.

    Flags           Reserved. Must be 0.

    pAsyncInfo      Pointer to DR_ASYNC_INFO structure.
                    See description of to DR_ASYNC_INFO structure.

Remarks:

    Appends extent range specified by ExtentIndex and ExtentCount for pSrcName stream to the end of the
    pDestName stream. No actual copy is done, both streams point to the same extents. Extent is kept
    available while all streams referencing it are present.

--*/
DRYADAPI_EXT
DrError
DRYADAPI
DrConcatenateStream(
    IN  PCSTR           pDestName,
    IN  PCSTR           pSrcName,
    IN  UINT            ExtentIndex,
    IN  UINT            ExtentCount,
    IN OUT PUINT        pDestExtentIndex,
    IN  UINT            Flags,
    IN  PCDR_ASYNC_INFO pAsyncInfo
    );

/*++

DrConcatenateStreamEx

Parameters:

    Session         Session handle, may be NULL.

    pDestName       Pointer to UTF8 name of the stream to receive extents from source stream.

    pDestExtentIndex    If not NULL, pointer to the expected extent index where the new extents will be appended to the destination
                    stream. If NULL, or if the value is DR_UNKOWN_EXTENT_INDEX, then the current end of the stream is used; otherwise,
                    the request fails with DrError_AppendNotAtEnd if the given value does not match the current length of the
                    stream. On return, if not NULL, the value is updated with the actual index at which the extents were appended.
                    Note: On older clusters, the returned value may be DR_UNKNOWN_EXTENT_INDEX if the DRM does not
                    support the feature.

    pSrcRanges      Pointer to array of DR_RANGE structures describing source ranges. See DR_RANGE.
                    There is no need to keep this data in memory for the duration of async call, i.e. it can be allocate on the stack.

    cSrcRanges      Number of DR_RANGE stuctures in pSrcRanges array.

    Flags           Zero or combination of next values:

                        DR_CONCATENATE_ALLOW_CREATE_NEW     create new stream if pDestName stream does not exists.

                        DR_CONCATENATE_REQUIRE_CREATE_NEW   create new stream, fails if pDestName stream does not exists.

    pAsyncInfo      Pointer to DR_ASYNC_INFO structure.
                    See description of to DR_ASYNC_INFO structure.

Remarks:

    Appends extent range specified by ExtentIndex and ExtentCount for pSrcName stream to the end of the
    pDestName stream. No actual copy is done, both streams point to the same extents. Extent is kept
    available while all streams referencing it are present.

--*/
DRYADAPI_EXT
DrError
DRYADAPI
DrConcatenateStreamEx(
    IN  DRSESSIONHANDLE Session,
    IN  PCSTR           pDestName,
    IN OUT PUINT        pDestExtentIndex,
    IN  PCDR_RANGE      pSrcRanges,
    IN  SIZE_T          cSrcRanges,
    IN  UINT            Flags,
    IN  PCDR_ASYNC_INFO pAsyncInfo
);

/*++

DrGetStreamInformation
DrGetStreamInformationEx

Retrieves stream information for stream with given name.

Parameters:

    pStreamName             UTF8 encoded fully qualified cosmos URI, file URI, UNC name, or local file name.
                            Cannot be NULL.

    Reserved0               Reserved. Must be zero.

    StartExtentIndex        Index of first stream extent to return information about.
                            Pass 0 for all extents.

    NumberOfExtentsToFill   Number of extents to return information about.
                            Pass 0 for no extent information, DR_ALL_EXTENTS for information about all
                            extents in the stream.

    ppStreamInfo            Pointer to a variable to receive pointer to DR_STREAM structure.
                            Returned structure must be freed by call to DrFreeMemory.

    pAsyncInfo              Pointer to DR_ASYNC_INFO structure.
                            See description of to DR_ASYNC_INFO structure.
--*/
DRYADAPI_EXT
DrError
DRYADAPI
DrGetStreamInformation(
    IN  PCSTR           pStreamName,
    IN  UINT64          Reserved0,
    IN  UINT            StartExtentIndex,
    IN  UINT            NumberOfExtentsToFill,
    OUT PDR_STREAM*     ppStreamInfo,
    IN  PCDR_ASYNC_INFO pAsyncInfo
);

DRYADAPI_EXT
DrError
DRYADAPI
DrGetStreamInformationEx(
    IN  DRSESSIONHANDLE Session,
    IN  PCSTR           pStreamName,
    IN  UINT64          Reserved0,
    IN  UINT            StartExtentIndex,
    IN  UINT            NumberOfExtentsToFill,
    OUT PDR_STREAM*     ppStreamInfo,
    IN  PCDR_ASYNC_INFO pAsyncInfo
);

/*++
DrGetStreamInformationByHandle

Retrieves stream information for stream with given handle.

See DrGetStreamInformation for parameter description.
--*/
DRYADAPI_EXT
DrError
DRYADAPI
DrGetStreamInformationByHandle(
    IN  DRHANDLE        StreamHandle,
    IN  UINT64          Reserved0,
    IN  UINT            StartExtentIndex,
    IN  UINT            NumberOfExtentsToFill,
    OUT PDR_STREAM*     ppStreamInfo,
    IN  PCDR_ASYNC_INFO pAsyncInfo
);


/*++

DrSetStreamProperties

Parameters:

    pStreamName         Pointer to UTF8 name of the stream.

    pStreamProperties   Pointer to DR_STREAM_PROPERTIES structure containing properties to change.

    pAsyncInfo          Pointer to DR_ASYNC_INFO structure.
                        See description of to DR_ASYNC_INFO structure.

Remarks:

    Attempts to changes stream properties such as expiration time or read-only attribute.

    Set pStreamProperties->ExpirePeriod to number of 100 nanoseconds ticks to change stream's
    expiration time (resulting expiration time is calculated as server current time + pStreamProperties->ExpirePeriod).
    Set to 0 leave stream's expiration time intact.
    Set to DR_INIFINTE to reset stream expiration time to never expire.
    NOTE: for read only stream it's only possible to increase expiration period. Attempt to srink it fails
    with DrError_StreamReadOnly error.

    pStreamProperties->ReadOffsetHint is not used and should be set to 0.

    pStreamProperties->FlagsMask specifies flags to be modified and pStreamProperties->Flags specifies new flag values.
    Set pStreamProperties->FlagsMask to 0 to skip flag modification.

    Available flags:
        DR_STREAM_READ_ONLY
        DR_STREAM_SEALED

    NOTE: Flag values applied before expiration period. If you attempt to set read-only flag and shrink expiration
    period in one call, stream becomes read-only and expiration period is not modified. Such call fails
    with DrError_StreamReadOnly error.

--*/
DRYADAPI_EXT
DrError
DRYADAPI
DrSetStreamProperties(
    IN  PCSTR                   pStreamName,
    IN  PCDR_STREAM_PROPERTIES  pStreamProperties,
    IN  PCDR_ASYNC_INFO         pAsyncInfo
);

//
// Extended version of setting stream property API which caller can attach a session handle
//

DRYADAPI_EXT
DrError
DRYADAPI
DrSetStreamPropertiesEx(
    IN  DRSESSIONHANDLE         Session,
    IN  PCSTR                   pStreamName,
    IN  PCDR_STREAM_PROPERTIES  pStreamProperties,
    IN  PCDR_ASYNC_INFO         pAsyncInfo
);

/*++

DrSetStreamProperties

Parameters:

    StreamHandle    Stream handle returned by DrOpenStream.

    pStreamProperties   Pointer to DR_STREAM_PROPERTIES structure containing properties to change.

    pAsyncInfo          Pointer to DR_ASYNC_INFO structure.
                        See description of to DR_ASYNC_INFO structure.

Remarks:

    See remarks for DrSetStreamProperties function.

--*/
DRYADAPI_EXT
DrError
DRYADAPI
DrSetStreamPropertiesByHandle(
    IN  DRHANDLE                StreamHandle,
    IN  PCDR_STREAM_PROPERTIES  pStreamProperties,
    IN  PCDR_ASYNC_INFO         pAsyncInfo
);


DRYADAPI_EXT
DrError
DRYADAPI
DrSetStreamOption(
    IN  DRHANDLE    StreamHandle,
    IN  UINT        StreamOption,
    IN  PCVOID      pData,
    IN  SIZE_T      cbData
);


DRYADAPI_EXT
DrError
DRYADAPI
DrGetStreamOption(
    IN  DRHANDLE    StreamHandle,
    IN  UINT        StreamOption,
    OUT PVOID       pBuffer,
    IN  SIZE_T      cbBuffer
);


//
// Directory APIs
//

/*++

DrEnumerateDirectory

Enumerates content of directory.

Parameters:

    pDirectoryName  UTF8 encoded fully qualified cosmos directory URI, file system directory URI,
                    UNC name, or local directory name.

    Flags


    ppDirectoryInfo Pointer to variable that receives

    pAsyncInfo      Pointer to DR_ASYNC_INFO structure.
                    See description of to DR_ASYNC_INFO structure.
--*/
DRYADAPI_EXT
DrError
DRYADAPI
DrEnumerateDirectory(
    IN  PCSTR           pDirectoryName,
    IN  UINT            Flags,
    OUT PDR_DIRECTORY*  ppDirectoryInfo,
    IN  PCDR_ASYNC_INFO pAsyncInfo
    );

//
// Extended version of enumerating directory API which caller can attach a session handle
//

DRYADAPI_EXT
DrError
DRYADAPI
DrEnumerateDirectoryEx(
    IN  DRSESSIONHANDLE Session,
    IN  PCSTR           pDirectoryName,
    IN  UINT            Flags,
    OUT PDR_DIRECTORY*  ppDirectoryInfo,
    IN  PCDR_ASYNC_INFO pAsyncInfo
    );

//
// Volume APIs
//

DRYADAPI_EXT
DrError
DRYADAPI
DrGetVolumeInformation(
    IN  PCSTR           pVolumeName,
    OUT PDR_VOLUME*     ppVolumeInfo,
    IN  PCDR_ASYNC_INFO pAsyncInfo
);

//
// Extended version of Volume API which caller can attach a session handle
//

DRYADAPI_EXT
DrError
DRYADAPI
DrGetVolumeInformationEx(
    IN  DRSESSIONHANDLE Session,
    IN  PCSTR           pVolumeName,
    OUT PDR_VOLUME*     ppVolumeInfo,
    IN  PCDR_ASYNC_INFO pAsyncInfo
);

//
// Auxiliary APIs
//

/*++

DrGetErrorMessage

Parameters:

    StatusCode      Status code

    pBuffer         Pointer to a buffer

    cbBuffer        Size of the buffer in bytes

Remarks:

    Fills in pBuffer with UTF8 encoded error message corresponding to StatusCode.
    If cbBuffer bytes is not enough to save the whole message
    returns HRESULT_FROM_WIN32( ERROR_INSUFFICIENT_BUFFER ).

--*/
DRYADAPI_EXT
DrError
DRYADAPI
DrGetErrorMessage(
    IN  DrError StatusCode,
    OUT PCHAR   pBuffer,
    IN  SIZE_T  cbBuffer
    );


/*++

DrSetParentId

Parameters:

    ParentId     Parent operation Id for the next client API call.

Remarks:

    Sets parent operation id for the subsequent call to the other client API function 
    on the same thread.
    Provided id is logged at the start of the next client API call. It makes it possible to
    relate log statements for user operations and client library operations.

--*/
DRYADAPI_EXT
void
DRYADAPI
DrSetParentId(
    IN  UINT ParentId
    );

/*++

DrSetOpId

Parameters:

    OpId     Operation Id for the next client API call.

Remarks:

    Sets operation id for the subsequent call to the other client API function on the same thread.

    MAKE SURE that you always call DrOpidNew to generate the operation id, so that it is 
    guaranteed to be unique in the process.  

    NEVER generate the operation id by youself or any other way.
--*/
DRYADAPI_EXT
void
DRYADAPI
DrSetOpId(
    IN  UINT OpId
    );

/*++

DrComposeExtentStreamInfo

Parameters:

    pCluster,
    pVolume                 cluster and volume names where the extent belongs

    pExtentInfo             Extent info of the extent to become stream
    
    ppStreamName            Pointer to a variable to receive .extentid stream name

    ppStreamInfo            Pointer to a variable to receive pointer to DR_STREAM structure.

                            Returned structure must be freed by call to DrFreeMemory,
                            which also frees *ppStreamName.
                            DO NOT free *ppStreamName alone!

Remarks:

    Allocate and fill in fake stream information for the extent information provided here.
    *ppStreamName and *ppStreamInfo may be passed as parameters straight into 
    DrOpenStreamFromStreamInfo

--*/
DRYADAPI_EXT
DrError
DRYADAPI
DrComposeExtentStreamInfo (
    IN  PCSTR        pCluster,
    IN  PCSTR        pVolume,
    IN  PDR_EXTENT   pExtentInfo,
    OUT PCSTR      * ppStreamName,
    OUT PDR_STREAM * ppStreamInfo
    );


#endif  // JC if 0


#pragma pack( pop )

#pragma warning( pop )

#if defined(__cplusplus)
}
#endif

