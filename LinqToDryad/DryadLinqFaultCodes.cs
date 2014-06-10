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

using System;
using System.Collections.Generic;
using System.Text;
using System.Linq.Expressions;
using System.Runtime.Serialization;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace Microsoft.Research.DryadLinq
{
    internal enum DryadLinqErrorCodeCategory : int
    {
        QueryAPI = 0x01000000,
        CodeGen = 0x02000000,
        JobSubmission = 0x03000000,
        Serialization = 0x04000000,
        StoreClient= 0x05000000,
        VertexRuntime = 0x06000000,
        LocalDebug = 0x07000000,
        Unknown = 0x0f000000
    }

    /// <summary>
    /// Lists all error code in DryadLinq
    /// </summary>
    /// <remarks>
    /// NOTE: New error codes must be appended to a category
    /// NOTE: Error codes cannot be deleted
    /// </remarks>
    internal static class DryadLinqErrorCode
    {
        internal const int codesPerCategory = 0x01000000;

        #region CodeGen
        public const int TypeRequiredToBePublic = (int) DryadLinqErrorCodeCategory.CodeGen + 0;
        public const int CustomSerializerMustSupportDefaultCtor = (int) DryadLinqErrorCodeCategory.CodeGen + 1;
        public const int CustomSerializerMustBeClassOrStruct = (int) DryadLinqErrorCodeCategory.CodeGen + 2;
        public const int TypeNotSerializable = (int) DryadLinqErrorCodeCategory.CodeGen + 3;
        public const int CannotHandleSubtypes = (int)DryadLinqErrorCodeCategory.CodeGen + 4;
        public const int UDTMustBeConcreteType = (int)DryadLinqErrorCodeCategory.CodeGen + 5;
        public const int UDTHasFieldOfNonPublicType = (int)DryadLinqErrorCodeCategory.CodeGen + 6;
        public const int UDTIsDelegateType = (int)DryadLinqErrorCodeCategory.CodeGen + 7;
        public const int FailedToBuild = (int) DryadLinqErrorCodeCategory.CodeGen + 8;
        public const int OutputTypeCannotBeAnonymous = (int) DryadLinqErrorCodeCategory.CodeGen + 9;
        public const int InputTypeCannotBeAnonymous = (int) DryadLinqErrorCodeCategory.CodeGen + 10;
        public const int BranchOfForkNotUsed = (int) DryadLinqErrorCodeCategory.CodeGen + 11;
        public const int ComparerMustBeSpecifiedOrKeyTypeMustBeIComparable= (int) DryadLinqErrorCodeCategory.CodeGen + 12;
        public const int ComparerMustBeSpecifiedOrKeyTypeMustBeIEquatable = (int)DryadLinqErrorCodeCategory.CodeGen + 13;
        public const int ComparerExpressionMustBeSpecifiedOrElementTypeMustBeIEquatable = (int)DryadLinqErrorCodeCategory.CodeGen + 14;
        public const int TooManyHomomorphicAttributes = (int) DryadLinqErrorCodeCategory.CodeGen + 15;
        public const int HomomorphicApplyNeedsSamePartitionCount = (int)DryadLinqErrorCodeCategory.CodeGen + 16;
        public const int UnrecognizedDataSource = (int) DryadLinqErrorCodeCategory.CodeGen + 17;
        public const int CannotConcatDatasetsWithDifferentCompression = (int) DryadLinqErrorCodeCategory.CodeGen + 21;
        public const int AggregateOperatorNotSupported = (int) DryadLinqErrorCodeCategory.CodeGen + 23;
        public const int FinalizerReturnTypeMismatch = (int)DryadLinqErrorCodeCategory.CodeGen + 24;
        public const int CannotHandleCircularTypes = (int)DryadLinqErrorCodeCategory.CodeGen + 26;
        public const int OperatorNotSupported = (int)DryadLinqErrorCodeCategory.CodeGen + 27;
        public const int AggregationOperatorRequiresIComparable = (int)DryadLinqErrorCodeCategory.CodeGen + 28;
        public const int DecomposerTypeDoesNotImplementInterface = (int)DryadLinqErrorCodeCategory.CodeGen + 29;
        public const int DecomposerTypeImplementsTooManyInterfaces = (int)DryadLinqErrorCodeCategory.CodeGen + 30;
        public const int DecomposerTypesDoNotMatch = (int)DryadLinqErrorCodeCategory.CodeGen + 31;
        public const int DecomposerTypeMustBePublic = (int)DryadLinqErrorCodeCategory.CodeGen + 32;
        public const int DecomposerTypeDoesNotHavePublicDefaultCtor = (int)DryadLinqErrorCodeCategory.CodeGen + 33;
        public const int AssociativeMethodHasWrongForm = (int)DryadLinqErrorCodeCategory.QueryAPI + 34;
        public const int AssociativeTypeDoesNotImplementInterface = (int)DryadLinqErrorCodeCategory.CodeGen + 35;
        public const int AssociativeTypeImplementsTooManyInterfaces = (int)DryadLinqErrorCodeCategory.CodeGen + 36;
        public const int AssociativeTypesDoNotMatch = (int)DryadLinqErrorCodeCategory.CodeGen + 37;
        public const int AssociativeTypeMustBePublic = (int)DryadLinqErrorCodeCategory.CodeGen + 38;
        public const int AssociativeTypeDoesNotHavePublicDefaultCtor = (int)DryadLinqErrorCodeCategory.CodeGen + 39;
        public const int CannotCreatePartitionNodeRandom = (int)DryadLinqErrorCodeCategory.CodeGen + 43;
        public const int PartitionKeysNotProvided = (int)DryadLinqErrorCodeCategory.CodeGen + 44;
        public const int PartitionKeysAreNotConsistentlyOrdered = (int)DryadLinqErrorCodeCategory.CodeGen + 45;
        public const int IsDescendingIsInconsistent = (int)DryadLinqErrorCodeCategory.CodeGen + 46;
        public const int BadSeparatorCount = (int)DryadLinqErrorCodeCategory.CodeGen + 65;
        public const int TypeMustHaveDataMembers = (int)DryadLinqErrorCodeCategory.CodeGen + 66;
        public const int CannotHandleObjectFields = (int)DryadLinqErrorCodeCategory.CodeGen + 67;
        public const int CannotHandleDerivedtypes = (int)DryadLinqErrorCodeCategory.CodeGen + 68;
        public const int MultipleOutputsWithSameDscUri = (int)DryadLinqErrorCodeCategory.CodeGen + 69;
        public const int OutputUriAlsoQueryInput = (int)DryadLinqErrorCodeCategory.CodeGen + 70;

        //The "internal" code is used for internal errors that should not be hit by users.
        //The messages may be informative, but the error code doesn't need to be and it avoids users
        //seeing all the error codes in intellisense and/or wondering if they should catch & deal with them etc.
        public const int Internal = (int)DryadLinqErrorCodeCategory.CodeGen + 71;

        #endregion

        #region StoreClient
        public const int DSCStreamError = (int) DryadLinqErrorCodeCategory.StoreClient + 0;
        public const int StreamDoesNotExist = (int) DryadLinqErrorCodeCategory.StoreClient + 1;
        public const int StreamAlreadyExists = (int) DryadLinqErrorCodeCategory.StoreClient + 2;
        public const int AttemptToReadFromAWriteStream = (int) DryadLinqErrorCodeCategory.StoreClient + 3;
        public const int FailedToCreateStream = (int) DryadLinqErrorCodeCategory.StoreClient + 4;
        public const int JobToCreateTableWasCanceled = (int) DryadLinqErrorCodeCategory.StoreClient + 5;
        public const int FailedToGetReadPathsForStream = (int) DryadLinqErrorCodeCategory.StoreClient + 6;
        public const int CannotAccesFilePath = (int)DryadLinqErrorCodeCategory.StoreClient + 7;
        public const int PositionNotSupported = (int)DryadLinqErrorCodeCategory.StoreClient + 8;
        public const int GetFileSizeError = (int)DryadLinqErrorCodeCategory.StoreClient + 9;
        public const int ReadFileError = (int)DryadLinqErrorCodeCategory.StoreClient + 10;
        public const int UnknownCompressionScheme = (int)DryadLinqErrorCodeCategory.StoreClient + 11;
        public const int WriteFileError = (int)DryadLinqErrorCodeCategory.StoreClient + 12;
        public const int MultiBlockEmptyPartitionList = (int)DryadLinqErrorCodeCategory.StoreClient + 13;
        public const int GetURINotSupported = (int)DryadLinqErrorCodeCategory.StoreClient + 14;
        public const int SetCalcFPNotSupported = (int)DryadLinqErrorCodeCategory.StoreClient + 15;
        public const int GetFPNotSupported = (int)DryadLinqErrorCodeCategory.StoreClient + 16;
        public const int FailedToAllocateNewNativeBuffer = (int)DryadLinqErrorCodeCategory.StoreClient + 17;
        public const int FailedToReadFromInputChannel = (int)DryadLinqErrorCodeCategory.StoreClient + 18;
        public const int FailedToWriteToOutputChannel = (int)DryadLinqErrorCodeCategory.StoreClient + 19;
        public const int MultiBlockCannotAccesFilePath = (int)DryadLinqErrorCodeCategory.StoreClient + 25;
        #endregion

        #region JobSubmission
        public const int DryadHomeMustBeSpecified = (int) DryadLinqErrorCodeCategory.JobSubmission + 0;
        public const int ClusterNameMustBeSpecified = (int) DryadLinqErrorCodeCategory.JobSubmission + 1;
        public const int UnexpectedJobStatus = (int) DryadLinqErrorCodeCategory.JobSubmission + 2;
        public const int JobStatusQueryError = (int) DryadLinqErrorCodeCategory.JobSubmission + 3;
        public const int JobOptionNotImplemented = (int) DryadLinqErrorCodeCategory.JobSubmission + 4;
        public const int DryadLinqJobMinMustBe2OrMore = (int) DryadLinqErrorCodeCategory.JobSubmission + 5;
        public const int SubmissionFailure = (int)DryadLinqErrorCodeCategory.JobSubmission + 6;
        public const int UnsupportedSchedulerType = (int)DryadLinqErrorCodeCategory.JobSubmission + 7;
        public const int UnsupportedExecutionKind = (int)DryadLinqErrorCodeCategory.JobSubmission + 8;
        public const int DidNotCompleteSuccessfully = (int)DryadLinqErrorCodeCategory.JobSubmission + 9;
        public const int Binaries32BitNotSupported = (int)DryadLinqErrorCodeCategory.JobSubmission + 10;
        #endregion

        #region QueryAPI
        public const int DistinctAttributeComparerNotDefined = (int) DryadLinqErrorCodeCategory.QueryAPI + 0;
        public const int SerializerTypeMustBeNonNull = (int) DryadLinqErrorCodeCategory.QueryAPI + 1;
        public const int SerializerTypeMustSupportIDryadLinqSerializer = (int) DryadLinqErrorCodeCategory.QueryAPI + 2;
        public const int UnrecognizedOperatorName = (int) DryadLinqErrorCodeCategory.QueryAPI + 3; 
        public const int UnsupportedExpressionsType = (int) DryadLinqErrorCodeCategory.QueryAPI + 7;
        public const int UnsupportedExpressionType = (int) DryadLinqErrorCodeCategory.QueryAPI + 8;
        public const int IndexTooSmall = (int)DryadLinqErrorCodeCategory.QueryAPI + 10;
        public const int MultiQueryableKeyOutOfRange = (int) DryadLinqErrorCodeCategory.QueryAPI + 11;
        public const int IndexOutOfRange = (int) DryadLinqErrorCodeCategory.QueryAPI + 12;
        public const int ExpressionTypeNotHandled = (int) DryadLinqErrorCodeCategory.QueryAPI + 15;
        public const int FailedToGetStreamProps = (int) DryadLinqErrorCodeCategory.QueryAPI + 16;
        public const int MetadataRecordType = (int) DryadLinqErrorCodeCategory.QueryAPI + 17;
        public const int JobToCreateTableFailed = (int) DryadLinqErrorCodeCategory.QueryAPI + 20;
        public const int OnlyAvailableForPhysicalData = (int) DryadLinqErrorCodeCategory.QueryAPI + 22;
        public const int FileSetMustBeSealed = (int)DryadLinqErrorCodeCategory.QueryAPI + 23;
        public const int FileSetCouldNotBeOpened = (int)DryadLinqErrorCodeCategory.QueryAPI + 24;
        public const int FileSetMustHaveAtLeastOneFile = (int)DryadLinqErrorCodeCategory.QueryAPI + 25;
        public const int CouldNotGetClientVersion = (int)DryadLinqErrorCodeCategory.QueryAPI + 27;
        public const int CouldNotGetServerVersion = (int)DryadLinqErrorCodeCategory.QueryAPI + 28;
        public const int ContextDisposed = (int)DryadLinqErrorCodeCategory.QueryAPI + 29;
        public const int UnhandledQuery = (int)DryadLinqErrorCodeCategory.QueryAPI + 30; //@@TODO: when possible, reword the sr.txt entry.
        public const int ExpressionMustBeMethodCall= (int)DryadLinqErrorCodeCategory.QueryAPI + 31;
        public const int UntypedProviderMethodsNotSupported = (int)DryadLinqErrorCodeCategory.QueryAPI + 32;
        public const int ErrorReadingMetadata = (int)DryadLinqErrorCodeCategory.QueryAPI + 33;
        public const int MustStartFromContext = (int)DryadLinqErrorCodeCategory.QueryAPI + 34;
        #endregion

        #region Serialization
        public const int FailedToReadFrom = (int) DryadLinqErrorCodeCategory.Serialization + 0;
        public const int EndOfStreamEncountered = (int) DryadLinqErrorCodeCategory.Serialization + 1;
        public const int SettingPositionNotSupported = (int) DryadLinqErrorCodeCategory.Serialization + 2;
        public const int FingerprintDisabled = (int) DryadLinqErrorCodeCategory.Serialization + 3;
        public const int RecordSizeMax2GB = (int) DryadLinqErrorCodeCategory.Serialization + 4;
        public const int ReadByteNotAllowed = (int) DryadLinqErrorCodeCategory.Serialization + 6;
        public const int ReadNotAllowed = (int) DryadLinqErrorCodeCategory.Serialization + 7;
        public const int SeekNotSupported = (int) DryadLinqErrorCodeCategory.Serialization + 8;
        public const int SetLengthNotSupported = (int) DryadLinqErrorCodeCategory.Serialization + 9;
        public const int FailedToDeserialize = (int) DryadLinqErrorCodeCategory.Serialization + 10;
        public const int ChannelCannotBeReadMoreThanOnce = (int) DryadLinqErrorCodeCategory.Serialization + 11;
        public const int WriteNotSupported = (int)DryadLinqErrorCodeCategory.Serialization + 13;
        public const int WriteByteNotSupported = (int)DryadLinqErrorCodeCategory.Serialization + 14;
        public const int CannotSerializeDryadLinqQuery = (int)DryadLinqErrorCodeCategory.Serialization + 15;
        public const int CannotSerializeObject = (int)DryadLinqErrorCodeCategory.Serialization + 16;
        public const int GeneralSerializeFailure = (int)DryadLinqErrorCodeCategory.Serialization + 17;
        #endregion

        #region VertexRuntime
        public const int SourceOfMergesortMustBeMultiEnumerable = (int) DryadLinqErrorCodeCategory.VertexRuntime + 1;
        public const int ThenByNotSupported = (int) DryadLinqErrorCodeCategory.VertexRuntime + 2;
        public const int AggregateNoElements = (int) DryadLinqErrorCodeCategory.VertexRuntime + 3;
        public const int FirstNoElementsFirst = (int) DryadLinqErrorCodeCategory.VertexRuntime + 4;
        public const int SingleMoreThanOneElement = (int) DryadLinqErrorCodeCategory.VertexRuntime + 5;
        public const int SingleNoElements = (int) DryadLinqErrorCodeCategory.VertexRuntime + 6;
        public const int LastNoElements = (int) DryadLinqErrorCodeCategory.VertexRuntime + 7;
        public const int MinNoElements = (int) DryadLinqErrorCodeCategory.VertexRuntime + 8;
        public const int MaxNoElements = (int) DryadLinqErrorCodeCategory.VertexRuntime + 9;
        public const int AverageNoElements = (int) DryadLinqErrorCodeCategory.VertexRuntime + 10;
        public const int RangePartitionKeysMissing = (int) DryadLinqErrorCodeCategory.VertexRuntime + 11;
        public const int PartitionFuncReturnValueExceedsNumPorts = (int) DryadLinqErrorCodeCategory.VertexRuntime + 12;
        public const int FailureInExcept = (int) DryadLinqErrorCodeCategory.VertexRuntime + 13;
        public const int FailureInIntersect = (int) DryadLinqErrorCodeCategory.VertexRuntime + 14;
        public const int FailureInSort = (int) DryadLinqErrorCodeCategory.VertexRuntime + 15;
        public const int RangePartitionInputOutputMismatch = (int) DryadLinqErrorCodeCategory.VertexRuntime + 16;
        public const int KeyNotFound = (int)DryadLinqErrorCodeCategory.VertexRuntime + 18;
        public const int TooManyItems = (int)DryadLinqErrorCodeCategory.VertexRuntime + 19;
        public const int FailureInHashGroupBy = (int)DryadLinqErrorCodeCategory.VertexRuntime + 20;
        public const int FailureInSortGroupBy = (int)DryadLinqErrorCodeCategory.VertexRuntime + 21;
        public const int FailureInHashJoin = (int)DryadLinqErrorCodeCategory.VertexRuntime + 22;
        public const int FailureInHashGroupJoin = (int)DryadLinqErrorCodeCategory.VertexRuntime + 23;
        public const int FailureInDistinct = (int)DryadLinqErrorCodeCategory.VertexRuntime + 24;
        public const int FailureInOperator = (int)DryadLinqErrorCodeCategory.VertexRuntime + 25;
        public const int FailureInUserApplyFunction = (int)DryadLinqErrorCodeCategory.VertexRuntime + 26;
        public const int FailureInOrderedGroupBy = (int)DryadLinqErrorCodeCategory.VertexRuntime + 27;
        public const int TooManyElementsBeforeReduction = (int)DryadLinqErrorCodeCategory.VertexRuntime + 33;
        #endregion

        #region LocalDebug
        public const int CreatingDscDataFromLocalDebugFailed = (int)DryadLinqErrorCodeCategory.LocalDebug + 0;
        #endregion

        #region Unknown
        public const int UnknownError = (int) DryadLinqErrorCodeCategory.Unknown + 0;
        
        #endregion
              
        /// <summary>
        /// Returns the category of the specified error code
        /// </summary>
        internal static DryadLinqErrorCodeCategory Category(int code)
        {
            if ((code >= (int)DryadLinqErrorCodeCategory.QueryAPI) &&
                (code < (int)DryadLinqErrorCodeCategory.QueryAPI+ codesPerCategory))
            {
                return DryadLinqErrorCodeCategory.QueryAPI;
            }
            else if ((code >= (int)DryadLinqErrorCodeCategory.CodeGen) &&
                     (code < (int)DryadLinqErrorCodeCategory.CodeGen+ codesPerCategory))
            {
                return DryadLinqErrorCodeCategory.CodeGen;
            }
            else if ((code >= (int)DryadLinqErrorCodeCategory.JobSubmission) &&
                     (code < (int)DryadLinqErrorCodeCategory.JobSubmission+ codesPerCategory))
            {
                return DryadLinqErrorCodeCategory.JobSubmission;
            }
            else if ((code >= (int)DryadLinqErrorCodeCategory.Serialization) &&
                     (code < (int)DryadLinqErrorCodeCategory.Serialization+ codesPerCategory))
            {
                return DryadLinqErrorCodeCategory.Serialization;
            }
            else if ((code >= (int)DryadLinqErrorCodeCategory.StoreClient) &&
                     (code < (int)DryadLinqErrorCodeCategory.StoreClient+ codesPerCategory))
            {
                return DryadLinqErrorCodeCategory.StoreClient;
            }
            else if ((code >= (int)DryadLinqErrorCodeCategory.VertexRuntime) &&
                     (code < (int)DryadLinqErrorCodeCategory.VertexRuntime+ codesPerCategory))
            {
                return DryadLinqErrorCodeCategory.VertexRuntime;
            }
            else if ((code >= (int)DryadLinqErrorCodeCategory.LocalDebug) &&
                     (code < (int)DryadLinqErrorCodeCategory.LocalDebug+ codesPerCategory))
            {
                return DryadLinqErrorCodeCategory.LocalDebug;
            }
            else
            {
                return DryadLinqErrorCodeCategory.Unknown;
            }
        }
    }
}
