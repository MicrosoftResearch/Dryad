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

//
// � Microsoft Corporation.  All rights reserved.
//
using System;
using System.Collections.Generic;
using System.Text;
using System.Linq.Expressions;
using System.Runtime.Serialization;
    using System.Reflection;

namespace Microsoft.Research.DryadLinq
{

    // Make HpcLinq error categories internal for SP2. Tracked via bug 13313.
    internal enum HpcLinqErrorCodeCategory : int
    {
        QueryAPI = 0x01000000,
        CodeGen = 0x02000000,
        JobSubmission = 0x03000000,
        Serialization = 0x04000000,
        DscClient= 0x05000000,
        VertexRuntime = 0x06000000,
        LocalDebug = 0x07000000,
        Unknown = 0x0f000000
    }

    //@@TODO: when possible, remove the sr.txt entries for all the items marked "DEL"

    /// <summary>
    /// Lists all error code in HpcLinq
    /// </summary>
    /// <remarks>
    /// NOTE: New error codes must be appended to a category
    /// NOTE: Error codes cannot be deleted
    /// </remarks>
    public static class HpcLinqErrorCode
    {
        internal const int codesPerCategory = 0x01000000;

        #region CodeGen
        public const int TypeRequiredToBePublic = (int) HpcLinqErrorCodeCategory.CodeGen + 0;
        public const int CustomSerializerMustSupportDefaultCtor = (int) HpcLinqErrorCodeCategory.CodeGen + 1;
        public const int CustomSerializerMustBeClassOrStruct = (int) HpcLinqErrorCodeCategory.CodeGen + 2;
        public const int TypeNotSerializable = (int) HpcLinqErrorCodeCategory.CodeGen + 3;
        public const int CannotHandleSubtypes = (int)HpcLinqErrorCodeCategory.CodeGen + 4;
        public const int UDTMustBeConcreteType = (int)HpcLinqErrorCodeCategory.CodeGen + 5;
        public const int UDTHasFieldOfNonPublicType = (int)HpcLinqErrorCodeCategory.CodeGen + 6;
        public const int UDTIsDelegateType = (int)HpcLinqErrorCodeCategory.CodeGen + 7;
        public const int FailedToBuild = (int) HpcLinqErrorCodeCategory.CodeGen + 8;
        public const int OutputTypeCannotBeAnonymous = (int) HpcLinqErrorCodeCategory.CodeGen + 9;
        public const int InputTypeCannotBeAnonymous = (int) HpcLinqErrorCodeCategory.CodeGen + 10;
        public const int BranchOfForkNotUsed = (int) HpcLinqErrorCodeCategory.CodeGen + 11;
        public const int ComparerMustBeSpecifiedOrKeyTypeMustBeIComparable= (int) HpcLinqErrorCodeCategory.CodeGen + 12;
        public const int ComparerMustBeSpecifiedOrKeyTypeMustBeIEquatable = (int)HpcLinqErrorCodeCategory.CodeGen + 13;
        public const int ComparerExpressionMustBeSpecifiedOrElementTypeMustBeIEquatable = (int)HpcLinqErrorCodeCategory.CodeGen + 14;
        public const int TooManyHomomorphicAttributes = (int) HpcLinqErrorCodeCategory.CodeGen + 15;
        public const int HomomorphicApplyNeedsSamePartitionCount = (int)HpcLinqErrorCodeCategory.CodeGen + 16;
        public const int UnrecognizedDataSource = (int) HpcLinqErrorCodeCategory.CodeGen + 17;
        // [deleted] #18 ReducerDoesntExist
        // [deleted] #19 CombinerDoesntExist
        // [deleted] #20 CombinerReturnTypeMismatch
        public const int CannotConcatDatasetsWithDifferentCompression = (int) HpcLinqErrorCodeCategory.CodeGen + 21;
        // [deleted] #22 CannotCreateTablesWithDifferentCompression 
        public const int AggregateOperatorNotSupported = (int) HpcLinqErrorCodeCategory.CodeGen + 23;
        public const int FinalizerReturnTypeMismatch = (int)HpcLinqErrorCodeCategory.CodeGen + 24;
        // [deleted] #25 FinalizerDoesNotExist  
        public const int CannotHandleCircularTypes = (int)HpcLinqErrorCodeCategory.CodeGen + 26;
        public const int OperatorNotSupported = (int)HpcLinqErrorCodeCategory.CodeGen + 27;
        public const int AggregationOperatorRequiresIComparable = (int)HpcLinqErrorCodeCategory.CodeGen + 28;
        public const int DecomposerTypeDoesNotImplementInterface = (int)HpcLinqErrorCodeCategory.CodeGen + 29;
        public const int DecomposerTypeImplementsTooManyInterfaces = (int)HpcLinqErrorCodeCategory.CodeGen + 30;
        public const int DecomposerTypesDoNotMatch = (int)HpcLinqErrorCodeCategory.CodeGen + 31;
        public const int DecomposerTypeMustBePublic = (int)HpcLinqErrorCodeCategory.CodeGen + 32;
        public const int DecomposerTypeDoesNotHavePublicDefaultCtor = (int)HpcLinqErrorCodeCategory.CodeGen + 33;
        public const int AssociativeMethodHasWrongForm = (int)HpcLinqErrorCodeCategory.QueryAPI + 34;
        public const int AssociativeTypeDoesNotImplementInterface = (int)HpcLinqErrorCodeCategory.CodeGen + 35;
        public const int AssociativeTypeImplementsTooManyInterfaces = (int)HpcLinqErrorCodeCategory.CodeGen + 36;
        public const int AssociativeTypesDoNotMatch = (int)HpcLinqErrorCodeCategory.CodeGen + 37;
        public const int AssociativeTypeMustBePublic = (int)HpcLinqErrorCodeCategory.CodeGen + 38;
        public const int AssociativeTypeDoesNotHavePublicDefaultCtor = (int)HpcLinqErrorCodeCategory.CodeGen + 39;
        // [deleted] #40 ClientNETVersion;
        //->Internal public const int Internal_CannotBeUsedForValueType = (int)HpcLinqErrorCodeCategory.CodeGen + 41;
        //->Internal  public const int Internal_TypeDoesNotContainRequestedField = (int)HpcLinqErrorCodeCategory.CodeGen + 42;
        public const int CannotCreatePartitionNodeRandom = (int)HpcLinqErrorCodeCategory.CodeGen + 43;
        public const int PartitionKeysNotProvided = (int)HpcLinqErrorCodeCategory.CodeGen + 44;
        public const int PartitionKeysAreNotConsistentlyOrdered = (int)HpcLinqErrorCodeCategory.CodeGen + 45;
        public const int IsDescendingIsInconsistent = (int)HpcLinqErrorCodeCategory.CodeGen + 46;
        //DEL public const int Internal_FailedToRemoveMergeNode = (int)HpcLinqErrorCodeCategory.CodeGen + 47;
        //->Internal public const int Internal_CannotAttach = (int)HpcLinqErrorCodeCategory.CodeGen + 48;
        //->Internal public const int Internal_CannotAddTeeToNode = (int)HpcLinqErrorCodeCategory.CodeGen + 49;
        //DEL public const int Internal_ShouldNotCreateCodeForInput = (int)HpcLinqErrorCodeCategory.CodeGen + 50;
        //DEL public const int Internal_ShouldNotCreateCodeForOutput = (int)HpcLinqErrorCodeCategory.CodeGen + 51;
        //DEL public const int Internal_ShouldNotCreateCodeForConcat = (int)HpcLinqErrorCodeCategory.CodeGen + 52;
        //->Internal public const int Internal_DynamicManagerType = (int)HpcLinqErrorCodeCategory.CodeGen + 53;
        //DEL public const int Internal_ShouldNotCreateCodeForTee = (int)HpcLinqErrorCodeCategory.CodeGen + 54;
        //->Internal public const int Internal_IllegalDynamicManagerType = (int)HpcLinqErrorCodeCategory.CodeGen + 55;
        //->Internal public const int Internal_OptimizationPhaseError = (int)HpcLinqErrorCodeCategory.CodeGen + 56;
        //->Internal public const int Internal_DistinctOnlyTakesTwoArgs = (int)HpcLinqErrorCodeCategory.CodeGen + 57;
        //DEL public const int Internal_InputArityMustEqualChildren = (int)HpcLinqErrorCodeCategory.CodeGen + 58;
        //->Internal public const int Internal_AddVertexNotHandled = (int)HpcLinqErrorCodeCategory.CodeGen + 59;
        //->Internal public const int Internal_CannotBeEmpty = (int)HpcLinqErrorCodeCategory.CodeGen + 60;
        //DEL public const int Internal_MustSpecifyOutputAssemblyFileName = (int)HpcLinqErrorCodeCategory.CodeGen + 61;
        //->Internal public const int Internal_AutogeneratedAssemblyMissing = (int)HpcLinqErrorCodeCategory.CodeGen + 62;
        //DEL public const int Internal_ShouldNotCreateCodeForDummyNode = (int)HpcLinqErrorCodeCategory.CodeGen + 63;
        //->Internal public const int Internal_CannotBeUsedForReferenceType = (int)HpcLinqErrorCodeCategory.CodeGen + 64;
        public const int BadSeparatorCount = (int)HpcLinqErrorCodeCategory.CodeGen + 65;
        public const int TypeMustHaveDataMembers = (int)HpcLinqErrorCodeCategory.CodeGen + 66;
        public const int CannotHandleObjectFields = (int)HpcLinqErrorCodeCategory.CodeGen + 67;
        public const int CannotHandleDerivedtypes = (int)HpcLinqErrorCodeCategory.CodeGen + 68;
        public const int MultipleOutputsWithSameDscUri = (int)HpcLinqErrorCodeCategory.CodeGen + 69;
        public const int OutputUriAlsoQueryInput = (int)HpcLinqErrorCodeCategory.CodeGen + 70;

        //The "internal" code is used for internal errors that should not be hit by users.
        //The messages may be informative, but the error code doesn't need to be and it avoids users
        //seeing all the error codes in intellisense and/or wondering if they should catch & deal with them etc.
        public const int Internal = (int)HpcLinqErrorCodeCategory.CodeGen + 71;

        #endregion

        #region DscClient
        public const int DSCStreamError = (int) HpcLinqErrorCodeCategory.DscClient + 0;
        public const int StreamDoesNotExist = (int) HpcLinqErrorCodeCategory.DscClient + 1;
        public const int StreamAlreadyExists = (int) HpcLinqErrorCodeCategory.DscClient + 2;
        public const int AttemptToReadFromAWriteStream = (int) HpcLinqErrorCodeCategory.DscClient + 3;
        public const int FailedToCreateStream = (int) HpcLinqErrorCodeCategory.DscClient + 4;
        public const int JobToCreateTableWasCanceled = (int) HpcLinqErrorCodeCategory.DscClient + 5;
        public const int FailedToGetReadPathsForStream = (int) HpcLinqErrorCodeCategory.DscClient + 6;
        public const int CannotAccesFilePath = (int)HpcLinqErrorCodeCategory.DscClient + 7;
        public const int PositionNotSupported = (int)HpcLinqErrorCodeCategory.DscClient + 8;
        public const int GetFileSizeError = (int)HpcLinqErrorCodeCategory.DscClient + 9;
        public const int ReadFileError = (int)HpcLinqErrorCodeCategory.DscClient + 10;
        public const int UnknownCompressionScheme = (int)HpcLinqErrorCodeCategory.DscClient + 11;
        public const int WriteFileError = (int)HpcLinqErrorCodeCategory.DscClient + 12;
        public const int MultiBlockEmptyPartitionList = (int)HpcLinqErrorCodeCategory.DscClient + 13;
        public const int GetURINotSupported = (int)HpcLinqErrorCodeCategory.DscClient + 14;
        public const int SetCalcFPNotSupported = (int)HpcLinqErrorCodeCategory.DscClient + 15;
        public const int GetFPNotSupported = (int)HpcLinqErrorCodeCategory.DscClient + 16;
        public const int FailedToAllocateNewNativeBuffer = (int)HpcLinqErrorCodeCategory.DscClient + 17;
        public const int FailedToReadFromInputChannel = (int)HpcLinqErrorCodeCategory.DscClient + 18;
        public const int FailedToWriteToOutputChannel = (int)HpcLinqErrorCodeCategory.DscClient + 19;
        //DEL public const int Internal_PrefixAlreadyUsedForOtherProvider = (int)HpcLinqErrorCodeCategory.DscClient + 20;
        //->Internal public const int Internal_UnknownProvider = (int)HpcLinqErrorCodeCategory.DscClient + 21;
        //DEL public const int Internal_CannotCallPartitionInfoOnType = (int)HpcLinqErrorCodeCategory.DscClient + 22;
        //DEL public const int Internal_IllFormedUriArguments = (int)HpcLinqErrorCodeCategory.DscClient + 23;
        //DEL public const int Internal_OpenForWriteError = (int)HpcLinqErrorCodeCategory.DscClient + 24;
        public const int MultiBlockCannotAccesFilePath = (int)HpcLinqErrorCodeCategory.DscClient + 25;
        #endregion

        #region JobSubmission
        public const int DryadHomeMustBeSpecified = (int) HpcLinqErrorCodeCategory.JobSubmission + 0;
        public const int ClusterNameMustBeSpecified = (int) HpcLinqErrorCodeCategory.JobSubmission + 1;
        public const int UnexpectedJobStatus = (int) HpcLinqErrorCodeCategory.JobSubmission + 2;
        public const int JobStatusQueryError = (int) HpcLinqErrorCodeCategory.JobSubmission + 3;
        public const int JobOptionNotImplemented = (int) HpcLinqErrorCodeCategory.JobSubmission + 4;
        public const int HpcLinqJobMinMustBe2OrMore = (int) HpcLinqErrorCodeCategory.JobSubmission + 5;
        public const int SubmissionFailure = (int)HpcLinqErrorCodeCategory.JobSubmission + 6;
        public const int UnsupportedSchedulerType = (int)HpcLinqErrorCodeCategory.JobSubmission + 7;
        public const int UnsupportedExecutionKind = (int)HpcLinqErrorCodeCategory.JobSubmission + 8;
        public const int DidNotCompleteSuccessfully = (int)HpcLinqErrorCodeCategory.JobSubmission + 9;
        public const int Binaries32BitNotSupported = (int)HpcLinqErrorCodeCategory.JobSubmission + 10;
        #endregion

        #region QueryAPI
        public const int DistinctAttributeComparerNotDefined = (int) HpcLinqErrorCodeCategory.QueryAPI + 0;
        public const int SerializerTypeMustBeNonNull = (int) HpcLinqErrorCodeCategory.QueryAPI + 1;
        public const int SerializerTypeMustSupportIHpcSerializer = (int) HpcLinqErrorCodeCategory.QueryAPI + 2;
        public const int UnrecognizedOperatorName = (int) HpcLinqErrorCodeCategory.QueryAPI + 3; 
        //[deleted] #4 CouldNotInferCombiner
        //[deleted] #5 CombinerHasWrongType
        //[deleted] #6 UnknownCombiner
        public const int UnsupportedExpressionsType = (int) HpcLinqErrorCodeCategory.QueryAPI + 7;
        public const int UnsupportedExpressionType = (int) HpcLinqErrorCodeCategory.QueryAPI + 8;
        //[deleted/duplicate] #9 IndexOutOfRange
        public const int IndexTooSmall = (int)HpcLinqErrorCodeCategory.QueryAPI + 10;
        public const int MultiQueryableKeyOutOfRange = (int) HpcLinqErrorCodeCategory.QueryAPI + 11;
        public const int IndexOutOfRange = (int) HpcLinqErrorCodeCategory.QueryAPI + 12;
        //->ArgumentException public const int NotAHpcLinqQuery = (int) HpcLinqErrorCodeCategory.QueryAPI + 13;
        public const int ToDscUsedIncorrectly = (int) HpcLinqErrorCodeCategory.QueryAPI + 14;
        public const int ExpressionTypeNotHandled = (int) HpcLinqErrorCodeCategory.QueryAPI + 15;
        public const int FailedToGetStreamProps = (int) HpcLinqErrorCodeCategory.QueryAPI + 16;
        public const int MetadataRecordType = (int) HpcLinqErrorCodeCategory.QueryAPI + 17;
        //[deleted] #18 MetadataCompressionScheme
        //[deleted] #19 CannotHaveZeroPartitions
        public const int JobToCreateTableFailed = (int) HpcLinqErrorCodeCategory.QueryAPI + 20;
        //[deleted/duplicate] #21 UnrecognizedDataSource
        public const int OnlyAvailableForPhysicalData = (int) HpcLinqErrorCodeCategory.QueryAPI + 22;
        public const int FileSetMustBeSealed = (int)HpcLinqErrorCodeCategory.QueryAPI + 23;
        public const int FileSetCouldNotBeOpened = (int)HpcLinqErrorCodeCategory.QueryAPI + 24;
        public const int FileSetMustHaveAtLeastOneFile = (int)HpcLinqErrorCodeCategory.QueryAPI + 25;
        //->ArgumentException public const int AtLeastOneOperatorRequired = (int)HpcLinqErrorCodeCategory.QueryAPI + 26;
        public const int CouldNotGetClientVersion = (int)HpcLinqErrorCodeCategory.QueryAPI + 27;
        public const int CouldNotGetServerVersion = (int)HpcLinqErrorCodeCategory.QueryAPI + 28;
        public const int ContextDisposed = (int)HpcLinqErrorCodeCategory.QueryAPI + 29;
        public const int UnhandledQuery = (int)HpcLinqErrorCodeCategory.QueryAPI + 30; //@@TODO: when possible, reword the sr.txt entry.
        public const int ExpressionMustBeMethodCall= (int)HpcLinqErrorCodeCategory.QueryAPI + 31;
        public const int UntypedProviderMethodsNotSupported = (int)HpcLinqErrorCodeCategory.QueryAPI + 32;
        public const int ErrorReadingMetadata = (int)HpcLinqErrorCodeCategory.QueryAPI + 33;
        public const int MustStartFromContext = (int)HpcLinqErrorCodeCategory.QueryAPI + 34;
        public const int ToHdfsUsedIncorrectly = (int)HpcLinqErrorCodeCategory.QueryAPI + 35;
        //->Internal public const int Internal_TypeDoesNotContainMember = (int)HpcLinqErrorCodeCategory.QueryAPI + 35;
        //->Internal public const int Internal_BugInHandlingAnonymousClass = (int)HpcLinqErrorCodeCategory.QueryAPI + 36;
        //->Internal public const int Internal_FieldAnnotationIncorrectParameter = (int)HpcLinqErrorCodeCategory.QueryAPI + 37;
        //->Internal public const int Internal_UnnamedParameterExpression = (int)HpcLinqErrorCodeCategory.QueryAPI + 38;
        //DEL public const int Internal_UriIsNotDsc = (int)HpcLinqErrorCodeCategory.QueryAPI + 39;
        //->ArgumentException public const int AlreadySubmitted = (int)HpcLinqErrorCodeCategory.QueryAPI + 40;
        #endregion

        #region Serialization
        public const int FailedToReadFrom = (int) HpcLinqErrorCodeCategory.Serialization + 0;
        public const int EndOfStreamEncountered = (int) HpcLinqErrorCodeCategory.Serialization + 1;
        public const int SettingPositionNotSupported = (int) HpcLinqErrorCodeCategory.Serialization + 2;
        public const int FingerprintDisabled = (int) HpcLinqErrorCodeCategory.Serialization + 3;
        public const int RecordSizeMax2GB = (int) HpcLinqErrorCodeCategory.Serialization + 4;
        //[deleted/duplicate] #5 SettingPositionNotSupported
        public const int ReadByteNotAllowed = (int) HpcLinqErrorCodeCategory.Serialization + 6;
        public const int ReadNotAllowed = (int) HpcLinqErrorCodeCategory.Serialization + 7;
        public const int SeekNotSupported = (int) HpcLinqErrorCodeCategory.Serialization + 8;
        public const int SetLengthNotSupported = (int) HpcLinqErrorCodeCategory.Serialization + 9;
        public const int FailedToDeserialize = (int) HpcLinqErrorCodeCategory.Serialization + 10;
        public const int ChannelCannotBeReadMoreThanOnce = (int) HpcLinqErrorCodeCategory.Serialization + 11;
        //[deleted/duplicate] #12 IndexOutOfRange
        public const int WriteNotSupported = (int)HpcLinqErrorCodeCategory.Serialization + 13;
        public const int WriteByteNotSupported = (int)HpcLinqErrorCodeCategory.Serialization + 14;
        public const int CannotSerializeHpcLinqQuery = (int)HpcLinqErrorCodeCategory.Serialization + 15;
        public const int CannotSerializeObject = (int)HpcLinqErrorCodeCategory.Serialization + 16;
        public const int GeneralSerializeFailure = (int)HpcLinqErrorCodeCategory.Serialization + 17;
        //DEL public const int Internal_ShouldNotCallReset = (int)HpcLinqErrorCodeCategory.Serialization + 18;
        #endregion

        #region VertexRuntime
        public const int SourceOfMergesortMustBeMultiEnumerable = (int) HpcLinqErrorCodeCategory.VertexRuntime + 1;
        public const int ThenByNotSupported = (int) HpcLinqErrorCodeCategory.VertexRuntime + 2;
        public const int AggregateNoElements = (int) HpcLinqErrorCodeCategory.VertexRuntime + 3;
        public const int FirstNoElementsFirst = (int) HpcLinqErrorCodeCategory.VertexRuntime + 4;
        public const int SingleMoreThanOneElement = (int) HpcLinqErrorCodeCategory.VertexRuntime + 5;
        public const int SingleNoElements = (int) HpcLinqErrorCodeCategory.VertexRuntime + 6;
        public const int LastNoElements = (int) HpcLinqErrorCodeCategory.VertexRuntime + 7;
        public const int MinNoElements = (int) HpcLinqErrorCodeCategory.VertexRuntime + 8;
        public const int MaxNoElements = (int) HpcLinqErrorCodeCategory.VertexRuntime + 9;
        public const int AverageNoElements = (int) HpcLinqErrorCodeCategory.VertexRuntime + 10;
        public const int RangePartitionKeysMissing = (int) HpcLinqErrorCodeCategory.VertexRuntime + 11;
        public const int PartitionFuncReturnValueExceedsNumPorts = (int) HpcLinqErrorCodeCategory.VertexRuntime + 12;
        public const int FailureInExcept = (int) HpcLinqErrorCodeCategory.VertexRuntime + 13;
        public const int FailureInIntersect = (int) HpcLinqErrorCodeCategory.VertexRuntime + 14;
        public const int FailureInSort = (int) HpcLinqErrorCodeCategory.VertexRuntime + 15;
        public const int RangePartitionInputOutputMismatch = (int) HpcLinqErrorCodeCategory.VertexRuntime + 16;
        //DEL public const int Internal_CannotHaveMoreThanOneOutput = (int)HpcLinqErrorCodeCategory.VertexRuntime + 17;
        public const int KeyNotFound = (int)HpcLinqErrorCodeCategory.VertexRuntime + 18;
        public const int TooManyItems = (int)HpcLinqErrorCodeCategory.VertexRuntime + 19;
        public const int FailureInHashGroupBy = (int)HpcLinqErrorCodeCategory.VertexRuntime + 20;
        public const int FailureInSortGroupBy = (int)HpcLinqErrorCodeCategory.VertexRuntime + 21;
        public const int FailureInHashJoin = (int)HpcLinqErrorCodeCategory.VertexRuntime + 22;
        public const int FailureInHashGroupJoin = (int)HpcLinqErrorCodeCategory.VertexRuntime + 23;
        public const int FailureInDistinct = (int)HpcLinqErrorCodeCategory.VertexRuntime + 24;
        public const int FailureInOperator = (int)HpcLinqErrorCodeCategory.VertexRuntime + 25;
        public const int FailureInUserApplyFunction = (int)HpcLinqErrorCodeCategory.VertexRuntime + 26;
        public const int FailureInOrderedGroupBy = (int)HpcLinqErrorCodeCategory.VertexRuntime + 27;
        //DEL public const int Internal_NullSelector = (int)HpcLinqErrorCodeCategory.VertexRuntime + 28;
        //DEL public const int Internal_CannotResetIEnumerator = (int)HpcLinqErrorCodeCategory.VertexRuntime + 29;
        //DEL public const int Internal_WrongFlagCombination = (int)HpcLinqErrorCodeCategory.VertexRuntime + 30;
        //DEL public const int Internal_SourceMustBeDryadVertexReader = (int)HpcLinqErrorCodeCategory.VertexRuntime + 31;
        //->Internal public const int Internal_SortedChunkCannotBeEmpty = (int)HpcLinqErrorCodeCategory.VertexRuntime + 32;
        public const int TooManyElementsBeforeReduction = (int)HpcLinqErrorCodeCategory.VertexRuntime + 33; //@@TODO: when possible, reword the sr.txt entry.
        #endregion

        #region LocalDebug
        public const int CreatingDscDataFromLocalDebugFailed = (int)HpcLinqErrorCodeCategory.LocalDebug + 0;
        #endregion

        #region Unknown
        public const int UnknownError = (int) HpcLinqErrorCodeCategory.Unknown + 0;
        
        #endregion
              
        /// <summary>
        /// Returns the category of the specified error code
        /// </summary>
        internal static HpcLinqErrorCodeCategory Category(int code)
        {
            if ((code >= (int) HpcLinqErrorCodeCategory.QueryAPI) && (code < (int) HpcLinqErrorCodeCategory.QueryAPI+ codesPerCategory))
            {
                return HpcLinqErrorCodeCategory.QueryAPI;
            }
            else if ((code >= (int) HpcLinqErrorCodeCategory.CodeGen) && (code < (int) HpcLinqErrorCodeCategory.CodeGen+ codesPerCategory))
            {
                return HpcLinqErrorCodeCategory.CodeGen;
            }
            else if ((code >= (int) HpcLinqErrorCodeCategory.JobSubmission) && (code < (int) HpcLinqErrorCodeCategory.JobSubmission+ codesPerCategory))
            {
                return HpcLinqErrorCodeCategory.JobSubmission;
            }
            else if ((code >= (int) HpcLinqErrorCodeCategory.Serialization) && (code < (int) HpcLinqErrorCodeCategory.Serialization+ codesPerCategory))
            {
                return HpcLinqErrorCodeCategory.Serialization;
            }
            else if ((code >= (int)HpcLinqErrorCodeCategory.DscClient) && (code < (int)HpcLinqErrorCodeCategory.DscClient+ codesPerCategory))
            {
                return HpcLinqErrorCodeCategory.DscClient;
            }
            else if ((code >= (int)HpcLinqErrorCodeCategory.VertexRuntime) && (code < (int)HpcLinqErrorCodeCategory.VertexRuntime+ codesPerCategory))
            {
                return HpcLinqErrorCodeCategory.VertexRuntime;
            }
            else if ((code >= (int)HpcLinqErrorCodeCategory.LocalDebug) && (code < (int)HpcLinqErrorCodeCategory.LocalDebug+ codesPerCategory))
            {
                return HpcLinqErrorCodeCategory.LocalDebug;
            }
            else
            {
                return HpcLinqErrorCodeCategory.Unknown;
            }
        }

    }
}
