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

DRBASECLASS(DrStageStatistics)
{
public:
    DrStageStatistics();

    /* the name is used as a text string to identify this class of
       statistics */
    void SetName(DrString name);
    DrString GetName();

    /* tell the class how many vertices there are in the sample
       set. This number can be reset up or down even after
       measurements have started coming in. If this number goes up,
       then the number of measurements may fall below 50% of the
       sample size in which case GotEstimate will start returning
       false even if it returned true before */
    void SetSampleSize(int sampleSize);
    void IncrementSampleSize();
    void DecrementSampleSize();
    void IncrementStartedCount();
    void DecrementStartedCount();

    /* add a new measurement. The client can add more measurements
       than sampleSize e.g. because of re-executions */
    void AddMeasurement(DrGraphParametersPtr params, DrResourcePtr machine,
                        DrVertexExecutionStatisticsPtr statistics);

    /* this returns the class' current best estimate for an outlier
       threshold. This may be CsTimeInterval_Infinite if insufficient
       data has been gathered so far to figure it out. The graph is
       passed in to read thresholds out of. */
    DrTimeInterval GetOutlierThreshold(DrGraphParametersPtr params);

    void ReportFinalStatistics(FILE* f);
    void DumpRawStatisticsData(FILE* f);

    DRINTERNALBASECLASS(Measurement)
    {
    public:
        void Initialize(DrResourcePtr machine,
                        DrVertexExecutionStatisticsPtr statistics);

        DrResourceRef       m_machine;
        double              m_elapsed;
        double              m_dataSize;
        double              m_deviation;
    };
    DRREF(Measurement);

private:
    typedef DrArrayList<MeasurementRef> MeasurementList;
    DRAREF(MeasurementList,MeasurementRef);

    void SortSignedDeviations();
    void SortUnsignedDeviations();
    void SortElapsed();
    void GetDataSizeScaler();
    void ComputeNextEstimationThreshold();
    void ComputeDeviations(double startup, double dataMultiplier, double stdDeviation);
    double StdDevFromMeasurementPrefix(int prefixLength);
    void ReflectDeviationPrefix(int prefixLength);
    void LinearRegression(MeasurementListRef measurementArray, int prefixLength, double dataSizeScaler,
                          double& pStartup /* OUT */, double& pDataMultiplier /* OUT */);
    void RandomlySample(int robustEstimatePrefix);
    void ComputeRelativeStandardDeviation();
    void ReEstimate(DrGraphParametersPtr params);

    /* a string to print out to identify these statistics */
    DrString             m_name;

    /* this is the total number of vertices in the group of vertices
       that we are considering together for the purposes of gathering
       statistics. The number of measurements we see may be greater
       than m_sampleSize because vertices can be executed more than
       once. */
    int                  m_sampleSize;

    /* this is a list of measurements we have seen */
    MeasurementListRef   m_measurement;

    /* this is the number of vertices that have started (and possibly
       completed). It can go down if a vertex fails. */
    int                  m_numberOfStarted;

    /* this is the next percentage of completed executions at which we
       are going to re-estimate the model. This starts at 50% then
       increases in 5% increments */
    int                  m_nextReEstimationPercentage;

    /* this is true if we have an estimate of the model: in practice
       due to the behavior of m_nextReEstimationPercentage this is
       true whenever the number of measurements received is over 50%
       of the sample size */
    bool                 m_gotEstimate;

    /* this is the current non-parametric estimate for the outlier
       threshold, or DrTimeInterval_Infinite if we haven't seen enough
       data to say yet */
    DrTimeInterval       m_nonParametricOutlierEstimate;

    /* this is our estimate of the model parameters for execution
       time. The model says that a vertex with x bytes of input data
       will take time

       m_startup + m_dataMultiplier*x + \nu * m_stdDeviation

       where \nu is iid unit Gaussian-distributed noise

    */
    double               m_startup;
    double               m_dataMultiplier;
    double               m_stdDeviation;

    /* for numerial stability we rescale the data sizes to be close to
       1 during internal computations: this is the scaler we use */
    double               m_dataSizeScaler;

    /* this is the standard deviation relative to the median elapsed
       time, which is a general-purpose estimate for how well we are
       modeling the data */
    double               m_relativeStdDev;

    /* this is the number of measurements more than 3 sigmas away from
       the model prediction */
    int                  m_numberOfOutliers;

    /* this class may be attached to more than one stage manager, and
       each one will tell it to report but we only want to do it
       once. These flags record whether we've dumped yet. */
    bool                 m_reportedFinalStatistics;
    bool                 m_dumpedRawStatisticsData;
};
DRREF(DrStageStatistics);
