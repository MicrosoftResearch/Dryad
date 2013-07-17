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

#include <DrStageHeaders.h>

static const double s_conservativeOutlierFraction = 0.5;
static const double s_outlierFraction = 0.20;
static const double s_outlierThresholdInSigmas = 3.0;
static const int s_numberOfTrials = 10;
static const double s_conditionThreshold = 10000.0;

static const int s_firstEstimationPercentage = 50;
static const int s_reEstimationPercentage = 5;

void DrStageStatistics::Measurement::Initialize(DrResourcePtr machine,
                                                DrVertexExecutionStatisticsPtr statistics)
{
    m_machine = machine;
    m_elapsed = (double) (statistics->m_completionTime - statistics->m_runningTime);
    m_dataSize = (double) (statistics->m_totalInputData->m_dataRead);
    m_deviation = 0.0;
}

DrStageStatistics::DrStageStatistics()
{
    m_numberOfStarted = 0;
    m_measurement = DrNew MeasurementList();

    m_sampleSize = 0;
    m_nextReEstimationPercentage = s_firstEstimationPercentage;

    m_startup = 0.0;
    m_dataMultiplier = 0.0;
    m_stdDeviation = 0.0;
    m_relativeStdDev = 1.0;
    m_numberOfOutliers = 0;

    m_gotEstimate = false;
    m_nonParametricOutlierEstimate = DrTimeInterval_Infinite;
    m_reportedFinalStatistics = false;
    m_dumpedRawStatisticsData = false;

    m_name = "(No name)";
}

void DrStageStatistics::SetName(DrString name)
{
    m_name = name;
}

DrString DrStageStatistics::GetName()
{
    return m_name;
}

void DrStageStatistics::ComputeNextEstimationThreshold()
{
    int nextReEstimation;

    do {
        nextReEstimation = (m_sampleSize * m_nextReEstimationPercentage) / 100;
        if (nextReEstimation < 2)
        {
            nextReEstimation = 2;
        }
        if (nextReEstimation < m_measurement->Size()+1)
        {
            /* note this can go over 100% because there can be more
               executions than m_sampleSize due to re-executions of
               the same vertex */
            m_nextReEstimationPercentage += s_reEstimationPercentage;
        }
    } while (nextReEstimation < m_measurement->Size()+1);
}

DrTimeInterval DrStageStatistics::GetOutlierThreshold(DrGraphParametersPtr params)
{
    if (m_sampleSize <= params->m_duplicateEverythingThreshold)
    {
        DrTimeInterval defaultThreshold = params->m_defaultOutlierThreshold;
        if (m_nonParametricOutlierEstimate < defaultThreshold)
        {
            return m_nonParametricOutlierEstimate;
        }
        else
        {
            return defaultThreshold;
        }
    }
    else
    {
        return m_nonParametricOutlierEstimate;
    }
}

void DrStageStatistics::SetSampleSize(int sampleSize)
{
    m_sampleSize = sampleSize;

    /* recompute the threshold for the next re-estimation */
    int m_nextReEstimationPercentage = s_firstEstimationPercentage;
    ComputeNextEstimationThreshold();

    if (m_nextReEstimationPercentage == s_firstEstimationPercentage)
    {
        /* we don't have enough datapoints for a good estimate yet */
        m_gotEstimate = false;
        m_nonParametricOutlierEstimate = DrTimeInterval_Infinite;
    }
}

void DrStageStatistics::IncrementSampleSize()
{
    SetSampleSize(m_sampleSize+1);
}

void DrStageStatistics::DecrementSampleSize()
{
    DrAssert(m_sampleSize > 0);
    SetSampleSize(m_sampleSize-1);
}

void DrStageStatistics::IncrementStartedCount()
{
    ++m_numberOfStarted;
}

void DrStageStatistics::DecrementStartedCount()
{
    DrAssert(m_numberOfStarted > 0);
    --m_numberOfStarted;
}

void DrStageStatistics::AddMeasurement(DrGraphParametersPtr params, DrResourcePtr machine,
                                       DrVertexExecutionStatisticsPtr statistics)
{
    MeasurementRef m = DrNew Measurement();
    m->Initialize(machine, statistics);

    m_measurement->Add(m);

    int nextReEstimation = (m_sampleSize * m_nextReEstimationPercentage) / 100;
    if (nextReEstimation < 2)
    {
        nextReEstimation = 2;
    }

    if (m_measurement->Size() == nextReEstimation)
    {
        DrLogI("Re-estimating stage statistics Stage %s", m_name.GetChars());

        ReEstimate(params);

        DrTimeInterval tiStartup = (DrTimeInterval) m_startup;
        DrTimeInterval tiMultiplier = (DrTimeInterval) m_dataMultiplier;
        tiMultiplier = tiMultiplier / (1024*1024);
        DrTimeInterval tiStdDev = (DrTimeInterval) m_stdDeviation;

        DrLogI("Got new stage model estimate Stage=%s startup=%lf multiplier=%lf/MB std.dev=%lf "
               "relative std.dev=%lf number of outliers=%d",
               m_name.GetChars(),
               (double) tiStartup / (double) DrTimeInterval_Second,
               (double) tiMultiplier / (double) DrTimeInterval_Second,
               (double) tiStdDev / (double) DrTimeInterval_Second,
               m_relativeStdDev, m_numberOfOutliers);

        ComputeNextEstimationThreshold();
    }
    else
    {
        DrAssert(m_measurement->Size() < nextReEstimation);
    }
}

DRCLASS(DrSignedDeviationComparer) : public DrComparer<DrStageStatistics::MeasurementRef>
{
public:
    virtual int Compare(DrStageStatistics::MeasurementRef aM, DrStageStatistics::MeasurementRef bM) DROVERRIDE
    {
        double a = aM->m_deviation;
        double b = bM->m_deviation;

        return ((a < b) ? (-1) : ((a > b) ? (1) : (0)));
    }
};
DRREF(DrSignedDeviationComparer);

void DrStageStatistics::SortSignedDeviations()
{
    DrSignedDeviationComparerRef comparer = DrNew DrSignedDeviationComparer();
    m_measurement->Sort(comparer);
}

#include <math.h>

DRCLASS(DrUnSignedDeviationComparer) : public DrComparer<DrStageStatistics::MeasurementRef>
{
public:
    virtual int Compare(DrStageStatistics::MeasurementRef aM, DrStageStatistics::MeasurementRef bM) DROVERRIDE
    {
        double a = fabs(aM->m_deviation);
        double b = fabs(bM->m_deviation);

        return ((a < b) ? (-1) : ((a > b) ? (1) : (0)));
    }
};
DRREF(DrUnSignedDeviationComparer);

void DrStageStatistics::SortUnsignedDeviations()
{
    DrUnSignedDeviationComparerRef comparer = DrNew DrUnSignedDeviationComparer();
    m_measurement->Sort(comparer);
}

DRCLASS(DrElapsedComparer) : public DrComparer<DrStageStatistics::MeasurementRef>
{
public:
    virtual int Compare(DrStageStatistics::MeasurementRef aM, DrStageStatistics::MeasurementRef bM) DROVERRIDE
    {
        double a = aM->m_elapsed;
        double b = bM->m_elapsed;

        return ((a < b) ? (-1) : ((a > b) ? (1) : (0)));
    }
};
DRREF(DrElapsedComparer);

void DrStageStatistics::SortElapsed()
{
    DrElapsedComparerRef comparer = DrNew DrElapsedComparer();
    m_measurement->Sort(comparer);
}

void DrStageStatistics::ComputeDeviations(double startup, double dataMultiplier, double stdDeviation)
{
    double outlierThreshold = stdDeviation * s_outlierThresholdInSigmas;

    dataMultiplier *= m_dataSizeScaler;

    m_numberOfOutliers = 0;
    int i;
    for (i=0; i<m_measurement->Size(); ++i)
    {
        double expectedTime = startup + m_measurement[i]->m_dataSize * dataMultiplier;
        double deviation = m_measurement[i]->m_elapsed - expectedTime;

        m_measurement[i]->m_deviation = deviation;

        if (deviation > outlierThreshold)
        {
            ++m_numberOfOutliers;
        }
    }
}

double DrStageStatistics::StdDevFromMeasurementPrefix(int prefixLength)
{
    DrAssert(prefixLength <= m_measurement->Size());

    double totalSquared = 0.0;
    int i;
    for (i=0; i<prefixLength; ++i)
    {
        totalSquared += m_measurement[i]->m_deviation * m_measurement[i]->m_deviation;
    }

    double meanSquared = totalSquared / (double) prefixLength;

    return sqrt(meanSquared);
}

void DrStageStatistics::ReflectDeviationPrefix(int prefixLength)
{
    DrAssert(prefixLength <= m_measurement->Size());

    int suffixLength = m_measurement->Size() - prefixLength;
    /* the prefix must be at least half the total number of measurements */
    DrAssert(suffixLength <= prefixLength);

    int i;
    for (i=0; i<suffixLength; ++i)
    {
        m_measurement[m_measurement->Size()-1 - i]->m_deviation = m_measurement[i]->m_deviation;
    }
}

void DrStageStatistics::LinearRegression(MeasurementListRef measurementArray,
                                         int prefixLength, double dataSizeScaler,
                                         double& pStartup /* OUT */, double& pDataMultiplier /* OUT */)
{
    /* we'll estimate the best least-squares approximation for x
       satisfying Ax = b where:

       A^T = ( 1          1          ... 1                       )
             ( dataSize_0 dataSize_1 ... dataSize_prefixLength-1 )

       b^T = ( elapsed_0 elapsed_1 ... elapsed_prefixLength-1 )

       we do this via linear regression so \hat{x} = (A^T A)^-1 A^T b
       where:

       \hat{x}^T = ( startup dataMultiplier )

       (A^T A) = ( unitSum     dataSizeSum        )
                 ( dataSizeSum dataSizeSquaredSum )

       (A^T A)^-1 = 1/determinant ( dataSizeSquaredSum -dataSizeSum )
                                  ( -dataSizeSum       unitSum      )

       (A^T B)^T = ( elapsedSum dataSizeElapsedProductSum )
    */

    double unitSum = (double) prefixLength;
    double dataSizeSum = 0.0;
    double dataSizeSquaredSum = 0.0;
    double elapsedSum = 0.0;
    double dataSizeElapsedProductSum = 0.0;

    int i;
    for (i=0; i<prefixLength; ++i)
    {
        double scaledDataSize = measurementArray[i]->m_dataSize * dataSizeScaler;

        dataSizeSum += (scaledDataSize);
        dataSizeSquaredSum += (scaledDataSize * scaledDataSize);
        elapsedSum += (measurementArray[i]->m_elapsed);
        dataSizeElapsedProductSum += (scaledDataSize * measurementArray[i]->m_elapsed);
    }

    /* if the data sizes are almost equal then this calculation will
       be ill-conditioned and we will just set the data size
       multiplier to be zero and absorb everything into the startup
       estimate. Let C = (A^T A), i.e. the matrix we need to
       invert. Then the condition number of C is |C^-1| |C| where we
       use the l_infinity norm for |.| so

         | (a b) | = max(|a|,|b|,|c|,|d|)
         | (c d) |

       In this case since C is symmetric we denote it

         (a b)
         (b d)

       and we are looking for

         |(a b)| |1/det(d -b)|
         |(b d)| |     (-b a)|

         = z^2/|det| where z = max(|a|,|b|,|d|)

       The system is therefore ill-conditioned if

       z^2 > conditionThreshold |det|
    */

    double absa = fabs(unitSum);
    double absb = fabs(dataSizeSum);
    double absd = fabs(dataSizeSquaredSum);
    double z = (absa > absb) ? absa : absb;
    z = (z > absd) ? z : absd;

    double determinant = unitSum*dataSizeSquaredSum - dataSizeSum*dataSizeSum;

    if (z*z > (s_conditionThreshold * fabs(determinant)))
    {
        /* this means the data sizes are close to equal, so we'll just
           estimate everything as being startup time */
        pStartup = elapsedSum / unitSum;
        pDataMultiplier = 0.0;
    }
    else
    {
        double unnormalizedStartup = dataSizeSquaredSum * elapsedSum + -dataSizeSum * dataSizeElapsedProductSum;
        double unnormalizedDataSize = -dataSizeSum * elapsedSum + unitSum * dataSizeElapsedProductSum;

        pStartup = unnormalizedStartup / determinant;
        pDataMultiplier = unnormalizedDataSize / determinant;
    }
}

void DrStageStatistics::RandomlySample(int robustEstimatePrefix)
{
    DrAssert(m_measurement->Size() > 1);

    double minStdDev = 0.0;

    int i;
    for (i=0; i<s_numberOfTrials; ++i)
    {
        int p1 = rand() % m_measurement->Size();
        int p2;
        do {
            p2 = rand() % m_measurement->Size();
        } while (p1 == p2);

        MeasurementListRef m = DrNew MeasurementList();

        m->Add(m_measurement[p1]);
        m->Add(m_measurement[p2]);

        double startup, dataMultiplier;
        LinearRegression(m, 2, m_dataSizeScaler, startup, dataMultiplier);

        ComputeDeviations(startup, dataMultiplier, 0.0);
        SortUnsignedDeviations();
        double trialStdDev = StdDevFromMeasurementPrefix(robustEstimatePrefix);

        if (i == 0 || trialStdDev < minStdDev)
        {
            minStdDev = trialStdDev;
            m_startup = startup;
            m_dataMultiplier = dataMultiplier;
        }
    }
}

void DrStageStatistics::ComputeRelativeStandardDeviation()
{
    /* get the median elapsed time */
    int medianMeasurement = m_measurement->Size()/2;
    double medianElapsed = m_measurement[medianMeasurement]->m_elapsed;

    /* now compute the std. deviation relative to the median elapsed
       time: this is a proxy for how well we are estimating: the
       smaller the better */
    if (medianElapsed == 0.0)
    {
        /* just conceivably everything could be finishing so fast that
           we don't witness any running time, in which case we'll just
           punt on the relative error */
        m_relativeStdDev = 1.0;
    }
    else
    {
        m_relativeStdDev = m_stdDeviation / medianElapsed;
    }
}

void DrStageStatistics::GetDataSizeScaler()
{
    /* for stability in the linear regression, it's good to have the
       data sizes come out looking close to 1, so we'll find the max
       data size and scale everything by it */
    double maxDataSize = m_measurement[0]->m_dataSize;

    int i;
    for (i=1; i<m_measurement->Size(); ++i)
    {
        if (m_measurement[i]->m_dataSize > maxDataSize)
        {
            maxDataSize = m_measurement[i]->m_dataSize;
        }
    }

    if (maxDataSize == 0.0)
    {
        m_dataSizeScaler = 1.0;
    }
    else
    {
        m_dataSizeScaler = 1.0 / maxDataSize;
    }
}

void DrStageStatistics::ReEstimate(DrGraphParametersPtr params)
{
    DrAssert(m_measurement->Size() > 1);

    int conservativeOutlierCount = (int) ((double) m_measurement->Size() * s_conservativeOutlierFraction);
    int robustEstimatePrefix = m_measurement->Size() - conservativeOutlierCount;

    int outlierCount = (int) ((double) m_measurement->Size() * s_outlierFraction);
    int reflectionPrefix = m_measurement->Size() - outlierCount;

    GetDataSizeScaler();

    /* first try a bunch of candidate pairs of points to get a good
       robust rough estimate of the startup and multiplier */
    RandomlySample(robustEstimatePrefix);

    /* get the deviations based on the best rough estimates we tried
       during random sampling */
    ComputeDeviations(m_startup, m_dataMultiplier, 0.0);

    /* this replaces m_startup and m_dataMultiplier with a
       regression based on the prefix of measurements with smallest
       deviation */
    SortUnsignedDeviations();
    double startup, dataMultiplier;
    LinearRegression(m_measurement, robustEstimatePrefix,
                     m_dataSizeScaler, startup, dataMultiplier);
    m_startup = startup;
    m_dataMultiplier = dataMultiplier;

    /* now we have the best estimate we're going to get of the startup
       and multiplier, let's try to get a decent estimate of the
       noise. */

    /* First recompute the deviations again based on our good
       parameter estimates */
    ComputeDeviations(m_startup, m_dataMultiplier, 0.0);

    /* replace the largest deviations by reflection. The assumption
       here is that we may have picked up some outlier measurements,
       but that all outliers are biased to be slow not fast. Therefore
       if we throw away the slowest ones we get a better estimate, but
       then it would be biased. To remove the bias, we replace them by
       reflection with the matching fastest ones, which we believe are
       not outliers and therefore fair draws from the real Gaussian
       distribution */
    SortSignedDeviations();
    ReflectDeviationPrefix(reflectionPrefix);

    /* after the reflection, we use all the available deviations when
       estimating the standard deviation */
    m_stdDeviation = StdDevFromMeasurementPrefix(m_measurement->Size());

    /* compute the deviations one last time since a side-effect of
       this is that we count the number of outliers */
    ComputeDeviations(m_startup, m_dataMultiplier, m_stdDeviation);

    /* now let's see how good our estimate is. First sort by elapsed
       time */
    SortElapsed();

    /* compute the std. deviation relative to the median elapsed time:
       this is a proxy for how well we are estimating: the smaller the
       better */
    ComputeRelativeStandardDeviation();

    /* rescale the data multiplier to its true value */
    m_dataMultiplier *= m_dataSizeScaler;

    if (params != DrNull)
    {
        int thresholdIndex = (int) ((double) m_numberOfStarted * params->m_nonParametricThresholdFraction);

        if (thresholdIndex < m_measurement->Size())
        {
            DrTimeInterval minThreshold = params->m_minOutlierThreshold;

            m_nonParametricOutlierEstimate =
                (DrTimeInterval) m_measurement[thresholdIndex]->m_elapsed;

            DrLogI("Computed new non-parametric estimate %lf",
                   (double) m_nonParametricOutlierEstimate / (double) DrTimeInterval_Second);

            if (m_nonParametricOutlierEstimate < minThreshold)
            {
                m_nonParametricOutlierEstimate = minThreshold;

                DrLogI("Reset non-parametric estimate to minimum %lf",
                       (double) m_nonParametricOutlierEstimate / (double) DrTimeInterval_Second);
            }
        }
        else
        {
            /* don't update the estimate. We may already have an estimate
               from a previous call to ReEstimate though */
            DrLogI("Not recomputing new non-parametric estimate. "
                   "Number started=%d Fraction=%lf Completed=%d Estimate=%lf",
                   m_numberOfStarted, params->m_nonParametricThresholdFraction,
                   m_measurement->Size(),
                   (double) m_nonParametricOutlierEstimate / (double) DrTimeInterval_Second);
        }
    }

    m_gotEstimate = true;
}

void DrStageStatistics::ReportFinalStatistics(FILE* f)
{
    if (m_reportedFinalStatistics)
    {
        /* this class may be attached to more than one stage manager,
           and each one will tell it to report but we only want to do
           it once */
        return;
    }

    m_reportedFinalStatistics = true;

    if (m_measurement->Size() < 2)
    {
        fprintf(f, "Final statistics for stage %s unavailable: %s collected\n\n", m_name.GetChars(),
                (m_measurement->Size() == 0) ? "no measurements" : "only 1 measurement");
        return;
    }

    ReEstimate(DrNull);

    fprintf(f, "Final statistics for stage %s: %d measurements, %d vertices\n",
            m_name.GetChars(), m_measurement->Size(), m_sampleSize);

    DrTimeInterval tiStartup = (DrTimeInterval) m_startup;
    DrTimeInterval tiMultiplier = (DrTimeInterval) (m_dataMultiplier * (1024.0*1024.0));
    DrTimeInterval tiStdDev = (DrTimeInterval) m_stdDeviation;

    fprintf(f, "Model estimate:\n"
            "startup=%lf multiplier=%lf/MB std.dev=%lf\n"
            "relative std.dev=%lf number of outliers=%d\n\n",
            (double) tiStartup / (double) DrTimeInterval_Second,
            (double) tiMultiplier / (double) DrTimeInterval_Second,
            (double) tiStdDev / (double) DrTimeInterval_Second,
            m_relativeStdDev, m_numberOfOutliers);
}

void DrStageStatistics::DumpRawStatisticsData(FILE* f)
{
    if (m_dumpedRawStatisticsData)
    {
        /* this class may be attached to more than one stage manager,
           and each one will tell it to report but we only want to do
           it once */
        return;
    }

    m_dumpedRawStatisticsData = true;

    fprintf(f,
            "Raw statistics for stage %s: %d measurements, %d vertices\n",
            m_name.GetChars(), m_measurement->Size(), m_sampleSize);

    int i;
    for (i=0; i<m_measurement->Size(); ++i)
    {
        fprintf(f, "%s%s,%I64u,%I64u",
                (i == 0) ? "" : ",",
                m_measurement[i]->m_machine->GetName().GetChars(),
                (UINT64) m_measurement[i]->m_dataSize,
                (UINT64) m_measurement[i]->m_elapsed);
    }
    fprintf(f, "\n\n");
}
