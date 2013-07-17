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

namespace Microsoft.Research.Dryad
{
    using System;
    using System.Threading;
    using Microsoft.Research.Dryad;

    public class JobStatus
    {
        private int m_progressStepsCompleted = 0;
        private int m_totalProgressSteps = 0;
        private ISchedulerHelper m_schedulerHelper = null;

        public JobStatus(ISchedulerHelper helper)
        {
            m_schedulerHelper = helper;
        }

        public void IncrementProgress(string message)
        {
            Interlocked.Increment(ref m_progressStepsCompleted);
            ShowProgress(message, false);
        }

        public void IncrementTotalSteps(bool update)
        {
            Interlocked.Increment(ref m_totalProgressSteps);
            if (update)
            {
                ShowProgress(null, false);
            }
        }

        public void DecrementTotalSteps(bool update)
        {
            Interlocked.Decrement(ref m_totalProgressSteps);
            if (update)
            {
                ShowProgress(null, false);
            }
        }


        public void ResetProgress(int totalSteps, bool update)
        {
            Interlocked.Exchange(ref m_totalProgressSteps, totalSteps);
            if (update)
            {
                ShowProgress(null, false);
            }
        }

        public void CompleteProgress(string message)
        {
            ShowProgress(message, true);
        }

        private void ShowProgress(string message, bool finished)
        {
            Int32 nPercent = 0;
            // Progress is incremented as active vertices complete, when they're all done
            // the GM still has to seal the output stream, which may take a nontrivial amount 
            // of time, so scale to 99% until the final progress update.
            double scalingFactor = finished ? 100.0 : 99.0;

            try
            {
                nPercent = Convert.ToInt32(Convert.ToDouble(m_progressStepsCompleted) / Convert.ToDouble(m_totalProgressSteps) * scalingFactor);
                DryadLogger.LogDebug("Set Job Progress", "{0} percent complete", nPercent);
            }
            catch (OverflowException e)
            {
                DryadLogger.LogWarning("Set Job Progress", "OverflowException calculating percent complete: {0}", e.ToString());
                nPercent = 100;
            }

            if (nPercent > 100)
            {
                DryadLogger.LogWarning("Set Job Progress", "Percent complete greater than 100: {0} / {1} steps reported complete", m_progressStepsCompleted, m_totalProgressSteps);
                nPercent = 100;
            }

            try
            {
                if (message == null)
                {
                    message = String.Empty;
                }
                else if (message.Length > 80)
                {
                    // Job progress messages have max length of 80
                    message = message.Substring(0, 80);
                }
                m_schedulerHelper.SetJobProgress(nPercent, message);
            }
            catch (Exception e)
            {
                DryadLogger.LogWarning("Set Job Progress", "Failed to set job progress: {0}", e.ToString());
            }
        }

        public void SetProgress(int completedSteps, string message)
        {
            Interlocked.Exchange(ref m_progressStepsCompleted, completedSteps);
            ShowProgress(message, false);
        }

    }
}
