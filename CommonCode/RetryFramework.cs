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

//------------------------------------------------------------------------------
// Security review: nzeng 01-11-06
//------------------------------------------------------------------------------

#region Using directives

using System;
using System.Text;
using System.Diagnostics;
using System.Threading;

#endregion

namespace Microsoft.Hpc
{
    internal class RetryManager
    {
        public const int InfiniteRetries = -1;

        RetryWaitTimer _waitTimer;

        int _maxRetries;
        int _totalTimeLimit = Timeout.Infinite;

        int _retryCount = 0;
        int _totalWaitTime = 0;
        int _currentWaitTime = 0;

        public RetryManager(RetryWaitTimer waitTimer) : this(waitTimer, InfiniteRetries) { }
        public RetryManager(RetryWaitTimer waitTimer, int maxRetries) : this(waitTimer, maxRetries, Timeout.Infinite) { }

        public RetryManager(RetryWaitTimer waitTimer, int maxRetries, int totalTimeLimit)
        {
            if (waitTimer == null)
            {
                throw new ArgumentNullException("wait");
            }
            _waitTimer = waitTimer;

            SetMaxRetries(maxRetries);
            SetTotalTimeLimit(totalTimeLimit);
        }


        /// <summary>
        /// Gets the number of retries attempted thus far
        /// </summary>
        public int RetryCount       { get { return _retryCount;  } }

        /// <summary>
        /// Get the total spent waiting between retries
        /// </summary>
        public int ElaspsedWaitTime { get { return _totalWaitTime; } }

        /// <summary>
        /// Gets or sets the maximum number of retries
        /// </summary>
        public int MaxRetryCount
        {
            get { return _maxRetries; }
            set { SetMaxRetries(value); }
        }

        /// <summary>
        /// Gets or sets the total amount of time that may be spend waiting for retries.        
        /// </summary>
        public int TotalTimeLimit
        {
            get { return _totalTimeLimit; }
            set { SetTotalTimeLimit(value); }
        }

        void SetMaxRetries(int n)
        {
            if (n <= 0 && n != RetryManager.InfiniteRetries)
            {
                throw new ArgumentException("The maximum number of retries must be greater than zero, or RetryOperator.InfiniteRetries");
            }
            _maxRetries = n;
        }

        void SetTotalTimeLimit(int t)
        {
            if (t <= 0 && t != Timeout.Infinite)
            {
                throw new ArgumentException("The specified time must be greater than zero, or Timeout.Infinite");
            }
            _totalTimeLimit = t;
        }


        /// <summary>
        /// Returns true if there are more retries left
        /// </summary>
        public bool HasAttemptsLeft
        {
            get
            {
                return ((_maxRetries == RetryManager.InfiniteRetries || _retryCount < _maxRetries)
                        && (_totalTimeLimit == Timeout.Infinite || _totalWaitTime < _totalTimeLimit));
            }
        }

        /// <summary>
        /// Get the next wait time
        /// </summary>
        public int NextWaitTime
        {
            get
            {
                int waitTime = _waitTimer.GetNextWaitTime(_retryCount, _currentWaitTime);
                if (_totalTimeLimit != Timeout.Infinite && (_totalWaitTime + waitTime > _totalTimeLimit))
                {
                    waitTime = _totalTimeLimit - _totalWaitTime;
                }
                return waitTime;
            }
        }

        /// <summary>
        /// Increment the retry count and advance the total wait time without actually waiting
        /// </summary>
        public void SimulateNextAttempt()
        {
            WaitForNextAttempt(false);
        }


        /// <summary>
        /// Wait until the next retry by making the current thread sleep for the appropriate amount of time.
        /// May return immediately if the wait is zero.
        /// </summary>
        public void WaitForNextAttempt()
        {
            WaitForNextAttempt(true);
        }

       
        void WaitForNextAttempt(bool doSleep)
        {
            if (!HasAttemptsLeft)
            {
                throw new InvalidOperationException("There are no more retry attempts remaining");
            }

            _currentWaitTime = NextWaitTime;
            _retryCount++;

            Debug.Assert(_currentWaitTime >= 0);
            if (_currentWaitTime > 0)
            {
                if (doSleep)
                {
                    Thread.Sleep(_currentWaitTime);
                }
                _totalWaitTime += _currentWaitTime;
            }            
        }

        /// <summary>
        /// Resets the retry manager's retry count
        /// </summary>
        public void Reset()
        {
            _retryCount = 0;
            _totalWaitTime = 0;
            _currentWaitTime = 0;
        }
    }


    /// <summary>
    /// Defines how long a retry manager will wait between sub-sequent retries
    /// </summary>
    internal abstract class RetryWaitTimer
    {
        internal abstract int GetNextWaitTime(int retryCount, int currentWaitTime);
    }


    /// <summary>
    /// Instantly returns without waiting
    /// </summary>
    internal class InstantRetryTimer : RetryWaitTimer
    {
        internal override int GetNextWaitTime(int retryCount, int currentWaitTime)
        {
            return 0;
        }

        // This class should be a singleton
        private InstantRetryTimer() { }        
        
        static InstantRetryTimer _instance = new InstantRetryTimer();
        public static InstantRetryTimer Instance 
        { 
            get { return _instance; } 
        }
    }

    /// <summary>
    /// Waits a constant time between subsequent retries
    /// </summary>
    internal class PeriodicRetryTimer : RetryWaitTimer
    {
        int _period;

        public PeriodicRetryTimer(int period)
        {
            if (period < 0)
            {
                throw new ArgumentOutOfRangeException("period", "The period must be a non-negative integer (in milliseconds)");
            }
            _period = period;
        }

        internal override int GetNextWaitTime(int retryCount, int currentWaitTime)
        {
            return _period;
        }
    }

    /// <summary>
    /// A retry timer where wait time at retry n depends on the wait at retry n-1.
    /// </summary>
    internal abstract class BoundedBackoffRetryTimer : RetryWaitTimer
    {
        int _initialWait;
        int _waitUpperBound;

        protected BoundedBackoffRetryTimer(int initialWait, int waitUpperBound)
        {
            if (initialWait <= 0)
            {
                throw new ArgumentOutOfRangeException("initialWait", "Initial value must be a positive integer (in milliseconds)");
            }
            if (waitUpperBound <= 0 && waitUpperBound != Timeout.Infinite)
            {
                throw new ArgumentOutOfRangeException("waitCap", "The wait cap must be greater than zero, or Timeout.Infinite");
            }

            _initialWait = initialWait;            
            _waitUpperBound = waitUpperBound;
        }

        internal override int GetNextWaitTime(int retryCount, int currentWaitTime)
        {
            if (retryCount == 0)
            {
                return _initialWait;
            }

            int nextWaitTime = GetBackOffValue(currentWaitTime);
            if (nextWaitTime < 0)
            {
                return 0;
            }
            if (_waitUpperBound != Timeout.Infinite && nextWaitTime > _waitUpperBound)
            {
                return _waitUpperBound;
            }
            return nextWaitTime;
        }

        protected abstract int GetBackOffValue(int currentValue);
    }


    /// <summary>
    /// Wait times will increase exponentially
    /// </summary>
    internal class ExponentialBackoffRetryTimer : BoundedBackoffRetryTimer
    {
        double _growthFactor;        

        public ExponentialBackoffRetryTimer(int initialWait) : this(initialWait, Timeout.Infinite, 2) { }
        public ExponentialBackoffRetryTimer(int initialWait, int waitUpperBound) : this(initialWait, waitUpperBound, 2) { }

        public ExponentialBackoffRetryTimer(int initialWait, int waitUpperBound, double growthFactor)
            : base(initialWait, waitUpperBound)
        {
            if (growthFactor <= 0)
            {
                throw new ArgumentOutOfRangeException("growthFactor", "The growth factor must be a positive value");
            }
            _growthFactor = growthFactor;
        }

        protected override int GetBackOffValue(int currentValue)
        {
 	        return (int)Math.Round(currentValue * _growthFactor);
        }
    }

    /// <summary>
    /// Wait times will increase exponentially and also vary a bit randomly
    /// </summary>
    internal class ExponentialRandomBackoffRetryTimer : ExponentialBackoffRetryTimer
    {
        Random _rand = null;
        public ExponentialRandomBackoffRetryTimer(int initialWait) : this(initialWait, Timeout.Infinite, 2) { }
        public ExponentialRandomBackoffRetryTimer(int initialWait, int waitUpperBound) : this(initialWait, waitUpperBound, 2) { }

        public ExponentialRandomBackoffRetryTimer(int initialWait, int waitUpperBound, double growthFactor)
            : base(initialWait, waitUpperBound,growthFactor)
        {
            _rand = new Random();
        }

        protected override int GetBackOffValue(int currentValue)
        {
            return ((int)base.GetBackOffValue(currentValue)) + _rand.Next(0, currentValue);
        }
    }


    /// <summary>
    /// Wait times will increase linearly
    /// </summary>
    internal class LinearBackoffRetryTimer : BoundedBackoffRetryTimer
    {
        int _increment;

        public LinearBackoffRetryTimer(int initialWait) : this(initialWait, Timeout.Infinite, initialWait) { }
        public LinearBackoffRetryTimer(int initialWait, int waitUpperBound) : this(initialWait, waitUpperBound, initialWait) { }

        public LinearBackoffRetryTimer(int initialWait, int waitUpperBound, int increment)
            : base(initialWait, waitUpperBound)
        {
            _increment = increment;
        }

        protected override int GetBackOffValue(int currentValue)
        {
            return currentValue + _increment;
        }
    }
}