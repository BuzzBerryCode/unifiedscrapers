import React, { useState, useEffect } from 'react';
import { 
  CheckCircleIcon, 
  XCircleIcon, 
  ClockIcon, 
  ArrowPathIcon,
  PlayIcon,
  PauseIcon
} from '@heroicons/react/24/outline';
import { Job } from '@/types';

interface JobProgressTrackerProps {
  job: Job;
  darkMode: boolean;
  onResume?: (jobId: string) => void;
  onCancel?: (jobId: string) => void;
}

export default function JobProgressTracker({ job, darkMode, onResume, onCancel }: JobProgressTrackerProps) {
  const [showDetails, setShowDetails] = useState(false);
  const [errorBreakdown, setErrorBreakdown] = useState<{[key: string]: number}>({});

  // Calculate progress percentage
  const processedItems = job.processed_items || 0;
  const totalItems = job.total_items || 0;
  const failedItems = job.failed_items || 0;
  const progressPercentage = totalItems > 0 ? (processedItems / totalItems) * 100 : 0;
  const successItems = processedItems - failedItems;

  // Analyze error patterns
  useEffect(() => {
    if (job.results?.failed) {
      const breakdown: {[key: string]: number} = {};
      job.results.failed.forEach(error => {
        const errorType = categorizeError(error);
        breakdown[errorType] = (breakdown[errorType] || 0) + 1;
      });
      setErrorBreakdown(breakdown);
    }
  }, [job.results?.failed]);

  const categorizeError = (error: string): string => {
    const lowerError = error.toLowerCase();
    if (lowerError.includes('timeout')) return 'Timeout';
    if (lowerError.includes('rate limit') || lowerError.includes('429')) return 'Rate Limit';
    if (lowerError.includes('api')) return 'API Error';
    if (lowerError.includes('database') || lowerError.includes('supabase')) return 'Database';
    if (lowerError.includes('network') || lowerError.includes('connection')) return 'Network';
    return 'Unknown';
  };

  const getStatusIcon = () => {
    switch (job.status) {
      case 'running':
        return <ArrowPathIcon className="h-5 w-5 text-blue-500 animate-spin" />;
      case 'completed':
        return <CheckCircleIcon className="h-5 w-5 text-green-500" />;
      case 'failed':
        return <XCircleIcon className="h-5 w-5 text-red-500" />;
      case 'cancelled':
        return <PauseIcon className="h-5 w-5 text-yellow-500" />;
      default:
        return <ClockIcon className="h-5 w-5 text-gray-500" />;
    }
  };

  const getStatusColor = () => {
    switch (job.status) {
      case 'running': return 'border-blue-500 bg-blue-50 dark:bg-blue-900/20';
      case 'completed': return 'border-green-500 bg-green-50 dark:bg-green-900/20';
      case 'failed': return 'border-red-500 bg-red-50 dark:bg-red-900/20';
      case 'cancelled': return 'border-yellow-500 bg-yellow-50 dark:bg-yellow-900/20';
      default: return 'border-gray-300 bg-gray-50 dark:bg-gray-800';
    }
  };

  const formatTime = (timestamp: string) => {
    return new Date(timestamp).toLocaleString();
  };

  const getErrorTypeColor = (errorType: string) => {
    const colors: {[key: string]: string} = {
      'Timeout': 'bg-orange-100 text-orange-800 dark:bg-orange-900/20 dark:text-orange-300',
      'Rate Limit': 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/20 dark:text-yellow-300',
      'API Error': 'bg-red-100 text-red-800 dark:bg-red-900/20 dark:text-red-300',
      'Database': 'bg-purple-100 text-purple-800 dark:bg-purple-900/20 dark:text-purple-300',
      'Network': 'bg-blue-100 text-blue-800 dark:bg-blue-900/20 dark:text-blue-300',
      'Unknown': 'bg-gray-100 text-gray-800 dark:bg-gray-900/20 dark:text-gray-300'
    };
    return colors[errorType] || colors['Unknown'];
  };

  return (
    <div className={`border-2 rounded-lg p-4 mb-4 ${getStatusColor()}`}>
      {/* Header */}
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center space-x-3">
          {getStatusIcon()}
          <div>
            <h3 className={`font-semibold ${darkMode ? 'text-white' : 'text-gray-900'}`}>
              {job.description}
            </h3>
            <p className={`text-sm ${darkMode ? 'text-gray-300' : 'text-gray-600'}`}>
              Started: {formatTime(job.created_at)}
            </p>
          </div>
        </div>
        
        <div className="flex items-center space-x-2">
          {job.status === 'cancelled' && onResume && (
            <button
              onClick={() => onResume(job.id)}
              className="px-3 py-1 bg-blue-500 text-white rounded text-sm hover:bg-blue-600 flex items-center space-x-1"
            >
              <PlayIcon className="h-4 w-4" />
              <span>Resume</span>
            </button>
          )}
          
          {job.status === 'running' && onCancel && (
            <button
              onClick={() => onCancel(job.id)}
              className="px-3 py-1 bg-red-500 text-white rounded text-sm hover:bg-red-600 flex items-center space-x-1"
            >
              <PauseIcon className="h-4 w-4" />
              <span>Cancel</span>
            </button>
          )}
          
          <button
            onClick={() => setShowDetails(!showDetails)}
            className={`px-3 py-1 rounded text-sm ${
              darkMode 
                ? 'bg-gray-700 text-white hover:bg-gray-600' 
                : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
            }`}
          >
            {showDetails ? 'Hide' : 'Show'} Details
          </button>
        </div>
      </div>

      {/* Progress Bar */}
      <div className="mb-3">
        <div className="flex justify-between text-sm mb-1">
          <span className={darkMode ? 'text-gray-300' : 'text-gray-600'}>
            Progress: {processedItems}/{totalItems}
          </span>
          <span className={darkMode ? 'text-gray-300' : 'text-gray-600'}>
            {progressPercentage.toFixed(1)}%
          </span>
        </div>
        
        <div className={`w-full bg-gray-200 rounded-full h-3 ${darkMode ? 'bg-gray-700' : ''}`}>
          <div className="relative h-3 rounded-full overflow-hidden">
            {/* Success portion */}
            <div 
              className="absolute left-0 top-0 h-full bg-green-500 transition-all duration-300"
              style={{ width: `${totalItems > 0 ? (successItems / totalItems) * 100 : 0}%` }}
            ></div>
            
            {/* Failed portion */}
            <div 
              className="absolute top-0 h-full bg-red-500 transition-all duration-300"
              style={{ 
                left: `${totalItems > 0 ? (successItems / totalItems) * 100 : 0}%`,
                width: `${totalItems > 0 ? (failedItems / totalItems) * 100 : 0}%`
              }}
            ></div>
          </div>
        </div>
        
        <div className="flex justify-between text-xs mt-1">
          <span className="text-green-600 dark:text-green-400">
            ✅ {successItems} successful
          </span>
          {failedItems > 0 && (
            <span className="text-red-600 dark:text-red-400">
              ❌ {failedItems} failed
            </span>
          )}
        </div>
      </div>

      {/* Detailed View */}
      {showDetails && (
        <div className="mt-4 space-y-4">
          {/* Job Information */}
          <div>
            <h4 className={`font-medium mb-2 ${darkMode ? 'text-white' : 'text-gray-900'}`}>
              Job Information:
            </h4>
            <div className="grid grid-cols-2 gap-4 text-sm">
              <div>
                <span className={`font-medium ${darkMode ? 'text-gray-300' : 'text-gray-700'}`}>Status:</span>
                <span className={`ml-2 ${darkMode ? 'text-gray-400' : 'text-gray-600'}`}>{job.status}</span>
              </div>
              <div>
                <span className={`font-medium ${darkMode ? 'text-gray-300' : 'text-gray-700'}`}>Progress:</span>
                <span className={`ml-2 ${darkMode ? 'text-gray-400' : 'text-gray-600'}`}>{processedItems}/{totalItems}</span>
              </div>
              <div>
                <span className={`font-medium ${darkMode ? 'text-gray-300' : 'text-gray-700'}`}>Success Rate:</span>
                <span className={`ml-2 ${darkMode ? 'text-gray-400' : 'text-gray-600'}`}>
                  {totalItems > 0 ? ((successItems / processedItems) * 100).toFixed(1) : 0}%
                </span>
              </div>
              <div>
                <span className={`font-medium ${darkMode ? 'text-gray-300' : 'text-gray-700'}`}>Created:</span>
                <span className={`ml-2 ${darkMode ? 'text-gray-400' : 'text-gray-600'}`}>{formatTime(job.created_at)}</span>
              </div>
            </div>
          </div>

          {/* Error Breakdown */}
          {Object.keys(errorBreakdown).length > 0 && (
            <div>
              <h4 className={`font-medium mb-2 ${darkMode ? 'text-white' : 'text-gray-900'}`}>
                Error Breakdown:
              </h4>
              <div className="grid grid-cols-2 md:grid-cols-3 gap-2">
                {Object.entries(errorBreakdown).map(([errorType, count]) => (
                  <div
                    key={errorType}
                    className={`px-2 py-1 rounded text-xs ${getErrorTypeColor(errorType)}`}
                  >
                    {errorType}: {count}
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Niche Breakdown for New Creator Jobs */}
          {job.results && job.results.niche_stats && (job.results.niche_stats.primary_niches || job.results.niche_stats.secondary_niches) && (
            <div className={`p-4 rounded-lg mb-4 ${
              darkMode ? 'bg-blue-900/20 border border-blue-700' : 'bg-blue-50 border border-blue-200'
            }`}>
              <h4 className={`text-sm font-medium mb-2 ${darkMode ? 'text-blue-300' : 'text-blue-800'}`}>
                📊 Niche Breakdown
              </h4>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                {job.results.niche_stats.primary_niches && Object.keys(job.results.niche_stats.primary_niches).length > 0 && (
                  <div>
                    <h5 className={`text-xs font-medium mb-1 ${darkMode ? 'text-blue-400' : 'text-blue-700'}`}>Primary Niches:</h5>
                    {Object.entries(job.results.niche_stats.primary_niches)
                      .sort(([,a], [,b]) => (b as number) - (a as number))
                      .map(([niche, count]) => (
                        <div key={niche} className={`text-xs flex justify-between ${darkMode ? 'text-gray-300' : 'text-gray-700'}`}>
                          <span>• {niche}</span>
                          <span className="font-medium">{count}</span>
                        </div>
                      ))}
                  </div>
                )}
                {job.results.niche_stats.secondary_niches && Object.keys(job.results.niche_stats.secondary_niches).length > 0 && (
                  <div>
                    <h5 className={`text-xs font-medium mb-1 ${darkMode ? 'text-purple-400' : 'text-purple-700'}`}>Secondary Niches:</h5>
                    {Object.entries(job.results.niche_stats.secondary_niches)
                      .sort(([,a], [,b]) => (b as number) - (a as number))
                      .map(([niche, count]) => (
                        <div key={niche} className={`text-xs flex justify-between ${darkMode ? 'text-gray-300' : 'text-gray-700'}`}>
                          <span>• {niche}</span>
                          <span className="font-medium">{count}</span>
                        </div>
                      ))}
                  </div>
                )}
              </div>
            </div>
          )}

          {/* Results Summary */}
          {job.results && (
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {job.results.added && job.results.added.length > 0 && (
                <div>
                  <h5 className={`font-medium mb-1 text-green-600 dark:text-green-400`}>
                    ✅ Added ({job.results.added.length})
                  </h5>
                  <div className="max-h-32 overflow-y-auto">
                    {job.results.added.slice(0, 5).map((item, index) => (
                      <div key={index} className={`text-xs ${darkMode ? 'text-gray-300' : 'text-gray-600'}`}>
                        {item}
                      </div>
                    ))}
                    {job.results.added.length > 5 && (
                      <div className={`text-xs ${darkMode ? 'text-gray-400' : 'text-gray-500'}`}>
                        ...and {job.results.added.length - 5} more
                      </div>
                    )}
                  </div>
                </div>
              )}

              {job.results.skipped && job.results.skipped.length > 0 && (
                <div>
                  <h5 className={`font-medium mb-1 text-blue-600 dark:text-blue-400`}>
                    ⏭️ Skipped ({job.results.skipped.length})
                  </h5>
                  <div className="max-h-32 overflow-y-auto">
                    {job.results.skipped.slice(0, 5).map((item, index) => (
                      <div key={index} className={`text-xs ${darkMode ? 'text-gray-300' : 'text-gray-600'}`}>
                        {item}
                      </div>
                    ))}
                    {job.results.skipped.length > 5 && (
                      <div className={`text-xs ${darkMode ? 'text-gray-400' : 'text-gray-500'}`}>
                        ...and {job.results.skipped.length - 5} more
                      </div>
                    )}
                  </div>
                </div>
              )}

              {job.results.filtered && job.results.filtered.length > 0 && (
                <div>
                  <h5 className={`font-medium mb-1 text-yellow-600 dark:text-yellow-400`}>
                    🔍 Filtered ({job.results.filtered.length})
                  </h5>
                  <div className="max-h-32 overflow-y-auto">
                    {job.results.filtered.slice(0, 5).map((item, index) => (
                      <div key={index} className={`text-xs ${darkMode ? 'text-gray-300' : 'text-gray-600'}`}>
                        {item}
                      </div>
                    ))}
                    {job.results.filtered.length > 5 && (
                      <div className={`text-xs ${darkMode ? 'text-gray-400' : 'text-gray-500'}`}>
                        ...and {job.results.filtered.length - 5} more
                      </div>
                    )}
                  </div>
                </div>
              )}

              {job.results.updated && job.results.updated.length > 0 && (
                <div>
                  <h5 className={`font-medium mb-1 text-green-600 dark:text-green-400`}>
                    ✅ Updated ({job.results.updated.length})
                  </h5>
                  <div className="max-h-32 overflow-y-auto">
                    {job.results.updated.slice(0, 5).map((item, index) => (
                      <div key={index} className={`text-xs ${darkMode ? 'text-gray-300' : 'text-gray-600'}`}>
                        {item}
                      </div>
                    ))}
                    {job.results.updated.length > 5 && (
                      <div className={`text-xs ${darkMode ? 'text-gray-400' : 'text-gray-500'}`}>
                        ...and {job.results.updated.length - 5} more
                      </div>
                    )}
                  </div>
                </div>
              )}

              {job.results.deleted && job.results.deleted.length > 0 && (
                <div>
                  <h5 className={`font-medium mb-1 text-yellow-600 dark:text-yellow-400`}>
                    🗑️ Deleted ({job.results.deleted.length})
                  </h5>
                  <div className="max-h-32 overflow-y-auto">
                    {job.results.deleted.slice(0, 5).map((item, index) => (
                      <div key={index} className={`text-xs ${darkMode ? 'text-gray-300' : 'text-gray-600'}`}>
                        {item}
                      </div>
                    ))}
                    {job.results.deleted.length > 5 && (
                      <div className={`text-xs ${darkMode ? 'text-gray-400' : 'text-gray-500'}`}>
                        ...and {job.results.deleted.length - 5} more
                      </div>
                    )}
                  </div>
                </div>
              )}

              {job.results.failed && job.results.failed.length > 0 && (
                <div>
                  <h5 className={`font-medium mb-1 text-red-600 dark:text-red-400`}>
                    ❌ Failed ({job.results.failed.length})
                  </h5>
                  <div className="max-h-32 overflow-y-auto">
                    {job.results.failed.slice(0, 5).map((item, index) => (
                      <div key={index} className={`text-xs ${darkMode ? 'text-gray-300' : 'text-gray-600'}`}>
                        {item}
                      </div>
                    ))}
                    {job.results.failed.length > 5 && (
                      <div className={`text-xs ${darkMode ? 'text-gray-400' : 'text-gray-500'}`}>
                        ...and {job.results.failed.length - 5} more
                      </div>
                    )}
                  </div>
                </div>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
