'use client'

import { useState } from 'react'
import { format } from 'date-fns'
import { 
  TrashIcon, 
  EyeIcon,
  CheckCircleIcon,
  XCircleIcon,
  ClockIcon,
  ExclamationTriangleIcon,
  PlayIcon,
  PauseIcon
} from '@heroicons/react/24/outline'
import { Job } from '@/types'

interface JobsTableProps {
  jobs: Job[]
  onCancelJob: (jobId: string) => Promise<void>
  darkMode?: boolean
}

export default function JobsTable({ jobs, onCancelJob, darkMode = false }: JobsTableProps) {
  const [selectedJob, setSelectedJob] = useState<Job | null>(null)
  const [showDetails, setShowDetails] = useState(false)

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'pending':
        return <ClockIcon className="h-5 w-5 text-yellow-500" />
      case 'running':
        return <PlayIcon className="h-5 w-5 text-blue-500" />
      case 'completed':
        return <CheckCircleIcon className="h-5 w-5 text-green-500" />
      case 'failed':
        return <XCircleIcon className="h-5 w-5 text-red-500" />
      case 'cancelled':
        return <PauseIcon className="h-5 w-5 text-gray-500" />
      default:
        return <ExclamationTriangleIcon className="h-5 w-5 text-gray-400" />
    }
  }

  const getStatusBadge = (status: string) => {
    const baseClasses = "inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium"
    
    switch (status) {
      case 'pending':
        return `${baseClasses} bg-yellow-100 text-yellow-800`
      case 'running':
        return `${baseClasses} bg-blue-100 text-blue-800`
      case 'completed':
        return `${baseClasses} bg-green-100 text-green-800`
      case 'failed':
        return `${baseClasses} bg-red-100 text-red-800`
      case 'cancelled':
        return `${baseClasses} bg-gray-100 text-gray-800`
      default:
        return `${baseClasses} bg-gray-100 text-gray-800`
    }
  }

  const getProgressPercentage = (job: Job) => {
    if (!job.total_items || job.total_items === 0) return 0
    return Math.round((job.processed_items || 0) / job.total_items * 100)
  }

  const getEstimatedTimeRemaining = (job: Job) => {
    if (job.status !== 'running' || !job.total_items || !job.processed_items) return null
    
    const processed = job.processed_items
    const total = job.total_items
    const remaining = total - processed
    
    if (processed === 0) return null
    
    // Calculate time elapsed since job started
    const startTime = new Date(job.created_at).getTime()
    const now = Date.now()
    const elapsedMs = now - startTime
    
    // Calculate average time per item
    const avgTimePerItem = elapsedMs / processed
    
    // Estimate remaining time
    const estimatedRemainingMs = avgTimePerItem * remaining
    
    // Convert to human readable format
    const minutes = Math.round(estimatedRemainingMs / (1000 * 60))
    if (minutes < 1) return '< 1 min'
    if (minutes < 60) return `${minutes} min`
    const hours = Math.round(minutes / 60)
    return `${hours}h ${minutes % 60}m`
  }

  const getAddedSkippedCounts = (job: Job) => {
    if (!job.results) return { added: 0, skipped: 0, failed: 0, filtered: 0 }
    
    const added = (job.results.added?.length || 0) + (job.results.updated?.length || 0)
    const skipped = job.results.skipped?.length || 0
    const failed = job.results.failed?.length || 0
    const filtered = job.results.filtered?.length || 0
    
    return { added, skipped, failed, filtered }
  }

  const handleViewDetails = (job: Job) => {
    setSelectedJob(job)
    setShowDetails(true)
  }

  return (
    <>
      <div className="overflow-hidden">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className={`${darkMode ? 'bg-gray-700' : 'bg-gray-50'}`}>
            <tr>
              <th className={`px-6 py-3 text-left text-xs font-medium uppercase tracking-wider ${
                darkMode ? 'text-gray-300' : 'text-gray-500'
              }`}>
                Job
              </th>
              <th className={`px-6 py-3 text-left text-xs font-medium uppercase tracking-wider ${
                darkMode ? 'text-gray-300' : 'text-gray-500'
              }`}>
                Status
              </th>
              <th className={`px-6 py-3 text-left text-xs font-medium uppercase tracking-wider ${
                darkMode ? 'text-gray-300' : 'text-gray-500'
              }`}>
                Progress & Time
              </th>
              <th className={`px-6 py-3 text-left text-xs font-medium uppercase tracking-wider ${
                darkMode ? 'text-gray-300' : 'text-gray-500'
              }`}>
                Added/Skipped
              </th>
              <th className={`px-6 py-3 text-left text-xs font-medium uppercase tracking-wider ${
                darkMode ? 'text-gray-300' : 'text-gray-500'
              }`}>
                Created
              </th>
              <th className={`px-6 py-3 text-left text-xs font-medium uppercase tracking-wider ${
                darkMode ? 'text-gray-300' : 'text-gray-500'
              }`}>
                Actions
              </th>
            </tr>
          </thead>
          <tbody className={`divide-y ${
            darkMode ? 'bg-gray-800 divide-gray-700' : 'bg-white divide-gray-200'
          }`}>
            {jobs.map((job) => {
              const { added, skipped, failed, filtered } = getAddedSkippedCounts(job)
              const estimatedTime = getEstimatedTimeRemaining(job)
              
              return (
                <tr key={job.id} className={`transition-colors ${
                  darkMode ? 'hover:bg-gray-700' : 'hover:bg-gray-50'
                }`}>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="flex items-center">
                      {getStatusIcon(job.status)}
                      <div className="ml-3">
                        <div className={`text-sm font-medium ${
                          darkMode ? 'text-white' : 'text-gray-900'
                        }`}>
                          {job.job_type.replace('_', ' ').toUpperCase()}
                        </div>
                        <div className={`text-sm max-w-xs truncate ${
                          darkMode ? 'text-gray-400' : 'text-gray-500'
                        }`}>
                          {job.description}
                        </div>
                      </div>
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <span className={getStatusBadge(job.status)}>
                      {job.status}
                    </span>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="flex items-center">
                      <div className="flex-1">
                        <div className={`flex justify-between text-sm mb-1 ${
                          darkMode ? 'text-gray-300' : 'text-gray-600'
                        }`}>
                          <span>{job.processed_items || 0} / {job.total_items || 0}</span>
                          <span>{getProgressPercentage(job)}%</span>
                        </div>
                        <div className={`w-full rounded-full h-2 ${
                          darkMode ? 'bg-gray-700' : 'bg-gray-200'
                        }`}>
                          <div
                            className={`h-2 rounded-full transition-all duration-300 ${
                              job.status === 'completed' ? 'bg-green-500' :
                              job.status === 'failed' ? 'bg-red-500' :
                              job.status === 'running' ? 'bg-blue-500' :
                              'bg-gray-400'
                            }`}
                            style={{ width: `${getProgressPercentage(job)}%` }}
                          ></div>
                        </div>
                        {estimatedTime && (
                          <div className={`text-xs mt-1 ${
                            darkMode ? 'text-gray-400' : 'text-gray-500'
                          }`}>
                            ETA: {estimatedTime}
                          </div>
                        )}
                      </div>
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="flex space-x-4">
                      <div className="flex items-center">
                        <CheckCircleIcon className="h-4 w-4 text-green-500 mr-1" />
                        <span className={`text-sm ${darkMode ? 'text-gray-300' : 'text-gray-600'}`}>
                          {added}
                        </span>
                      </div>
                      <div className="flex items-center">
                        <div className="h-4 w-4 rounded-full bg-blue-500 mr-1 flex items-center justify-center">
                          <div className="h-2 w-2 rounded-full bg-white"></div>
                        </div>
                        <span className={`text-sm ${darkMode ? 'text-gray-300' : 'text-gray-600'}`}>
                          {skipped}
                        </span>
                      </div>
                      {(failed > 0 || filtered > 0) && (
                        <div className="flex items-center">
                          <XCircleIcon className="h-4 w-4 text-red-500 mr-1" />
                          <span className={`text-sm ${darkMode ? 'text-gray-300' : 'text-gray-600'}`}>
                            {failed + filtered}
                          </span>
                        </div>
                      )}
                    </div>
                  </td>
                  <td className={`px-6 py-4 whitespace-nowrap text-sm ${
                    darkMode ? 'text-gray-400' : 'text-gray-500'
                  }`}>
                    {format(new Date(job.created_at), 'MMM dd, HH:mm')}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm font-medium">
                    <div className="flex space-x-2">
                      <button
                        onClick={() => handleViewDetails(job)}
                        className={`transition-colors ${
                          darkMode 
                            ? 'text-blue-400 hover:text-blue-300' 
                            : 'text-blue-600 hover:text-blue-900'
                        }`}
                      >
                        <EyeIcon className="h-4 w-4" />
                      </button>
                      {(job.status === 'pending' || job.status === 'running') && (
                        <button
                          onClick={() => onCancelJob(job.id)}
                          className={`transition-colors ${
                            darkMode 
                              ? 'text-red-400 hover:text-red-300' 
                              : 'text-red-600 hover:text-red-900'
                          }`}
                        >
                          <TrashIcon className="h-4 w-4" />
                        </button>
                      )}
                    </div>
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
        
        {jobs.length === 0 && (
          <div className="text-center py-12">
            <ClockIcon className={`mx-auto h-12 w-12 ${darkMode ? 'text-gray-500' : 'text-gray-400'}`} />
            <h3 className={`mt-2 text-sm font-medium ${darkMode ? 'text-white' : 'text-gray-900'}`}>
              No jobs yet
            </h3>
            <p className={`mt-1 text-sm ${darkMode ? 'text-gray-400' : 'text-gray-500'}`}>
              Upload a CSV file or start a rescraping job to get started.
            </p>
          </div>
        )}
      </div>

      {/* Job Details Modal */}
      {showDetails && selectedJob && (
        <div className="fixed inset-0 bg-gray-600 bg-opacity-50 overflow-y-auto h-full w-full z-50">
          <div className={`relative top-20 mx-auto p-5 border w-11/12 md:w-3/4 lg:w-1/2 shadow-lg rounded-md transition-colors ${
            darkMode 
              ? 'bg-gray-800 border-gray-700' 
              : 'bg-white border-gray-200'
          }`}>
            <div className="flex justify-between items-center mb-4">
              <h3 className={`text-lg font-medium ${darkMode ? 'text-white' : 'text-gray-900'}`}>
                Job Details: {selectedJob.job_type.replace('_', ' ').toUpperCase()}
              </h3>
              <button
                onClick={() => setShowDetails(false)}
                className={`transition-colors ${
                  darkMode 
                    ? 'text-gray-400 hover:text-gray-300' 
                    : 'text-gray-400 hover:text-gray-600'
                }`}
              >
                <XCircleIcon className="h-6 w-6" />
              </button>
            </div>
            
            <div className="space-y-4">
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className={`block text-sm font-medium ${darkMode ? 'text-gray-300' : 'text-gray-700'}`}>Status</label>
                  <span className={getStatusBadge(selectedJob.status)}>
                    {selectedJob.status}
                  </span>
                </div>
                <div>
                  <label className={`block text-sm font-medium ${darkMode ? 'text-gray-300' : 'text-gray-700'}`}>Progress</label>
                  <p className={`text-sm ${darkMode ? 'text-gray-300' : 'text-gray-900'}`}>
                    {selectedJob.processed_items || 0} / {selectedJob.total_items || 0} ({getProgressPercentage(selectedJob)}%)
                  </p>
                </div>
              </div>
              
              <div>
                <label className={`block text-sm font-medium ${darkMode ? 'text-gray-300' : 'text-gray-700'}`}>Description</label>
                <p className={`text-sm ${darkMode ? 'text-gray-300' : 'text-gray-900'}`}>{selectedJob.description}</p>
              </div>
              
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className={`block text-sm font-medium ${darkMode ? 'text-gray-300' : 'text-gray-700'}`}>Created</label>
                  <p className={`text-sm ${darkMode ? 'text-gray-300' : 'text-gray-900'}`}>
                    {format(new Date(selectedJob.created_at), 'MMM dd, yyyy HH:mm:ss')}
                  </p>
                </div>
                <div>
                  <label className={`block text-sm font-medium ${darkMode ? 'text-gray-300' : 'text-gray-700'}`}>Updated</label>
                  <p className={`text-sm ${darkMode ? 'text-gray-300' : 'text-gray-900'}`}>
                    {format(new Date(selectedJob.updated_at), 'MMM dd, yyyy HH:mm:ss')}
                  </p>
                </div>
              </div>

              {selectedJob.error_message && (
                <div>
                  <label className={`block text-sm font-medium ${darkMode ? 'text-red-400' : 'text-red-700'}`}>Error Message</label>
                  <p className={`text-sm p-2 rounded ${
                    darkMode 
                      ? 'text-red-300 bg-red-900/20 border border-red-800' 
                      : 'text-red-600 bg-red-50'
                  }`}>
                    {selectedJob.error_message}
                  </p>
                </div>
              )}

              {selectedJob.results && (
                <div>
                  <label className={`block text-sm font-medium ${darkMode ? 'text-gray-300' : 'text-gray-700'} mb-2`}>Results</label>
                  <div className={`p-4 rounded-lg space-y-3 ${
                    darkMode ? 'bg-gray-700 border border-gray-600' : 'bg-gray-50'
                  }`}>
                    {selectedJob.results.added && selectedJob.results.added.length > 0 && (
                      <div>
                        <span className={`text-sm font-medium ${darkMode ? 'text-green-400' : 'text-green-700'}`}>
                          ✅ Added ({selectedJob.results.added.length}):
                        </span>
                        <div className={`text-xs mt-1 max-h-20 overflow-y-auto ${
                          darkMode ? 'text-gray-300' : 'text-gray-600'
                        }`}>
                          {selectedJob.results.added.join(', ')}
                        </div>
                      </div>
                    )}
                    
                    {selectedJob.results.updated && selectedJob.results.updated.length > 0 && (
                      <div>
                        <span className={`text-sm font-medium ${darkMode ? 'text-blue-400' : 'text-blue-700'}`}>
                          🔄 Updated ({selectedJob.results.updated.length}):
                        </span>
                        <div className={`text-xs mt-1 max-h-20 overflow-y-auto ${
                          darkMode ? 'text-gray-300' : 'text-gray-600'
                        }`}>
                          {selectedJob.results.updated.join(', ')}
                        </div>
                      </div>
                    )}
                    
                    {selectedJob.results.skipped && selectedJob.results.skipped.length > 0 && (
                      <div>
                        <span className={`text-sm font-medium ${darkMode ? 'text-blue-400' : 'text-blue-700'}`}>
                          ⏭️ Skipped - Already Exists ({selectedJob.results.skipped.length}):
                        </span>
                        <div className={`text-xs mt-1 max-h-20 overflow-y-auto ${
                          darkMode ? 'text-gray-300' : 'text-gray-600'
                        }`}>
                          {selectedJob.results.skipped.join(', ')}
                        </div>
                      </div>
                    )}
                    
                    {selectedJob.results.filtered && selectedJob.results.filtered.length > 0 && (
                      <div>
                        <span className={`text-sm font-medium ${darkMode ? 'text-yellow-400' : 'text-yellow-700'}`}>
                          🔍 Filtered - Didn&apos;t Meet Criteria ({selectedJob.results.filtered.length}):
                        </span>
                        <div className={`text-xs mt-1 max-h-20 overflow-y-auto ${
                          darkMode ? 'text-gray-300' : 'text-gray-600'
                        }`}>
                          {selectedJob.results.filtered.join(', ')}
                        </div>
                      </div>
                    )}
                    
                    {selectedJob.results.failed && selectedJob.results.failed.length > 0 && (
                      <div>
                        <span className={`text-sm font-medium ${darkMode ? 'text-red-400' : 'text-red-700'}`}>
                          ❌ Failed - API/Processing Errors ({selectedJob.results.failed.length}):
                        </span>
                        <div className={`text-xs mt-1 max-h-20 overflow-y-auto ${
                          darkMode ? 'text-gray-300' : 'text-gray-600'
                        }`}>
                          {selectedJob.results.failed.join(', ')}
                        </div>
                      </div>
                    )}
                    
                    {selectedJob.results.deleted && selectedJob.results.deleted.length > 0 && (
                      <div>
                        <span className={`text-sm font-medium ${darkMode ? 'text-yellow-400' : 'text-yellow-700'}`}>
                          🗑️ Deleted - Inactive ({selectedJob.results.deleted.length}):
                        </span>
                        <div className={`text-xs mt-1 max-h-20 overflow-y-auto ${
                          darkMode ? 'text-gray-300' : 'text-gray-600'
                        }`}>
                          {selectedJob.results.deleted.join(', ')}
                        </div>
                      </div>
                    )}
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </>
  )
}
