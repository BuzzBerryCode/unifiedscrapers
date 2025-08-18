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
}

export default function JobsTable({ jobs, onCancelJob }: JobsTableProps) {
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

  const handleViewDetails = (job: Job) => {
    setSelectedJob(job)
    setShowDetails(true)
  }

  return (
    <>
      <div className="overflow-hidden">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Job
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Status
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Progress
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Created
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Actions
              </th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {jobs.map((job) => (
              <tr key={job.id} className="hover:bg-gray-50">
                <td className="px-6 py-4 whitespace-nowrap">
                  <div className="flex items-center">
                    {getStatusIcon(job.status)}
                    <div className="ml-3">
                      <div className="text-sm font-medium text-gray-900">
                        {job.job_type.replace('_', ' ').toUpperCase()}
                      </div>
                      <div className="text-sm text-gray-500 max-w-xs truncate">
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
                      <div className="flex justify-between text-sm text-gray-600 mb-1">
                        <span>{job.processed_items || 0} / {job.total_items || 0}</span>
                        <span>{getProgressPercentage(job)}%</span>
                      </div>
                      <div className="w-full bg-gray-200 rounded-full h-2">
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
                    </div>
                  </div>
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                  {format(new Date(job.created_at), 'MMM dd, HH:mm')}
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm font-medium">
                  <div className="flex space-x-2">
                    <button
                      onClick={() => handleViewDetails(job)}
                      className="text-blue-600 hover:text-blue-900"
                    >
                      <EyeIcon className="h-4 w-4" />
                    </button>
                    {(job.status === 'pending' || job.status === 'running') && (
                      <button
                        onClick={() => onCancelJob(job.id)}
                        className="text-red-600 hover:text-red-900"
                      >
                        <TrashIcon className="h-4 w-4" />
                      </button>
                    )}
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        
        {jobs.length === 0 && (
          <div className="text-center py-12">
            <ClockIcon className="mx-auto h-12 w-12 text-gray-400" />
            <h3 className="mt-2 text-sm font-medium text-gray-900">No jobs yet</h3>
            <p className="mt-1 text-sm text-gray-500">
              Upload a CSV file or start a rescraping job to get started.
            </p>
          </div>
        )}
      </div>

      {/* Job Details Modal */}
      {showDetails && selectedJob && (
        <div className="fixed inset-0 bg-gray-600 bg-opacity-50 overflow-y-auto h-full w-full z-50">
          <div className="relative top-20 mx-auto p-5 border w-11/12 md:w-3/4 lg:w-1/2 shadow-lg rounded-md bg-white">
            <div className="flex justify-between items-center mb-4">
              <h3 className="text-lg font-medium text-gray-900">
                Job Details: {selectedJob.job_type.replace('_', ' ').toUpperCase()}
              </h3>
              <button
                onClick={() => setShowDetails(false)}
                className="text-gray-400 hover:text-gray-600"
              >
                <XCircleIcon className="h-6 w-6" />
              </button>
            </div>
            
            <div className="space-y-4">
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700">Status</label>
                  <span className={getStatusBadge(selectedJob.status)}>
                    {selectedJob.status}
                  </span>
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700">Progress</label>
                  <p className="text-sm text-gray-900">
                    {selectedJob.processed_items || 0} / {selectedJob.total_items || 0} ({getProgressPercentage(selectedJob)}%)
                  </p>
                </div>
              </div>
              
              <div>
                <label className="block text-sm font-medium text-gray-700">Description</label>
                <p className="text-sm text-gray-900">{selectedJob.description}</p>
              </div>
              
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700">Created</label>
                  <p className="text-sm text-gray-900">
                    {format(new Date(selectedJob.created_at), 'MMM dd, yyyy HH:mm:ss')}
                  </p>
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700">Updated</label>
                  <p className="text-sm text-gray-900">
                    {format(new Date(selectedJob.updated_at), 'MMM dd, yyyy HH:mm:ss')}
                  </p>
                </div>
              </div>

              {selectedJob.error_message && (
                <div>
                  <label className="block text-sm font-medium text-red-700">Error Message</label>
                  <p className="text-sm text-red-600 bg-red-50 p-2 rounded">
                    {selectedJob.error_message}
                  </p>
                </div>
              )}

              {selectedJob.results && (
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">Results</label>
                  <div className="bg-gray-50 p-4 rounded-lg space-y-2">
                    {selectedJob.results.added && selectedJob.results.added.length > 0 && (
                      <div>
                        <span className="text-sm font-medium text-green-700">Added ({selectedJob.results.added.length}):</span>
                        <div className="text-xs text-gray-600 max-h-20 overflow-y-auto">
                          {selectedJob.results.added.join(', ')}
                        </div>
                      </div>
                    )}
                    
                    {selectedJob.results.updated && selectedJob.results.updated.length > 0 && (
                      <div>
                        <span className="text-sm font-medium text-blue-700">Updated ({selectedJob.results.updated.length}):</span>
                        <div className="text-xs text-gray-600 max-h-20 overflow-y-auto">
                          {selectedJob.results.updated.join(', ')}
                        </div>
                      </div>
                    )}
                    
                    {selectedJob.results.deleted && selectedJob.results.deleted.length > 0 && (
                      <div>
                        <span className="text-sm font-medium text-yellow-700">Deleted ({selectedJob.results.deleted.length}):</span>
                        <div className="text-xs text-gray-600 max-h-20 overflow-y-auto">
                          {selectedJob.results.deleted.join(', ')}
                        </div>
                      </div>
                    )}
                    
                    {selectedJob.results.failed && selectedJob.results.failed.length > 0 && (
                      <div>
                        <span className="text-sm font-medium text-red-700">Failed ({selectedJob.results.failed.length}):</span>
                        <div className="text-xs text-gray-600 max-h-20 overflow-y-auto">
                          {selectedJob.results.failed.join(', ')}
                        </div>
                      </div>
                    )}
                    
                    {selectedJob.results.skipped && selectedJob.results.skipped.length > 0 && (
                      <div>
                        <span className="text-sm font-medium text-gray-700">Skipped ({selectedJob.results.skipped.length}):</span>
                        <div className="text-xs text-gray-600 max-h-20 overflow-y-auto">
                          {selectedJob.results.skipped.join(', ')}
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
