'use client'

import { useState, useEffect, useCallback } from 'react'
import { toast } from 'react-hot-toast'
import { 
  CalendarIcon,
  ClockIcon,
  UserGroupIcon,
  PlayIcon,
  PauseIcon,
  CheckCircleIcon
} from '@heroicons/react/24/outline'

interface RescrapeStats {
  total_creators: number
  last_rescrape_date: string | null
  days_since_rescrape: number
  next_recommended_date: string
  is_overdue: boolean
  overdue_days: number
}

interface JobStatus {
  id: string
  status: string
  progress?: {
    current: number
    total: number
    percentage: number
  }
  created_at: string
  updated_at: string
}

export default function RescrapeManagement() {
  const [stats, setStats] = useState<RescrapeStats | null>(null)
  const [isLoading, setIsLoading] = useState(true)
  const [isRunning, setIsRunning] = useState(false)
  const [currentJob, setCurrentJob] = useState<JobStatus | null>(null)
  const [countdown, setCountdown] = useState<string>('')

  const fetchStats = useCallback(async () => {
    try {
      const response = await fetch('/api/rescraping/simple-stats')
      const data = await response.json()
      setStats(data)
    } catch (error) {
      console.error('Error fetching stats:', error)
      toast.error('Failed to fetch rescrape statistics')
    } finally {
      setIsLoading(false)
    }
  }, [])

  const updateCountdown = useCallback(() => {
    if (!stats?.next_recommended_date) return
    
    const now = new Date()
    const nextDate = new Date(stats.next_recommended_date)
    const timeDiff = nextDate.getTime() - now.getTime()
    
    if (timeDiff <= 0) {
      setCountdown('Rescrape recommended now!')
      return
    }
    
    const days = Math.floor(timeDiff / (1000 * 60 * 60 * 24))
    const hours = Math.floor((timeDiff % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60))
    const minutes = Math.floor((timeDiff % (1000 * 60 * 60)) / (1000 * 60))
    
    if (days > 0) {
      setCountdown(`${days}d ${hours}h ${minutes}m until next recommended rescrape`)
    } else if (hours > 0) {
      setCountdown(`${hours}h ${minutes}m until next recommended rescrape`)
    } else {
      setCountdown(`${minutes}m until next recommended rescrape`)
    }
  }, [stats?.next_recommended_date])

  const checkJobStatus = useCallback(async () => {
    if (!isRunning) return
    
    try {
      const response = await fetch('/api/jobs')
      const jobs = await response.json()
      const runningJob = jobs.find((job: JobStatus) => job.status === 'running')
      
      if (runningJob) {
        setCurrentJob(runningJob)
      } else {
        setIsRunning(false)
        setCurrentJob(null)
        toast.success('Rescraping completed!')
        fetchStats() // Refresh stats
      }
    } catch (error) {
      console.error('Error checking job status:', error)
    }
  }, [isRunning, fetchStats])

  const handleRescrape = async (platform?: string) => {
    const confirmMessage = platform 
      ? `Start rescraping all ${platform} creators?`
      : `Start rescraping all ${stats?.total_creators || 0} creators?`
    
    if (!confirm(confirmMessage)) return

    setIsRunning(true)
    setCurrentJob(null)
    
    try {
      const endpoint = platform 
        ? `/simple/rescrape-platform/${platform}`
        : '/simple/rescrape-all'
        
      const response = await fetch(endpoint, { method: 'POST' })
      const result = await response.json()
      
      if (result.status === 'started' || result.status === 'completed') {
        toast.success(result.status === 'completed' ? 'Rescraping completed!' : 'Rescraping started!')
        if (result.status === 'completed') {
          setIsRunning(false)
          fetchStats()
        }
      } else {
        throw new Error(result.error || 'Failed to start rescraping')
      }
    } catch (error) {
      console.error('Error starting rescrape:', error)
      toast.error(error instanceof Error ? error.message : 'Failed to start rescraping')
      setIsRunning(false)
    }
  }

  useEffect(() => {
    fetchStats()
  }, [fetchStats])

  useEffect(() => {
    if (stats) {
      updateCountdown()
      const interval = setInterval(updateCountdown, 60000) // Update every minute
      return () => clearInterval(interval)
    }
  }, [stats, updateCountdown])

  useEffect(() => {
    if (isRunning) {
      const interval = setInterval(checkJobStatus, 3000) // Check every 3 seconds
      return () => clearInterval(interval)
    }
  }, [isRunning, checkJobStatus])

  if (isLoading) {
    return (
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="animate-pulse">
          <div className="h-8 bg-gray-300 rounded w-1/4 mb-4"></div>
          <div className="h-4 bg-gray-300 rounded w-1/2 mb-8"></div>
          <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
            {[...Array(4)].map((_, i) => (
              <div key={i} className="h-32 bg-gray-300 rounded-lg"></div>
            ))}
          </div>
        </div>
      </div>
    )
  }

  if (!stats) {
    return (
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="text-center text-red-600">
          Failed to load rescrape statistics. Please try refreshing the page.
        </div>
      </div>
    )
  }

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <div className="mb-8">
        <h1 className="text-3xl font-bold text-gray-900 mb-2">Manual Rescraping</h1>
        <p className="text-lg text-gray-600">
          Manually rescrape creators when needed. Recommended interval: every 7 days.
        </p>
      </div>

      {/* Status Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
        <div className="bg-white rounded-lg shadow p-6">
          <div className="flex items-center">
            <UserGroupIcon className="h-8 w-8 text-blue-500 mr-3" />
            <div>
              <p className="text-sm font-medium text-gray-500">Total Creators</p>
              <p className="text-2xl font-bold text-gray-900">{stats.total_creators}</p>
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg shadow p-6">
          <div className="flex items-center">
            <CalendarIcon className="h-8 w-8 text-green-500 mr-3" />
            <div>
              <p className="text-sm font-medium text-gray-500">Last Rescrape</p>
              <p className="text-lg font-semibold text-gray-900">
                {stats.last_rescrape_date 
                  ? new Date(stats.last_rescrape_date).toLocaleDateString()
                  : 'Never'
                }
              </p>
              <p className="text-sm text-gray-500">
                {stats.days_since_rescrape > 0 
                  ? `${stats.days_since_rescrape} days ago`
                  : 'Today'
                }
              </p>
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg shadow p-6">
          <div className="flex items-center">
            <ClockIcon className={`h-8 w-8 mr-3 ${stats.is_overdue ? 'text-red-500' : 'text-yellow-500'}`} />
            <div>
              <p className="text-sm font-medium text-gray-500">Status</p>
              <p className={`text-lg font-semibold ${stats.is_overdue ? 'text-red-600' : 'text-green-600'}`}>
                {stats.is_overdue 
                  ? `Overdue (${stats.overdue_days} days)`
                  : 'Up to date'
                }
              </p>
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg shadow p-6">
          <div className="flex items-center">
            <CheckCircleIcon className="h-8 w-8 text-purple-500 mr-3" />
            <div>
              <p className="text-sm font-medium text-gray-500">Next Recommended</p>
              <p className="text-sm font-semibold text-gray-900">
                {new Date(stats.next_recommended_date).toLocaleDateString()}
              </p>
            </div>
          </div>
        </div>
      </div>

      {/* Countdown Timer */}
      <div className="bg-gradient-to-r from-blue-50 to-indigo-50 rounded-lg p-6 mb-8">
        <div className="flex items-center justify-center">
          <ClockIcon className="h-6 w-6 text-indigo-600 mr-3" />
          <p className={`text-lg font-medium ${stats.is_overdue ? 'text-red-600' : 'text-indigo-600'}`}>
            {countdown || 'Calculating...'}
          </p>
        </div>
      </div>

      {/* Progress Display */}
      {isRunning && currentJob && (
        <div className="bg-blue-50 rounded-lg p-6 mb-8">
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center">
              <PauseIcon className="h-6 w-6 text-blue-600 mr-3 animate-spin" />
              <h3 className="text-lg font-semibold text-blue-900">Rescraping in Progress</h3>
            </div>
            <div className="text-sm text-blue-700">
              Job ID: {currentJob.id}
            </div>
          </div>
          
          {currentJob.progress && (
            <>
              <div className="bg-blue-200 rounded-full h-3 mb-2">
                <div 
                  className="bg-blue-600 rounded-full h-3 transition-all duration-300"
                  style={{ width: `${currentJob.progress.percentage}%` }}
                ></div>
              </div>
              <p className="text-sm text-blue-700">
                {currentJob.progress.current} of {currentJob.progress.total} creators processed 
                ({currentJob.progress.percentage}%)
              </p>
            </>
          )}
          
          <p className="text-sm text-blue-600 mt-2">
            Started: {new Date(currentJob.created_at).toLocaleString()}
          </p>
        </div>
      )}

      {/* Action Buttons */}
      <div className="bg-white rounded-lg shadow p-6">
        <h3 className="text-xl font-semibold text-gray-900 mb-6">Start Manual Rescrape</h3>
        
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <button
            onClick={() => handleRescrape()}
            disabled={isRunning}
            className={`flex items-center justify-center px-6 py-3 rounded-lg font-medium transition-all ${
              isRunning 
                ? 'bg-gray-300 cursor-not-allowed text-gray-500'
                : stats.is_overdue
                  ? 'bg-red-600 hover:bg-red-700 text-white'
                  : 'bg-blue-600 hover:bg-blue-700 text-white'
            }`}
          >
            <PlayIcon className="h-5 w-5 mr-2" />
            Rescrape All Creators
            <span className="ml-2 text-sm">({stats.total_creators})</span>
          </button>

          <button
            onClick={() => handleRescrape('instagram')}
            disabled={isRunning}
            className={`flex items-center justify-center px-6 py-3 rounded-lg font-medium transition-all ${
              isRunning 
                ? 'bg-gray-300 cursor-not-allowed text-gray-500'
                : 'bg-pink-600 hover:bg-pink-700 text-white'
            }`}
          >
            <PlayIcon className="h-5 w-5 mr-2" />
            Instagram Only
          </button>

          <button
            onClick={() => handleRescrape('tiktok')}
            disabled={isRunning}
            className={`flex items-center justify-center px-6 py-3 rounded-lg font-medium transition-all ${
              isRunning 
                ? 'bg-gray-300 cursor-not-allowed text-gray-500'
                : 'bg-black hover:bg-gray-800 text-white'
            }`}
          >
            <PlayIcon className="h-5 w-5 mr-2" />
            TikTok Only
          </button>
        </div>

        <div className="mt-4 text-sm text-gray-500">
          <p>• Rescraping updates creator metrics, engagement rates, and follower counts</p>
          <p>• Existing niche data and creator information will be preserved</p>
          <p>• Process typically takes 5-10 minutes per 100 creators</p>
        </div>
      </div>
    </div>
  )
}