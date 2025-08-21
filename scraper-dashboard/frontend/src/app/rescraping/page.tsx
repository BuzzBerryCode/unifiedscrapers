'use client'

import { useState, useEffect } from 'react'
import { toast } from 'react-hot-toast'
import { 
  ArrowPathIcon,
  CalendarIcon,
  ChartBarIcon,
  ClockIcon,
  UserGroupIcon,
  PlayIcon,
  Cog6ToothIcon
} from '@heroicons/react/24/outline'

interface RescrapeStats {
  total_creators: number
  creators_need_dates: number
  creators_due_rescrape: number
  weekly_schedule: { [key: string]: { date: string; day: string; estimated_creators: number } }
  recent_jobs: Array<{
    id: string
    job_type: string
    status: string
    total_items: number
    created_at: string
  }>
}

interface DueCreator {
  id: string
  handle: string
  platform: string
  updated_at: string
  primary_niche: string
}

export default function RescrapeManagement() {
  const [stats, setStats] = useState<RescrapeStats | null>(null)
  const [dueCreators, setDueCreators] = useState<DueCreator[]>([])
  const [loading, setLoading] = useState(true)
  const [actionLoading, setActionLoading] = useState('')
  const [darkMode, setDarkMode] = useState(false)

  // Check dark mode on component mount
  useEffect(() => {
    const isDark = document.documentElement.classList.contains('dark')
    setDarkMode(isDark)
    
    // Listen for dark mode changes
    const observer = new MutationObserver((mutations) => {
      mutations.forEach((mutation) => {
        if (mutation.attributeName === 'class') {
          const isDark = document.documentElement.classList.contains('dark')
          setDarkMode(isDark)
        }
      })
    })
    
    observer.observe(document.documentElement, { attributes: true })
    return () => observer.disconnect()
  }, [])

  const fetchData = async () => {
    try {
      const token = localStorage.getItem('token')
      
      // Fetch rescraping stats
      const statsResponse = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/rescraping/stats`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      
      if (statsResponse.ok) {
        const statsData = await statsResponse.json()
        setStats(statsData)
      }
      
      // Fetch due creators
      const dueResponse = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/rescraping/due-creators`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      
      if (dueResponse.ok) {
        const dueData = await dueResponse.json()
        setDueCreators(dueData.creators_due || [])
      }
      
    } catch (error) {
      console.error('Error fetching rescraping data:', error)
      toast.error('Failed to load rescraping data')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchData()
    const interval = setInterval(fetchData, 30000) // Refresh every 30 seconds
    return () => clearInterval(interval)
  }, [])

  const handlePopulateDates = async () => {
    if (!confirm(`This will populate updated_at dates for ${stats?.creators_need_dates} creators. Continue?`)) return
    
    setActionLoading('populate')
    try {
      const token = localStorage.getItem('token')
      const response = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/rescraping/populate-dates`, {
        method: 'POST',
        headers: { 
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        }
      })
      
      if (response.ok) {
        const result = await response.json()
        toast.success(result.message)
        fetchData() // Refresh data
      } else {
        throw new Error('Failed to populate dates')
      }
    } catch (error) {
      console.error('Error populating dates:', error)
      toast.error('Failed to populate dates')
    } finally {
      setActionLoading('')
    }
  }

  const handleStartRescrape = async (platform: string) => {
    setActionLoading(`rescrape-${platform}`)
    try {
      const token = localStorage.getItem('token')
      
      let endpoint = '/rescraping/start-auto-rescrape'
      let body: { platform?: string; max_creators?: number } = { platform, max_creators: 100 }
      
      // Use daily scheduling for "daily" platform
      if (platform === 'daily') {
        endpoint = '/rescraping/schedule-daily'
        body = {}
      }
      
      const response = await fetch(`${process.env.NEXT_PUBLIC_API_URL}${endpoint}`, {
        method: 'POST',
        headers: { 
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(body)
      })
      
      if (response.ok) {
        const result = await response.json()
        toast.success(result.message)
        fetchData() // Refresh data
      } else {
        throw new Error('Failed to start rescraping')
      }
    } catch (error) {
      console.error('Error starting rescrape:', error)
      toast.error('Failed to start rescraping')
    } finally {
      setActionLoading('')
    }
  }

  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleDateString('en-US', {
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    })
  }

  const getStatusBadge = (status: string) => {
    const baseClasses = 'inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium'
    switch (status) {
      case 'pending': return `${baseClasses} bg-yellow-100 text-yellow-800 dark:bg-yellow-900/20 dark:text-yellow-300`
      case 'queued': return `${baseClasses} bg-blue-100 text-blue-600 dark:bg-blue-900/20 dark:text-blue-300`
      case 'running': return `${baseClasses} bg-blue-100 text-blue-800 dark:bg-blue-900/20 dark:text-blue-300`
      case 'completed': return `${baseClasses} bg-green-100 text-green-800 dark:bg-green-900/20 dark:text-green-300`
      case 'failed': return `${baseClasses} bg-red-100 text-red-800 dark:bg-red-900/20 dark:text-red-300`
      case 'cancelled': return `${baseClasses} bg-gray-100 text-gray-800 dark:bg-gray-900/20 dark:text-gray-300`
      default: return `${baseClasses} bg-gray-100 text-gray-800 dark:bg-gray-900/20 dark:text-gray-300`
    }
  }

  if (loading) {
    return (
      <div className={`min-h-screen ${darkMode ? 'bg-gray-900' : 'bg-gray-50'}`}>
        <div className="flex items-center justify-center h-64">
          <ArrowPathIcon className="h-8 w-8 animate-spin text-blue-500" />
        </div>
      </div>
    )
  }

  return (
    <div className={`min-h-screen ${darkMode ? 'bg-gray-900' : 'bg-gray-50'}`}>
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Header */}
        <div className="mb-8">
          <h1 className={`text-3xl font-bold ${darkMode ? 'text-white' : 'text-gray-900'}`}>
            Rescraping Management
          </h1>
          <p className={`mt-2 ${darkMode ? 'text-gray-400' : 'text-gray-600'}`}>
            Manage automatic 7-day rescraping schedule and monitor creator updates
          </p>
        </div>

        {/* Stats Cards */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
          <div className={`rounded-lg shadow p-6 ${darkMode ? 'bg-gray-800' : 'bg-white'}`}>
            <div className="flex items-center">
              <UserGroupIcon className="h-8 w-8 text-blue-500" />
              <div className="ml-4">
                <p className={`text-sm font-medium ${darkMode ? 'text-gray-400' : 'text-gray-500'}`}>
                  Total Creators
                </p>
                <p className={`text-2xl font-bold ${darkMode ? 'text-white' : 'text-gray-900'}`}>
                  {stats?.total_creators?.toLocaleString() || 0}
                </p>
              </div>
            </div>
          </div>

          <div className={`rounded-lg shadow p-6 ${darkMode ? 'bg-gray-800' : 'bg-white'}`}>
            <div className="flex items-center">
              <CalendarIcon className="h-8 w-8 text-yellow-500" />
              <div className="ml-4">
                <p className={`text-sm font-medium ${darkMode ? 'text-gray-400' : 'text-gray-500'}`}>
                  Need Dates
                </p>
                <p className={`text-2xl font-bold ${darkMode ? 'text-white' : 'text-gray-900'}`}>
                  {stats?.creators_need_dates?.toLocaleString() || 0}
                </p>
              </div>
            </div>
          </div>

          <div className={`rounded-lg shadow p-6 ${darkMode ? 'bg-gray-800' : 'bg-white'}`}>
            <div className="flex items-center">
              <ClockIcon className="h-8 w-8 text-red-500" />
              <div className="ml-4">
                <p className={`text-sm font-medium ${darkMode ? 'text-gray-400' : 'text-gray-500'}`}>
                  Due for Rescrape
                </p>
                <p className={`text-2xl font-bold ${darkMode ? 'text-white' : 'text-gray-900'}`}>
                  {stats?.creators_due_rescrape?.toLocaleString() || 0}
                </p>
              </div>
            </div>
          </div>

          <div className={`rounded-lg shadow p-6 ${darkMode ? 'bg-gray-800' : 'bg-white'}`}>
            <div className="flex items-center">
              <ChartBarIcon className="h-8 w-8 text-green-500" />
              <div className="ml-4">
                <p className={`text-sm font-medium ${darkMode ? 'text-gray-400' : 'text-gray-500'}`}>
                  Weekly Average
                </p>
                <p className={`text-2xl font-bold ${darkMode ? 'text-white' : 'text-gray-900'}`}>
                  {Math.round(((stats?.creators_due_rescrape || 0) + (stats?.creators_need_dates || 0)) / 7)}
                </p>
              </div>
            </div>
          </div>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
          {/* Weekly Schedule */}
          <div className={`rounded-lg shadow p-6 ${darkMode ? 'bg-gray-800' : 'bg-white'}`}>
            <h2 className={`text-lg font-medium mb-4 ${darkMode ? 'text-white' : 'text-gray-900'}`}>
              Weekly Rescraping Schedule
            </h2>
            <div className="space-y-3">
              {stats?.weekly_schedule && Object.values(stats.weekly_schedule).map((day) => (
                <div key={day.day} className="flex items-center justify-between">
                  <div>
                    <span className={`font-medium ${darkMode ? 'text-white' : 'text-gray-900'}`}>
                      {day.day}
                    </span>
                    <span className={`ml-2 text-sm ${darkMode ? 'text-gray-400' : 'text-gray-500'}`}>
                      {day.date}
                    </span>
                  </div>
                  <div className="flex items-center">
                    <div className={`w-32 bg-gray-200 dark:bg-gray-700 rounded-full h-2 mr-3`}>
                      <div 
                        className="bg-blue-500 h-2 rounded-full" 
                        style={{ width: `${Math.min((day.estimated_creators / 300) * 100, 100)}%` }}
                      ></div>
                    </div>
                    <span className={`text-sm font-medium ${darkMode ? 'text-white' : 'text-gray-900'}`}>
                      {day.estimated_creators}
                    </span>
                  </div>
                </div>
              ))}
            </div>
          </div>

          {/* Action Panel */}
          <div className={`rounded-lg shadow p-6 ${darkMode ? 'bg-gray-800' : 'bg-white'}`}>
            <h2 className={`text-lg font-medium mb-4 ${darkMode ? 'text-white' : 'text-gray-900'}`}>
              Actions
            </h2>
            <div className="space-y-4">
              {/* Populate Dates */}
              {(stats?.creators_need_dates || 0) > 0 && (
                <div className={`p-4 border rounded-lg ${darkMode ? 'border-gray-700 bg-gray-900/50' : 'border-gray-200 bg-gray-50'}`}>
                  <h3 className={`font-medium mb-2 ${darkMode ? 'text-white' : 'text-gray-900'}`}>
                    Populate Missing Dates
                  </h3>
                  <p className={`text-sm mb-3 ${darkMode ? 'text-gray-400' : 'text-gray-600'}`}>
                    {stats.creators_need_dates} creators need updated_at dates. This will spread them across the past week.
                  </p>
                  <button
                    onClick={handlePopulateDates}
                    disabled={actionLoading === 'populate'}
                    className="inline-flex items-center px-3 py-2 border border-transparent text-sm leading-4 font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:opacity-50"
                  >
                    {actionLoading === 'populate' ? (
                      <ArrowPathIcon className="h-4 w-4 animate-spin mr-2" />
                    ) : (
                      <Cog6ToothIcon className="h-4 w-4 mr-2" />
                    )}
                    Populate Dates
                  </button>
                </div>
              )}

              {/* Start Rescraping */}
              <div className={`p-4 border rounded-lg ${darkMode ? 'border-gray-700 bg-gray-900/50' : 'border-gray-200 bg-gray-50'}`}>
                <h3 className={`font-medium mb-2 ${darkMode ? 'text-white' : 'text-gray-900'}`}>
                  Start Rescraping
                </h3>
                <p className={`text-sm mb-3 ${darkMode ? 'text-gray-400' : 'text-gray-600'}`}>
                  Start automatic rescraping for creators that haven&apos;t been updated in 7+ days.
                </p>
                <div className="space-y-2">
                  <button
                    onClick={() => handleStartRescrape('daily')}
                    disabled={actionLoading.startsWith('rescrape')}
                    className="w-full inline-flex items-center justify-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:opacity-50"
                  >
                    {actionLoading === 'rescrape-daily' ? (
                      <ArrowPathIcon className="h-4 w-4 animate-spin mr-2" />
                    ) : (
                      <CalendarIcon className="h-4 w-4 mr-2" />
                    )}
                    Daily Rescrape (Recommended)
                  </button>
                  <div className="flex space-x-2">
                    <button
                      onClick={() => handleStartRescrape('instagram')}
                      disabled={actionLoading.startsWith('rescrape')}
                      className="flex-1 inline-flex items-center justify-center px-3 py-2 border border-transparent text-sm leading-4 font-medium rounded-md text-white bg-pink-600 hover:bg-pink-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-pink-500 disabled:opacity-50"
                    >
                      {actionLoading === 'rescrape-instagram' ? (
                        <ArrowPathIcon className="h-4 w-4 animate-spin mr-2" />
                      ) : (
                        <PlayIcon className="h-4 w-4 mr-2" />
                      )}
                      Instagram Only
                    </button>
                    <button
                      onClick={() => handleStartRescrape('tiktok')}
                      disabled={actionLoading.startsWith('rescrape')}
                      className="flex-1 inline-flex items-center justify-center px-3 py-2 border border-transparent text-sm leading-4 font-medium rounded-md text-white bg-black hover:bg-gray-800 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-gray-500 disabled:opacity-50"
                    >
                      {actionLoading === 'rescrape-tiktok' ? (
                        <ArrowPathIcon className="h-4 w-4 animate-spin mr-2" />
                      ) : (
                        <PlayIcon className="h-4 w-4 mr-2" />
                      )}
                      TikTok Only
                    </button>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Recent Jobs & Due Creators */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 mt-8">
          {/* Recent Jobs */}
          <div className={`rounded-lg shadow p-6 ${darkMode ? 'bg-gray-800' : 'bg-white'}`}>
            <h2 className={`text-lg font-medium mb-4 ${darkMode ? 'text-white' : 'text-gray-900'}`}>
              Recent Rescraping Jobs
            </h2>
            <div className="space-y-3">
              {stats?.recent_jobs && stats.recent_jobs.length > 0 ? (
                stats.recent_jobs.map((job) => (
                  <div key={job.id} className={`p-3 border rounded-lg ${darkMode ? 'border-gray-700' : 'border-gray-200'}`}>
                    <div className="flex items-center justify-between">
                      <div>
                        <span className={`font-medium ${darkMode ? 'text-white' : 'text-gray-900'}`}>
                          {job.job_type.replace(/_/g, ' ').replace(/\b\w/g, (l: string) => l.toUpperCase())}
                        </span>
                        <div className={`text-sm ${darkMode ? 'text-gray-400' : 'text-gray-500'}`}>
                          {job.total_items} creators • {formatDate(job.created_at)}
                        </div>
                      </div>
                      <span className={getStatusBadge(job.status)}>
                        {job.status}
                      </span>
                    </div>
                  </div>
                ))
              ) : (
                <p className={`text-sm ${darkMode ? 'text-gray-400' : 'text-gray-500'}`}>
                  No recent rescraping jobs
                </p>
              )}
            </div>
          </div>

          {/* Due Creators Sample */}
          <div className={`rounded-lg shadow p-6 ${darkMode ? 'bg-gray-800' : 'bg-white'}`}>
            <h2 className={`text-lg font-medium mb-4 ${darkMode ? 'text-white' : 'text-gray-900'}`}>
              Creators Due for Rescraping (Sample)
            </h2>
            <div className="space-y-3 max-h-64 overflow-y-auto">
              {dueCreators.length > 0 ? (
                dueCreators.slice(0, 10).map((creator) => (
                  <div key={creator.id} className={`p-3 border rounded-lg ${darkMode ? 'border-gray-700' : 'border-gray-200'}`}>
                    <div className="flex items-center justify-between">
                      <div>
                        <span className={`font-medium ${darkMode ? 'text-white' : 'text-gray-900'}`}>
                          @{creator.handle}
                        </span>
                        <div className={`text-sm ${darkMode ? 'text-gray-400' : 'text-gray-500'}`}>
                          {creator.platform} • {creator.primary_niche}
                        </div>
                      </div>
                      <div className={`text-xs ${darkMode ? 'text-gray-400' : 'text-gray-500'}`}>
                        {formatDate(creator.updated_at)}
                      </div>
                    </div>
                  </div>
                ))
              ) : (
                <p className={`text-sm ${darkMode ? 'text-gray-400' : 'text-gray-500'}`}>
                  No creators currently due for rescraping
                </p>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
