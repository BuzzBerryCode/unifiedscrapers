'use client'

import { useState, useEffect } from 'react'
import Image from 'next/image'
import Link from 'next/link'
import { toast, Toaster } from 'react-hot-toast'
import { 
  ArrowPathIcon,
  CalendarIcon,
  ClockIcon,
  UserGroupIcon,
  PlayIcon,
  Cog6ToothIcon,
  SunIcon,
  MoonIcon,
  ExclamationTriangleIcon,
  FireIcon,
  BoltIcon,
  WrenchScrewdriverIcon,
  ShieldExclamationIcon
} from '@heroicons/react/24/outline'

interface RescrapeStats {
  total_creators: number
  creators_need_dates: number
  creators_due_rescrape: number
  total_overdue_creators: number
  todays_scheduled_batch: number
  remaining_overdue: number
  weekly_schedule: { [key: string]: { 
    date: string; 
    day: string; 
    estimated_creators: number; 
    scheduled_time: string;
    scheduled_time_utc: string;
    is_today?: boolean; 
    is_past_time?: boolean 
  } }
  recent_jobs: Array<{
    id: string
    job_type: string
    status: string
    total_items: number
    created_at: string
  }>
  schedule_info: {
    time_sf: string
    time_utc: string
    description: string
  }
}

interface CorruptedCreator {
  id: string
  handle: string
  platform: string
  primary_niche: string | null
  secondary_niche: string | null
  updated_at: string
  followers_count: number
  average_views: number
  engagement_rate: number
  issues: string[]
}

interface CorruptedData {
  corrupted_creators: CorruptedCreator[]
  total_count: number
  missing_niches_count: number
  zero_metrics_count: number
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
  const [corruptedData, setCorruptedData] = useState<CorruptedData | null>(null)
  const [loading, setLoading] = useState(true)
  const [actionLoading, setActionLoading] = useState('')
  const [darkMode, setDarkMode] = useState(true) // Default to dark mode

  // Check dark mode on component mount and handle authentication
  useEffect(() => {
    // Check authentication first
    const token = localStorage.getItem('token')
    if (!token) {
      window.location.href = '/'
      return
    }

    // Set dark mode to true by default
    setDarkMode(true)
    document.documentElement.classList.add('dark')
  }, [])

  const handleLogout = () => {
    localStorage.removeItem('token')
    window.location.href = '/'
  }

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

      // Fetch corrupted creators
      const corruptedResponse = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/rescraping/corrupted-creators`, {
        headers: { 'Authorization': `Bearer ${token}` }
      })
      
      if (corruptedResponse.ok) {
        const corruptedData = await corruptedResponse.json()
        setCorruptedData(corruptedData)
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

  const handleStartOverdueRescrape = async (platform: string) => {
    setActionLoading(`overdue-${platform}`)
    try {
      const token = localStorage.getItem('token')
      
      const response = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/rescraping/start-overdue-only`, {
        method: 'POST',
        headers: { 
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ 
          platform, 
          max_creators: 200  // Higher limit for cleanup runs
        })
      })
      
      if (response.ok) {
        const result = await response.json()
        toast.success(result.message)
        fetchData() // Refresh data
      } else {
        throw new Error('Failed to start overdue rescraping')
      }
    } catch (error) {
      console.error('Error starting overdue rescrape:', error)
      toast.error('Failed to start overdue rescraping')
    } finally {
      setActionLoading('')
    }
  }

  const handleStartTodaysBatch = async (platform: string) => {
    setActionLoading(`todays-${platform}`)
    try {
      const token = localStorage.getItem('token')
      
      const response = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/rescraping/start-todays-batch`, {
        method: 'POST',
        headers: { 
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ platform })
      })
      
      if (response.ok) {
        const result = await response.json()
        toast.success(result.message)
        fetchData() // Refresh data
      } else {
        throw new Error('Failed to start today\'s batch rescraping')
      }
    } catch (error) {
      console.error('Error starting today\'s batch rescrape:', error)
      toast.error('Failed to start today\'s batch rescraping')
    } finally {
      setActionLoading('')
    }
  }

  const handleFixCorruptedCreators = async (platform: string) => {
    setActionLoading(`fix-corrupted-${platform}`)
    try {
      const token = localStorage.getItem('token')
      
      const response = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/rescraping/fix-corrupted-creators`, {
        method: 'POST',
        headers: { 
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ 
          platform, 
          max_creators: 100  // Reasonable limit for corruption fix
        })
      })
      
      if (response.ok) {
        const result = await response.json()
        toast.success(result.message)
        fetchData() // Refresh data
      } else {
        throw new Error('Failed to start corruption fix')
      }
    } catch (error) {
      console.error('Error starting corruption fix:', error)
      toast.error('Failed to start corruption fix')
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
      <Toaster position="top-right" />
      
      {/* Header */}
      <header className={`shadow-sm border-b transition-colors ${
        darkMode ? 'bg-gray-800 border-gray-700' : 'bg-white border-gray-200'
      }`}>
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between items-center h-16">
            <div className="flex items-center space-x-8">
              <div className="flex items-center space-x-3">
                <Image
                  src="/Buzzberry profile picture rounded corners-256x256.png" 
                  alt="BuzzBerry Logo" 
                  width={32} 
                  height={32} 
                  className="rounded-lg"
                />
                <h1 className={`text-2xl font-bold ${darkMode ? 'text-white' : 'text-gray-900'}`}>
                  Scraper Dashboard
                </h1>
              </div>
              
              {/* Navigation */}
              <nav className="flex items-center space-x-6">
                <Link
                  href="/"
                  className={`text-sm font-medium transition-colors ${
                    darkMode 
                      ? 'text-gray-300 hover:text-white' 
                      : 'text-gray-600 hover:text-gray-900'
                  }`}
                >
                  Dashboard
                </Link>
                <Link
                  href="/rescraping"
                  className={`text-sm font-medium transition-colors ${
                    darkMode 
                      ? 'text-white hover:text-gray-300' 
                      : 'text-gray-900 hover:text-gray-600'
                  }`}
                >
                  Rescraping
                </Link>
              </nav>
            </div>
            <div className="flex items-center space-x-4">
              <button
                onClick={() => {
                  const newDarkMode = !darkMode
                  setDarkMode(newDarkMode)
                  if (newDarkMode) {
                    document.documentElement.classList.add('dark')
                  } else {
                    document.documentElement.classList.remove('dark')
                  }
                }}
                className={`p-2 rounded-lg transition-colors ${
                  darkMode 
                    ? 'bg-gray-700 hover:bg-gray-600 text-yellow-400' 
                    : 'bg-gray-200 hover:bg-gray-300 text-gray-700'
                }`}
                title="Toggle dark mode"
              >
                {darkMode ? (
                  <SunIcon className="h-5 w-5" />
                ) : (
                  <MoonIcon className="h-5 w-5" />
                )}
              </button>
              <button
                onClick={handleLogout}
                className={`px-3 py-2 rounded-md text-sm font-medium transition-colors ${
                  darkMode 
                    ? 'text-gray-300 hover:text-white' 
                    : 'text-gray-500 hover:text-gray-700'
                }`}
              >
                Logout
              </button>
            </div>
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Header */}
        <div className="mb-8">
          <h1 className={`text-3xl font-bold ${darkMode ? 'text-white' : 'text-gray-900'}`}>
            Rescraping Management
          </h1>
          <p className={`mt-2 ${darkMode ? 'text-gray-400' : 'text-gray-600'}`}>
            Automatic rescraping at 8:00 AM San Francisco time includes ALL overdue creators. Monitor progress and handle selective cleanup.
          </p>
        </div>

        {/* Enhanced Stats Cards */}
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
              <CalendarIcon className="h-8 w-8 text-green-500" />
              <div className="ml-4">
                <p className={`text-sm font-medium ${darkMode ? 'text-gray-400' : 'text-gray-500'}`}>
                  Due Today
                </p>
                <p className={`text-2xl font-bold ${darkMode ? 'text-white' : 'text-gray-900'}`}>
                  {stats?.todays_scheduled_batch?.toLocaleString() || 0}
                </p>
                <p className={`text-xs ${darkMode ? 'text-gray-500' : 'text-gray-400'}`}>
                  7 days old
                </p>
              </div>
            </div>
          </div>

          <div className={`rounded-lg shadow p-6 ${darkMode ? 'bg-gray-800' : 'bg-white'}`}>
            <div className="flex items-center">
              <ExclamationTriangleIcon className={`h-8 w-8 ${(stats?.remaining_overdue || 0) > 0 ? 'text-orange-500' : 'text-gray-400'}`} />
              <div className="ml-4">
                <p className={`text-sm font-medium ${darkMode ? 'text-gray-400' : 'text-gray-500'}`}>
                  Overdue
                </p>
                <p className={`text-2xl font-bold ${(stats?.remaining_overdue || 0) > 0 ? 'text-orange-500' : (darkMode ? 'text-white' : 'text-gray-900')}`}>
                  {stats?.remaining_overdue?.toLocaleString() || 0}
                </p>
                <p className={`text-xs ${darkMode ? 'text-gray-500' : 'text-gray-400'}`}>
                  8+ days old
                </p>
              </div>
            </div>
          </div>

          <div className={`rounded-lg shadow p-6 ${darkMode ? 'bg-gray-800' : 'bg-white'}`}>
            <div className="flex items-center">
              <ClockIcon className="h-8 w-8 text-blue-500" />
              <div className="ml-4">
                <p className={`text-sm font-medium ${darkMode ? 'text-gray-400' : 'text-gray-500'}`}>
                  Schedule Time
                </p>
                <p className={`text-lg font-bold ${darkMode ? 'text-white' : 'text-gray-900'}`}>
                  {stats?.schedule_info?.time_sf || '8:00 AM PST/PDT'}
                </p>
                <p className={`text-xs ${darkMode ? 'text-gray-500' : 'text-gray-400'}`}>
                  {stats?.schedule_info?.time_utc || '15:00 UTC'}
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
              {stats?.weekly_schedule && Object.values(stats?.weekly_schedule || {}).map((day) => (
                <div key={day.day} className={`flex items-center justify-between p-3 rounded-lg ${
                  day.is_today 
                    ? (darkMode ? 'bg-blue-900/20 border border-blue-700' : 'bg-blue-50 border border-blue-200')
                    : (darkMode ? 'bg-gray-700/50' : 'bg-gray-50')
                }`}>
                  <div>
                    <div className="flex items-center">
                      <span className={`font-medium ${darkMode ? 'text-white' : 'text-gray-900'}`}>
                        {day.day}
                      </span>
                      {day.is_today && (
                        <span className="ml-2 text-xs px-2 py-1 rounded-full bg-blue-600 text-white">
                          TODAY
                        </span>
                      )}
                    </div>
                    <div className={`text-sm ${darkMode ? 'text-gray-400' : 'text-gray-500'}`}>
                      {day.date} • {day.scheduled_time} ({day.scheduled_time_utc})
                      <br />
                      {day.estimated_creators > 0 ? `${day.estimated_creators} creators due` : 'No creators due'}
                      {day.is_today && day.is_past_time && (
                        <span className="ml-2 text-xs px-2 py-1 rounded-full bg-orange-600 text-white">
                          OVERDUE
                        </span>
                      )}
                    </div>
                  </div>
                  <div className="flex items-center">
                    {day.estimated_creators > 0 && (
                      <>
                        <div className={`w-24 bg-gray-200 dark:bg-gray-600 rounded-full h-2 mr-3`}>
                          <div 
                            className={`h-2 rounded-full ${
                              day.is_today ? 'bg-blue-500' :
                              day.estimated_creators > 100 ? 'bg-red-500' :
                              day.estimated_creators > 50 ? 'bg-orange-500' : 'bg-green-500'
                            }`}
                            style={{ width: `${Math.min((day.estimated_creators / 200) * 100, 100)}%` }}
                          ></div>
                        </div>
                        <span className={`px-3 py-1 rounded-full text-sm font-medium ${
                          day.is_today
                            ? 'bg-blue-600 text-white'
                            : day.estimated_creators > 100
                            ? (darkMode ? 'bg-red-900/50 text-red-300' : 'bg-red-100 text-red-800')
                            : day.estimated_creators > 50
                            ? (darkMode ? 'bg-orange-900/50 text-orange-300' : 'bg-orange-100 text-orange-800')
                            : (darkMode ? 'bg-green-900/50 text-green-300' : 'bg-green-100 text-green-800')
                        }`}>
                          {day.estimated_creators}
                        </span>
                      </>
                    )}
                    {day.estimated_creators === 0 && (
                      <span className={`text-sm ${darkMode ? 'text-gray-500' : 'text-gray-400'}`}>
                        No jobs scheduled
                      </span>
                    )}
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
                    {stats?.creators_need_dates || 0} creators need updated_at dates. This will spread them across the past week.
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

              {/* Today's Missed Batch Section - Show when past scheduled time and creators still due */}
              {(stats?.todays_scheduled_batch || 0) > 0 && 
               stats?.weekly_schedule && 
               Object.values(stats.weekly_schedule).some(day => day.is_today && day.is_past_time) && (
                <div className={`p-4 border rounded-lg ${darkMode ? 'border-blue-600 bg-blue-900/10' : 'border-blue-300 bg-blue-50'}`}>
                  <h3 className={`font-medium mb-2 flex items-center ${darkMode ? 'text-blue-300' : 'text-blue-800'}`}>
                    <BoltIcon className="h-5 w-5 mr-2" />
                    Rescrape Today&apos;s Missed Batch
                  </h3>
                  <p className={`text-sm mb-3 ${darkMode ? 'text-blue-200' : 'text-blue-700'}`}>
                    {stats?.todays_scheduled_batch || 0} creators were scheduled for today&apos;s 8:00 AM rescrape but weren&apos;t processed. 
                    Run them manually now.
                  </p>
                  <div className="space-y-2">
                    <button
                      onClick={() => handleStartTodaysBatch('all')}
                      disabled={actionLoading.startsWith('todays')}
                      className="w-full inline-flex items-center justify-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:opacity-50"
                    >
                      {actionLoading === 'todays-all' ? (
                        <ArrowPathIcon className="h-4 w-4 animate-spin mr-2" />
                      ) : (
                        <BoltIcon className="h-4 w-4 mr-2" />
                      )}
                      Rescrape Today&apos;s Batch ({stats?.todays_scheduled_batch || 0})
                    </button>
                    <div className="flex space-x-2">
                      <button
                        onClick={() => handleStartTodaysBatch('instagram')}
                        disabled={actionLoading.startsWith('todays')}
                        className="flex-1 inline-flex items-center justify-center px-3 py-2 border border-transparent text-sm leading-4 font-medium rounded-md text-white bg-pink-600 hover:bg-pink-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-pink-500 disabled:opacity-50"
                      >
                        {actionLoading === 'todays-instagram' ? (
                          <ArrowPathIcon className="h-4 w-4 animate-spin mr-2" />
                        ) : (
                          <PlayIcon className="h-4 w-4 mr-2" />
                        )}
                        Instagram Only
                      </button>
                      <button
                        onClick={() => handleStartTodaysBatch('tiktok')}
                        disabled={actionLoading.startsWith('todays')}
                        className="flex-1 inline-flex items-center justify-center px-3 py-2 border border-transparent text-sm leading-4 font-medium rounded-md text-white bg-black hover:bg-gray-800 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-gray-500 disabled:opacity-50"
                      >
                        {actionLoading === 'todays-tiktok' ? (
                          <ArrowPathIcon className="h-4 w-4 animate-spin mr-2" />
                        ) : (
                          <PlayIcon className="h-4 w-4 mr-2" />
                        )}
                        TikTok Only
                      </button>
                    </div>
                  </div>
                </div>
              )}

              {/* Overdue Creators Section */}
              {(stats?.remaining_overdue || 0) > 0 && (
                <div className={`p-4 border rounded-lg ${darkMode ? 'border-orange-600 bg-orange-900/10' : 'border-orange-300 bg-orange-50'}`}>
                  <h3 className={`font-medium mb-2 flex items-center ${darkMode ? 'text-orange-300' : 'text-orange-800'}`}>
                    <FireIcon className="h-5 w-5 mr-2" />
                    Cleanup Overdue Creators
                  </h3>
                  <p className={`text-sm mb-3 ${darkMode ? 'text-orange-200' : 'text-orange-700'}`}>
                    {stats?.remaining_overdue || 0} creators are truly overdue (8+ days old) and need immediate attention. 
                    These missed their scheduled rescrape window and should be processed separately.
                  </p>
                  <div className="space-y-2">
                    <button
                      onClick={() => handleStartOverdueRescrape('all')}
                      disabled={actionLoading.startsWith('overdue')}
                      className="w-full inline-flex items-center justify-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-orange-600 hover:bg-orange-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-orange-500 disabled:opacity-50"
                    >
                      {actionLoading === 'overdue-all' ? (
                        <ArrowPathIcon className="h-4 w-4 animate-spin mr-2" />
                      ) : (
                        <FireIcon className="h-4 w-4 mr-2" />
                      )}
                      Rescrape Overdue Only ({stats?.remaining_overdue || 0})
                    </button>
                    <div className="flex space-x-2">
                      <button
                        onClick={() => handleStartOverdueRescrape('instagram')}
                        disabled={actionLoading.startsWith('overdue')}
                        className="flex-1 inline-flex items-center justify-center px-3 py-2 border border-transparent text-sm leading-4 font-medium rounded-md text-white bg-pink-600 hover:bg-pink-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-pink-500 disabled:opacity-50"
                      >
                        {actionLoading === 'overdue-instagram' ? (
                          <ArrowPathIcon className="h-4 w-4 animate-spin mr-2" />
                        ) : (
                          <PlayIcon className="h-4 w-4 mr-2" />
                        )}
                        Instagram Only
                      </button>
                      <button
                        onClick={() => handleStartOverdueRescrape('tiktok')}
                        disabled={actionLoading.startsWith('overdue')}
                        className="flex-1 inline-flex items-center justify-center px-3 py-2 border border-transparent text-sm leading-4 font-medium rounded-md text-white bg-black hover:bg-gray-800 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-gray-500 disabled:opacity-50"
                      >
                        {actionLoading === 'overdue-tiktok' ? (
                          <ArrowPathIcon className="h-4 w-4 animate-spin mr-2" />
                        ) : (
                          <PlayIcon className="h-4 w-4 mr-2" />
                        )}
                        TikTok Only
                      </button>
                    </div>
                  </div>
                </div>
              )}

              {/* Corrupted Data Fix Section */}
              {(corruptedData?.total_count || 0) > 0 && (
                <div className={`p-4 border rounded-lg ${darkMode ? 'border-red-600 bg-red-900/10' : 'border-red-300 bg-red-50'}`}>
                  <h3 className={`font-medium mb-2 flex items-center ${darkMode ? 'text-red-300' : 'text-red-800'}`}>
                    <WrenchScrewdriverIcon className="h-5 w-5 mr-2" />
                    Fix Corrupted Creator Data
                  </h3>
                  <p className={`text-sm mb-3 ${darkMode ? 'text-red-200' : 'text-red-700'}`}>
                    {corruptedData?.total_count || 0} creators have corrupted data from the rescraper bug: 
                    {corruptedData?.missing_niches_count || 0} missing niches, {corruptedData?.zero_metrics_count || 0} zero metrics.
                  </p>
                  
                  {/* Show sample of corrupted creators */}
                  {corruptedData && corruptedData.corrupted_creators.length > 0 && (
                    <div className={`mb-3 p-2 rounded ${darkMode ? 'bg-red-900/20' : 'bg-red-100'}`}>
                      <p className={`text-xs font-medium mb-1 ${darkMode ? 'text-red-300' : 'text-red-800'}`}>
                        Examples ({Math.min(3, corruptedData.corrupted_creators.length)} of {corruptedData.total_count}):
                      </p>
                      <div className="space-y-1">
                        {corruptedData.corrupted_creators.slice(0, 3).map(creator => (
                          <div key={creator.handle} className={`text-xs ${darkMode ? 'text-red-200' : 'text-red-700'}`}>
                            <span className="font-medium">@{creator.handle}</span> ({creator.platform}): 
                            {creator.issues.includes('missing_primary_niche') && ' Missing primary niche'}
                            {creator.issues.includes('missing_secondary_niche') && ' Missing secondary niche'}
                            {creator.issues.includes('zero_average_views') && ' Zero views'}
                            {creator.issues.includes('zero_engagement_rate') && ' Zero engagement rate'}
                          </div>
                        ))}
                      </div>
                    </div>
                  )}

                  <div className="space-y-2">
                    <button
                      onClick={() => handleFixCorruptedCreators('all')}
                      disabled={actionLoading.startsWith('fix-corrupted')}
                      className="w-full inline-flex items-center justify-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-red-600 hover:bg-red-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-red-500 disabled:opacity-50"
                    >
                      {actionLoading === 'fix-corrupted-all' ? (
                        <ArrowPathIcon className="h-4 w-4 animate-spin mr-2" />
                      ) : (
                        <WrenchScrewdriverIcon className="h-4 w-4 mr-2" />
                      )}
                      Fix All Corrupted Data ({corruptedData?.total_count || 0})
                    </button>
                    <div className="flex space-x-2">
                      <button
                        onClick={() => handleFixCorruptedCreators('instagram')}
                        disabled={actionLoading.startsWith('fix-corrupted')}
                        className="flex-1 inline-flex items-center justify-center px-3 py-2 border border-transparent text-sm leading-4 font-medium rounded-md text-white bg-pink-600 hover:bg-pink-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-pink-500 disabled:opacity-50"
                      >
                        {actionLoading === 'fix-corrupted-instagram' ? (
                          <ArrowPathIcon className="h-4 w-4 animate-spin mr-2" />
                        ) : (
                          <ShieldExclamationIcon className="h-4 w-4 mr-2" />
                        )}
                        Instagram Only
                      </button>
                      <button
                        onClick={() => handleFixCorruptedCreators('tiktok')}
                        disabled={actionLoading.startsWith('fix-corrupted')}
                        className="flex-1 inline-flex items-center justify-center px-3 py-2 border border-transparent text-sm leading-4 font-medium rounded-md text-white bg-black hover:bg-gray-800 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-gray-500 disabled:opacity-50"
                      >
                        {actionLoading === 'fix-corrupted-tiktok' ? (
                          <ArrowPathIcon className="h-4 w-4 animate-spin mr-2" />
                        ) : (
                          <ShieldExclamationIcon className="h-4 w-4 mr-2" />
                        )}
                        TikTok Only
                      </button>
                    </div>
                  </div>
                </div>
              )}

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
                stats?.recent_jobs?.map((job) => (
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
      </main>
    </div>
  )
}
// Force deployment Mon Aug 25 20:05:52 PDT 2025
