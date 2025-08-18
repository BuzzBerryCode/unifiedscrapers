'use client'

import { useState, useEffect } from 'react'
import Image from 'next/image'
import { toast, Toaster } from 'react-hot-toast'
import { 
  CloudArrowUpIcon, 
  PlayIcon, 
  PauseIcon,
  TrashIcon,
  ChartBarIcon,
  UsersIcon,
  ClockIcon,
  CheckCircleIcon,
  XCircleIcon,
  ExclamationTriangleIcon,
  SunIcon,
  MoonIcon
} from '@heroicons/react/24/outline'
import LoginForm from '@/components/LoginForm'
import FileUpload from '@/components/FileUpload'
import JobsTable from '@/components/JobsTable'
import CreatorChart from '@/components/CreatorChart'
import ModernStatsCards from '@/components/ModernStatsCards'
import { Job, DashboardStats } from '@/types'

export default function Dashboard() {
  const [isAuthenticated, setIsAuthenticated] = useState(false)
  const [loading, setLoading] = useState(false)
  const [jobs, setJobs] = useState<Job[]>([])
  const [stats, setStats] = useState<DashboardStats | null>(null)
  const [token, setToken] = useState<string | null>(null)
  const [darkMode, setDarkMode] = useState(true) // Default to dark mode

  useEffect(() => {
    // Check if user is already logged in
    const savedToken = localStorage.getItem('token')
    if (savedToken) {
      setToken(savedToken)
      setIsAuthenticated(true)
      fetchData()
    }
  }, [])

  useEffect(() => {
    if (isAuthenticated) {
      // Fetch data every 5 seconds for real-time updates
      const interval = setInterval(fetchData, 5000)
      return () => clearInterval(interval)
    }
  }, [isAuthenticated])

  const fetchData = async () => {
    if (!token) return

    try {
      // Fetch jobs and stats in parallel
      const [jobsResponse, statsResponse] = await Promise.all([
        fetch(`${process.env.NEXT_PUBLIC_API_URL}/jobs`, {
          headers: { Authorization: `Bearer ${token}` }
        }),
        fetch(`${process.env.NEXT_PUBLIC_API_URL}/stats`, {
          headers: { Authorization: `Bearer ${token}` }
        })
      ])

      if (jobsResponse.ok) {
        const jobsData = await jobsResponse.json()
        setJobs(jobsData)
      }

      if (statsResponse.ok) {
        const statsData = await statsResponse.json()
        setStats(statsData)
      }
    } catch (error) {
      console.error('Error fetching data:', error)
    }
  }

  const handleLogin = async (username: string, password: string) => {
    setLoading(true)
    try {
      const response = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/auth/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username, password })
      })

      if (response.ok) {
        const data = await response.json()
        setToken(data.access_token)
        localStorage.setItem('token', data.access_token)
        setIsAuthenticated(true)
        toast.success('Successfully logged in!')
        fetchData()
      } else {
        toast.error('Invalid credentials')
      }
    } catch {
      toast.error('Login failed. Please try again.')
    } finally {
      setLoading(false)
    }
  }

  const handleLogout = () => {
    localStorage.removeItem('token')
    setToken(null)
    setIsAuthenticated(false)
    setJobs([])
    setStats(null)
    toast.success('Logged out successfully')
  }

  const handleFileUpload = async (file: File) => {
    if (!token) return

    const formData = new FormData()
    formData.append('file', file)

    try {
      const response = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/jobs/upload-csv`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${token}` },
        body: formData
      })

      if (response.ok) {
        const data = await response.json()
        toast.success(`Job created! Processing ${data.creators_count} creators`)
        fetchData() // Refresh jobs list
      } else {
        const error = await response.json()
        toast.error(error.detail || 'Upload failed')
      }
    } catch {
      toast.error('Upload failed. Please try again.')
    }
  }

  const handleRescrapeAll = async () => {
    if (!token) return

    try {
      const response = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/jobs/rescrape`, {
        method: 'POST',
        headers: { 
          Authorization: `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ job_type: 'rescrape_all' })
      })

      if (response.ok) {
        const data = await response.json()
        toast.success(`Rescrape job created! Processing ${data.total_items} creators`)
        fetchData()
      } else {
        const error = await response.json()
        toast.error(error.detail || 'Failed to create rescrape job')
      }
    } catch {
      toast.error('Failed to create rescrape job')
    }
  }

  const handleRescrapeInstagram = async () => {
    if (!token) return

    try {
      const response = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/jobs/rescrape`, {
        method: 'POST',
        headers: { 
          Authorization: `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ 
          job_type: 'rescrape_platform',
          platform: 'instagram'
        })
      })

      if (response.ok) {
        const data = await response.json()
        toast.success(`Instagram rescrape job created! Processing ${data.total_items} creators`)
        fetchData()
      } else {
        const error = await response.json()
        toast.error(error.detail || 'Failed to create Instagram rescrape job')
      }
    } catch {
      toast.error('Failed to create Instagram rescrape job')
    }
  }

  const handleRescrapeTikTok = async () => {
    if (!token) return

    try {
      const response = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/jobs/rescrape`, {
        method: 'POST',
        headers: { 
          Authorization: `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ 
          job_type: 'rescrape_platform',
          platform: 'tiktok'
        })
      })

      if (response.ok) {
        const data = await response.json()
        toast.success(`TikTok rescrape job created! Processing ${data.total_items} creators`)
        fetchData()
      } else {
        const error = await response.json()
        toast.error(error.detail || 'Failed to create TikTok rescrape job')
      }
    } catch {
      toast.error('Failed to create TikTok rescrape job')
    }
  }

  const handleCancelJob = async (jobId: string) => {
    if (!token) return

    try {
      const response = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/jobs/${jobId}`, {
        method: 'DELETE',
        headers: { Authorization: `Bearer ${token}` }
      })

      if (response.ok) {
        toast.success('Job cancelled successfully')
        fetchData()
      } else {
        toast.error('Failed to cancel job')
      }
    } catch {
      toast.error('Failed to cancel job')
    }
  }

  if (!isAuthenticated) {
    return (
      <div className="min-h-screen bg-gray-900 flex items-center justify-center">
        <div className="max-w-md w-full">
          <div className="bg-gray-800 shadow-lg rounded-lg p-8 border border-gray-700">
            <div className="text-center mb-8">
              <div className="flex items-center justify-center mb-4">
                <Image 
                  src="/Buzzberry.png" 
                  alt="BuzzBerry Logo" 
                  width={48} 
                  height={48} 
                  className="rounded-xl"
                />
              </div>
              <h1 className="text-3xl font-bold text-white mb-2">
                Scraper Dashboard
              </h1>
              <p className="text-gray-300">
                Admin access required
              </p>
            </div>
            <LoginForm onLogin={handleLogin} loading={loading} darkMode={true} />
          </div>
        </div>
        <Toaster position="top-right" />
      </div>
    )
  }

  return (
    <div className={`min-h-screen transition-colors ${darkMode ? 'bg-gray-900' : 'bg-gray-50'}`}>
      <Toaster position="top-right" />
      
      {/* Header */}
      <header className={`shadow-sm border-b transition-colors ${
        darkMode ? 'bg-gray-800 border-gray-700' : 'bg-white border-gray-200'
      }`}>
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between items-center h-16">
            <div className="flex items-center space-x-3">
              <Image 
                src="/Buzzberry.png" 
                alt="BuzzBerry Logo" 
                width={32} 
                height={32} 
                className="rounded-lg"
              />
              <h1 className={`text-2xl font-bold ${darkMode ? 'text-white' : 'text-gray-900'}`}>
                Scraper Dashboard
              </h1>
            </div>
            <div className="flex items-center space-x-4">
              <button
                onClick={() => setDarkMode(!darkMode)}
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
        {/* Quick Actions - Moved to top */}
        <div className={`rounded-xl shadow-sm border p-6 mb-6 transition-all duration-200 ${
          darkMode 
            ? 'bg-gray-800 border-gray-700' 
            : 'bg-white border-gray-200'
        }`}>
          <h2 className={`text-lg font-semibold mb-4 ${darkMode ? 'text-white' : 'text-gray-900'}`}>
            Quick Actions
          </h2>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            <FileUpload onFileUpload={handleFileUpload} />
            
            <button
              onClick={handleRescrapeAll}
              className="flex items-center justify-center px-4 py-3 border border-transparent text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
            >
              <PlayIcon className="w-5 h-5 mr-2" />
              Rescrape All
            </button>
            
            <button
              onClick={handleRescrapeInstagram}
              className="flex items-center justify-center px-4 py-3 border border-transparent text-sm font-medium rounded-md text-white bg-pink-600 hover:bg-pink-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-pink-500"
            >
              <PlayIcon className="w-5 h-5 mr-2" />
              Rescrape Instagram
            </button>
            
            <button
              onClick={handleRescrapeTikTok}
              className="flex items-center justify-center px-4 py-3 border border-transparent text-sm font-medium rounded-md text-white bg-black hover:bg-gray-800 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-gray-500"
            >
              <PlayIcon className="w-5 h-5 mr-2" />
              Rescrape TikTok
            </button>
          </div>
        </div>

        {/* Recent Jobs - Moved up */}
        <div className={`rounded-xl shadow-sm border mb-8 transition-all duration-200 ${
          darkMode 
            ? 'bg-gray-800 border-gray-700' 
            : 'bg-white border-gray-200'
        }`}>
          <div className={`px-6 py-4 border-b transition-colors ${
            darkMode ? 'border-gray-700' : 'border-gray-200'
          }`}>
            <h2 className={`text-lg font-semibold ${darkMode ? 'text-white' : 'text-gray-900'}`}>
              Recent Jobs
            </h2>
          </div>
          <JobsTable jobs={jobs} onCancelJob={handleCancelJob} darkMode={darkMode} />
        </div>

        {/* Modern Stats Cards */}
        {stats && <ModernStatsCards stats={stats} darkMode={darkMode} />}

        {/* Creator Activity Chart */}
        <div className="mb-8">
          <CreatorChart jobs={jobs} darkMode={darkMode} />
        </div>
      </main>
    </div>
  )
}