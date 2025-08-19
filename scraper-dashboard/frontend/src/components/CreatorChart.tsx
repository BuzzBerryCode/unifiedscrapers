'use client'

import React from 'react'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend,
  ChartOptions
} from 'chart.js'
import { Bar } from 'react-chartjs-2'
import { Job } from '@/types'

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend
)

interface CreatorChartProps {
  jobs: Job[]
  darkMode: boolean
}

interface DailyData {
  date: string
  added: number
  notAdded: number
  rescraped: number
}

export default function CreatorChart({ jobs, darkMode }: CreatorChartProps) {
  // Process jobs data to get daily statistics
  const processJobsData = (): DailyData[] => {
    const dailyMap = new Map<string, DailyData>()
    
    // Get last 7 days
    const today = new Date()
    for (let i = 6; i >= 0; i--) {
      const date = new Date(today)
      date.setDate(date.getDate() - i)
      const dateStr = date.toISOString().split('T')[0]
      dailyMap.set(dateStr, {
        date: dateStr,
        added: 0,
        notAdded: 0,
        rescraped: 0
      })
    }
    
    // Process completed jobs
    jobs.filter(job => job.status === 'completed').forEach(job => {
      // Use updated_at (completion date) for rescrape jobs, created_at for new creator jobs
      const dateToUse = job.job_type.includes('rescrape') ? job.updated_at : job.created_at
      const jobDate = new Date(dateToUse).toISOString().split('T')[0]
      const dayData = dailyMap.get(jobDate)
      
      if (dayData && job.results) {
        if (job.job_type === 'new_creators') {
          // New creators job
          dayData.added += job.results.added?.length || 0
          dayData.notAdded += (job.results.failed?.length || 0) + 
                             (job.results.filtered?.length || 0) + 
                             (job.results.skipped?.length || 0)
        } else if (job.job_type.includes('rescrape')) {
          // Rescraping job
          dayData.rescraped += job.results.updated?.length || 0
        }
      }
    })
    
    return Array.from(dailyMap.values())
  }

  const dailyData = processJobsData()
  
  const chartData = {
    labels: dailyData.map(d => {
      const date = new Date(d.date)
      return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
    }),
    datasets: [
      {
        label: 'Added Creators',
        data: dailyData.map(d => d.added),
        backgroundColor: '#10B981', // Green
        borderColor: '#10B981',
        borderWidth: 0,
        borderRadius: 8,
        borderSkipped: false,
      },
      {
        label: 'Not Added',
        data: dailyData.map(d => d.notAdded),
        backgroundColor: '#EF4444', // Red
        borderColor: '#EF4444',
        borderWidth: 0,
        borderRadius: 8,
        borderSkipped: false,
      },
      {
        label: 'Rescraped Creators',
        data: dailyData.map(d => d.rescraped),
        backgroundColor: '#8B5CF6', // Purple
        borderColor: '#8B5CF6',
        borderWidth: 0,
        borderRadius: 8,
        borderSkipped: false,
      }
    ]
  }

  const options: ChartOptions<'bar'> = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        position: 'top' as const,
        labels: {
          color: darkMode ? '#E5E7EB' : '#374151',
          font: {
            size: 13,
            weight: 'bold'
          },
          padding: 20,
          usePointStyle: true,
          pointStyle: 'circle'
        }
      },
      title: {
        display: false
      },
      tooltip: {
        backgroundColor: darkMode ? '#1F2937' : '#FFFFFF',
        titleColor: darkMode ? '#F9FAFB' : '#111827',
        bodyColor: darkMode ? '#E5E7EB' : '#374151',
        borderColor: darkMode ? '#374151' : '#E5E7EB',
        borderWidth: 1,
        cornerRadius: 12,
        padding: 12,
        displayColors: true,
        titleFont: {
          size: 14,
          weight: 'bold'
        },
        bodyFont: {
          size: 13
        }
      }
    },
    scales: {
      x: {
        grid: {
          display: false
        },
        ticks: {
          color: darkMode ? '#9CA3AF' : '#6B7280',
          font: {
            size: 12,
            weight: 'normal'
          },
          padding: 8
        },
        border: {
          display: false
        }
      },
      y: {
        beginAtZero: true,
        grid: {
          display: false
        },
        ticks: {
          color: darkMode ? '#9CA3AF' : '#6B7280',
          stepSize: 1,
          font: {
            size: 12,
            weight: 'normal'
          },
          padding: 8
        },
        border: {
          display: false
        }
      }
    },
    elements: {
      bar: {
        borderRadius: 8
      }
    },
    interaction: {
      intersect: false,
      mode: 'index'
    }
  }

  return (
    <div className={`rounded-xl shadow-lg border transition-all duration-200 ${
      darkMode 
        ? 'bg-gradient-to-br from-gray-800 to-gray-900 border-gray-700' 
        : 'bg-gradient-to-br from-white to-gray-50 border-gray-200'
    }`}>
      <div className="p-6 border-b border-gray-200 dark:border-gray-700">
        <h3 className={`text-lg font-semibold ${darkMode ? 'text-white' : 'text-gray-900'}`}>
          Daily Creator Activity
        </h3>
        <p className={`text-sm mt-1 ${darkMode ? 'text-gray-400' : 'text-gray-600'}`}>
          Last 7 days performance overview
        </p>
      </div>
      <div className="p-6">
        <div style={{ height: '320px' }}>
          <Bar data={chartData} options={options} />
        </div>
      </div>
    </div>
  )
}

