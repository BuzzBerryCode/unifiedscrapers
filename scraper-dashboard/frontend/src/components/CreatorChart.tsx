'use client'

import { useEffect, useRef } from 'react'
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
      const jobDate = new Date(job.created_at).toISOString().split('T')[0]
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
        borderColor: '#059669',
        borderWidth: 1,
      },
      {
        label: 'Not Added (Filtered/Failed)',
        data: dailyData.map(d => d.notAdded),
        backgroundColor: '#F59E0B', // Amber
        borderColor: '#D97706',
        borderWidth: 1,
      },
      {
        label: 'Rescraped Creators',
        data: dailyData.map(d => d.rescraped),
        backgroundColor: '#3B82F6', // Blue
        borderColor: '#2563EB',
        borderWidth: 1,
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
            size: 12
          }
        }
      },
      title: {
        display: true,
        text: 'Daily Creator Activity (Last 7 Days)',
        color: darkMode ? '#F9FAFB' : '#111827',
        font: {
          size: 16,
          weight: 'bold'
        }
      },
      tooltip: {
        backgroundColor: darkMode ? '#374151' : '#FFFFFF',
        titleColor: darkMode ? '#F9FAFB' : '#111827',
        bodyColor: darkMode ? '#E5E7EB' : '#374151',
        borderColor: darkMode ? '#6B7280' : '#D1D5DB',
        borderWidth: 1,
      }
    },
    scales: {
      x: {
        grid: {
          color: darkMode ? '#374151' : '#E5E7EB',
        },
        ticks: {
          color: darkMode ? '#9CA3AF' : '#6B7280',
        }
      },
      y: {
        beginAtZero: true,
        grid: {
          color: darkMode ? '#374151' : '#E5E7EB',
        },
        ticks: {
          color: darkMode ? '#9CA3AF' : '#6B7280',
          stepSize: 1,
        }
      }
    }
  }

  return (
    <div className={`p-6 rounded-lg shadow-sm border transition-colors ${
      darkMode 
        ? 'bg-gray-800 border-gray-700' 
        : 'bg-white border-gray-200'
    }`}>
      <div style={{ height: '300px' }}>
        <Bar data={chartData} options={options} />
      </div>
    </div>
  )
}
