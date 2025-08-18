'use client'

import { 
  UsersIcon, 
  ChartBarIcon,
  ClockIcon,
  CheckCircleIcon,
  XCircleIcon,
  ExclamationTriangleIcon
} from '@heroicons/react/24/outline'
import { DashboardStats } from '@/types'

interface StatsCardsProps {
  stats: DashboardStats
}

export default function StatsCards({ stats }: StatsCardsProps) {
  const cards = [
    {
      title: 'Total Creators',
      value: stats.total_creators.toLocaleString(),
      icon: UsersIcon,
      color: 'bg-blue-500',
      description: 'All creators in database'
    },
    {
      title: 'Instagram',
      value: stats.instagram_creators.toLocaleString(),
      icon: ChartBarIcon,
      color: 'bg-pink-500',
      description: 'Instagram creators'
    },
    {
      title: 'TikTok',
      value: stats.tiktok_creators.toLocaleString(),
      icon: ChartBarIcon,
      color: 'bg-black',
      description: 'TikTok creators'
    },
    {
      title: 'Running Jobs',
      value: stats.job_stats.running.toString(),
      icon: ClockIcon,
      color: 'bg-yellow-500',
      description: 'Currently processing'
    },
    {
      title: 'Completed Jobs',
      value: stats.job_stats.completed.toString(),
      icon: CheckCircleIcon,
      color: 'bg-green-500',
      description: 'Successfully completed'
    },
    {
      title: 'Failed Jobs',
      value: stats.job_stats.failed.toString(),
      icon: XCircleIcon,
      color: 'bg-red-500',
      description: 'Failed or errored'
    }
  ]

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-6 gap-6 mb-8">
      {cards.map((card, index) => (
        <div key={index} className="bg-white overflow-hidden shadow rounded-lg">
          <div className="p-5">
            <div className="flex items-center">
              <div className="flex-shrink-0">
                <div className={`${card.color} rounded-md p-3`}>
                  <card.icon className="h-6 w-6 text-white" aria-hidden="true" />
                </div>
              </div>
              <div className="ml-5 w-0 flex-1">
                <dl>
                  <dt className="text-sm font-medium text-gray-500 truncate">
                    {card.title}
                  </dt>
                  <dd className="text-lg font-medium text-gray-900">
                    {card.value}
                  </dd>
                </dl>
              </div>
            </div>
            <div className="mt-3">
              <p className="text-xs text-gray-500">
                {card.description}
              </p>
            </div>
          </div>
        </div>
      ))}
    </div>
  )
}
