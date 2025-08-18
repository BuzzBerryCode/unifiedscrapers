'use client'

import React from 'react'
import { 
  UsersIcon, 
  ChartBarIcon,
  MapPinIcon,
  PlayIcon,
  CurrencyDollarIcon,
  BuildingOfficeIcon,
  GlobeAltIcon
} from '@heroicons/react/24/outline'
import { DashboardStats } from '@/types'

interface ModernStatsCardsProps {
  stats: DashboardStats
  darkMode?: boolean
}

interface LocationStats {
  [key: string]: number
}

interface NicheStats {
  crypto: number
  trading: number
  finance: number
}

export default function ModernStatsCards({ stats, darkMode = false }: ModernStatsCardsProps) {
  // Mock data for niches and locations - in real app, this would come from the API
  const nicheStats: NicheStats = {
    crypto: Math.floor(stats.total_creators * 0.4),
    trading: Math.floor(stats.total_creators * 0.35),
    finance: Math.floor(stats.total_creators * 0.25)
  }

  const locationStats: LocationStats = {
    'United States': Math.floor(stats.total_creators * 0.35),
    'Global': Math.floor(stats.total_creators * 0.25),
    'United Kingdom': Math.floor(stats.total_creators * 0.15),
    'Canada': Math.floor(stats.total_creators * 0.12),
    'Australia': Math.floor(stats.total_creators * 0.08),
    'Other': Math.floor(stats.total_creators * 0.05)
  }

  const StatCard = ({ 
    title, 
    value, 
    icon: Icon, 
    color = 'blue',
    subtitle = ''
  }: {
    title: string
    value: string | number
    icon: React.ComponentType<React.SVGProps<SVGSVGElement>>
    color?: 'blue' | 'green' | 'purple' | 'yellow' | 'red' | 'indigo' | 'pink'
    subtitle?: string
  }) => {
    const colorClasses = {
      blue: darkMode ? 'from-blue-500/20 to-blue-600/20' : 'from-blue-50 to-blue-100',
      green: darkMode ? 'from-green-500/20 to-green-600/20' : 'from-green-50 to-green-100',
      purple: darkMode ? 'from-purple-500/20 to-purple-600/20' : 'from-purple-50 to-purple-100',
      yellow: darkMode ? 'from-yellow-500/20 to-yellow-600/20' : 'from-yellow-50 to-yellow-100',
      red: darkMode ? 'from-red-500/20 to-red-600/20' : 'from-red-50 to-red-100',
      indigo: darkMode ? 'from-indigo-500/20 to-indigo-600/20' : 'from-indigo-50 to-indigo-100',
      pink: darkMode ? 'from-pink-500/20 to-pink-600/20' : 'from-pink-50 to-pink-100'
    }

    const iconColorClasses = {
      blue: darkMode ? 'text-blue-400' : 'text-blue-600',
      green: darkMode ? 'text-green-400' : 'text-green-600',
      purple: darkMode ? 'text-purple-400' : 'text-purple-600',
      yellow: darkMode ? 'text-yellow-400' : 'text-yellow-600',
      red: darkMode ? 'text-red-400' : 'text-red-600',
      indigo: darkMode ? 'text-indigo-400' : 'text-indigo-600',
      pink: darkMode ? 'text-pink-400' : 'text-pink-600'
    }

    return (
      <div className={`relative overflow-hidden rounded-xl p-6 transition-all duration-200 hover:scale-105 ${
        darkMode 
          ? 'bg-gradient-to-br from-gray-800 to-gray-900 border border-gray-700 shadow-lg hover:shadow-xl' 
          : 'bg-gradient-to-br from-white to-gray-50 border border-gray-200 shadow-md hover:shadow-lg'
      }`}>
        <div className="flex items-center justify-between">
          <div className="flex-1">
            <p className={`text-sm font-medium ${
              darkMode ? 'text-gray-300' : 'text-gray-600'
            }`}>
              {title}
            </p>
            <p className={`text-2xl font-bold mt-1 ${
              darkMode ? 'text-white' : 'text-gray-900'
            }`}>
              {typeof value === 'number' ? value.toLocaleString() : value}
            </p>
            {subtitle && (
              <p className={`text-xs mt-1 ${
                darkMode ? 'text-gray-400' : 'text-gray-500'
              }`}>
                {subtitle}
              </p>
            )}
          </div>
          <div className={`p-3 rounded-full bg-gradient-to-r ${colorClasses[color]} ${
            darkMode ? 'shadow-md' : 'shadow-sm'
          }`}>
            <Icon className={`h-6 w-6 ${iconColorClasses[color]}`} />
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-6 mb-8">
      {/* Main Platform Stats */}
      <div>
        <h2 className={`text-lg font-semibold mb-4 ${darkMode ? 'text-white' : 'text-gray-900'}`}>
          Platform Overview
        </h2>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          <StatCard
            title="Total Creators"
            value={stats.total_creators}
            icon={UsersIcon}
            color="blue"
            subtitle="Active influencers"
          />
          <StatCard
            title="Instagram"
            value={stats.instagram_creators}
            icon={ChartBarIcon}
            color="purple"
            subtitle={`${Math.round((stats.instagram_creators / stats.total_creators) * 100)}% of total`}
          />
          <StatCard
            title="TikTok"
            value={stats.tiktok_creators}
            icon={PlayIcon}
            color="pink"
            subtitle={`${Math.round((stats.tiktok_creators / stats.total_creators) * 100)}% of total`}
          />
        </div>
      </div>

      {/* Niche Breakdown */}
      <div>
        <h2 className={`text-lg font-semibold mb-4 ${darkMode ? 'text-white' : 'text-gray-900'}`}>
          Niche Distribution
        </h2>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          <StatCard
            title="Crypto"
            value={nicheStats.crypto}
            icon={CurrencyDollarIcon}
            color="yellow"
            subtitle="Cryptocurrency & Blockchain"
          />
          <StatCard
            title="Trading"
            value={nicheStats.trading}
            icon={ChartBarIcon}
            color="green"
            subtitle="Stock & Forex Trading"
          />
          <StatCard
            title="Finance"
            value={nicheStats.finance}
            icon={BuildingOfficeIcon}
            color="indigo"
            subtitle="Personal Finance & Investing"
          />
        </div>
      </div>

      {/* Location Breakdown */}
      <div>
        <h2 className={`text-lg font-semibold mb-4 ${darkMode ? 'text-white' : 'text-gray-900'}`}>
          Geographic Distribution
        </h2>
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
          {Object.entries(locationStats).map(([location, count], index) => {
            const colors = ['blue', 'green', 'purple', 'yellow', 'red', 'indigo'] as const
            return (
              <StatCard
                key={location}
                title={location}
                value={count}
                icon={location === 'Global' ? GlobeAltIcon : MapPinIcon}
                color={colors[index % colors.length]}
                subtitle={`${Math.round((count / stats.total_creators) * 100)}%`}
              />
            )
          })}
        </div>
      </div>
    </div>
  )
}
