export interface Job {
  id: string
  job_type: 'new_creators' | 'rescrape_all' | 'rescrape_platform'
  status: 'pending' | 'queued' | 'running' | 'completed' | 'failed' | 'cancelled'
  description: string
  total_items?: number
  processed_items?: number
  failed_items?: number
  results?: {
    added?: string[]
    updated?: string[]
    deleted?: string[]
    failed?: string[]
    skipped?: string[]
    filtered?: string[]
    niche_stats?: {
      primary_niches?: { [key: string]: number }
      secondary_niches?: { [key: string]: number }
    }
  }
  error_message?: string
  created_at: string
  updated_at: string
}

export interface DashboardStats {
  total_creators: number
  instagram_creators: number
  tiktok_creators: number
  recent_jobs: Job[]
  job_stats: {
    pending: number
    running: number
    completed: number
    failed: number
    cancelled: number
  }
}

export interface LoginCredentials {
  username: string
  password: string
}

export interface JobRequest {
  job_type: 'new_creators' | 'rescrape_all' | 'rescrape_platform'
  platform?: string
  description?: string
}

export interface ApiResponse<T> {
  data?: T
  error?: string
  message?: string
}
