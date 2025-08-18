'use client'

import { useState, useRef } from 'react'
import { CloudArrowUpIcon, DocumentIcon } from '@heroicons/react/24/outline'

interface FileUploadProps {
  onFileUpload: (file: File) => Promise<void>
}

export default function FileUpload({ onFileUpload }: FileUploadProps) {
  const [isDragging, setIsDragging] = useState(false)
  const [selectedFile, setSelectedFile] = useState<File | null>(null)
  const [uploading, setUploading] = useState(false)
  const fileInputRef = useRef<HTMLInputElement>(null)

  const handleDragOver = (e: React.DragEvent) => {
    e.preventDefault()
    setIsDragging(true)
  }

  const handleDragLeave = (e: React.DragEvent) => {
    e.preventDefault()
    setIsDragging(false)
  }

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault()
    setIsDragging(false)
    
    const files = Array.from(e.dataTransfer.files)
    const csvFile = files.find(file => file.name.endsWith('.csv'))
    
    if (csvFile) {
      setSelectedFile(csvFile)
    }
  }

  const handleFileSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (file && file.name.endsWith('.csv')) {
      setSelectedFile(file)
    }
  }

  const handleUpload = async () => {
    if (!selectedFile) return
    
    setUploading(true)
    try {
      await onFileUpload(selectedFile)
      setSelectedFile(null)
      if (fileInputRef.current) {
        fileInputRef.current.value = ''
      }
    } finally {
      setUploading(false)
    }
  }

  return (
    <div className="col-span-full">
      <div
        className={`relative border-2 border-dashed rounded-lg p-6 transition-colors ${
          isDragging
            ? 'border-blue-400 bg-blue-50'
            : 'border-gray-300 hover:border-gray-400'
        }`}
        onDragOver={handleDragOver}
        onDragLeave={handleDragLeave}
        onDrop={handleDrop}
      >
        <div className="text-center">
          <CloudArrowUpIcon className="mx-auto h-12 w-12 text-gray-400" />
          <div className="mt-4">
            <label htmlFor="file-upload" className="cursor-pointer">
              <span className="text-sm font-medium text-blue-600 hover:text-blue-500">
                Upload CSV file
              </span>
              <input
                ref={fileInputRef}
                id="file-upload"
                name="file-upload"
                type="file"
                accept=".csv"
                className="sr-only"
                onChange={handleFileSelect}
              />
            </label>
            <p className="text-sm text-gray-500 mt-1">
              or drag and drop
            </p>
          </div>
          <p className="text-xs text-gray-500 mt-2">
            CSV with Usernames and Platform columns
          </p>
        </div>
      </div>

      {selectedFile && (
        <div className="mt-4 flex items-center justify-between p-3 bg-gray-50 rounded-md">
          <div className="flex items-center">
            <DocumentIcon className="h-5 w-5 text-gray-400 mr-2" />
            <span className="text-sm text-gray-900">{selectedFile.name}</span>
            <span className="text-xs text-gray-500 ml-2">
              ({(selectedFile.size / 1024).toFixed(1)} KB)
            </span>
          </div>
          <button
            onClick={handleUpload}
            disabled={uploading}
            className="px-3 py-1 text-xs font-medium text-white bg-blue-600 hover:bg-blue-700 rounded-md disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {uploading ? (
              <div className="flex items-center">
                <div className="animate-spin rounded-full h-3 w-3 border-b border-white mr-1"></div>
                Uploading...
              </div>
            ) : (
              'Upload & Process'
            )}
          </button>
        </div>
      )}
    </div>
  )
}
