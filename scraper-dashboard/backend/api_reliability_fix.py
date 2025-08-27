"""
API Reliability Fix for Unified Scrapers
========================================

This module provides robust API calling with:
- Standardized retry logic with exponential backoff
- Proper error categorization and handling
- Rate limit management
- Timeout protection
- Circuit breaker pattern for repeated failures
"""

import requests
import time
import random
from typing import Dict, Optional, Tuple
from datetime import datetime, timedelta
import json

class APIReliabilityManager:
    """Manages reliable API calls with retry logic and error handling."""
    
    def __init__(self, api_key: str, fast_mode: bool = True):
        self.api_key = api_key
        self.headers = {"x-api-key": api_key}
        self.fast_mode = fast_mode
        
        # Retry Configuration (optimized for speed)
        if fast_mode:
            self.MAX_RETRIES = 3  # Faster: 3 retries instead of 5
            self.BASE_DELAY = 1   # Faster: 1s base delay instead of 2s
            self.MAX_DELAY = 60   # Faster: 1 minute max instead of 2 minutes
            self.REQUEST_TIMEOUT = 45  # Faster: 45s timeout instead of 60s
            self.circuit_breaker_threshold = 3  # Faster: 3 failures instead of 5
            self.circuit_breaker_reset_time = 15  # Faster: 15s instead of 30s
        else:
            self.MAX_RETRIES = 5
            self.BASE_DELAY = 2
            self.MAX_DELAY = 120
            self.REQUEST_TIMEOUT = 60
            self.circuit_breaker_threshold = 5
            self.circuit_breaker_reset_time = 30
        
        # Rate Limit Management  
        self.RATE_LIMIT_DELAY = 90 if fast_mode else 120  # Shorter rate limit wait
        self.last_rate_limit = {}  # Track rate limits per endpoint
        
        # Smart Circuit Breaker (responsive protection without slowdown)
        self.failure_count = {}
        self.half_open_attempts = {}  # Track "testing" requests
        
    def is_circuit_open(self, endpoint_base: str) -> bool:
        """Check if circuit breaker is open for this endpoint with half-open state."""
        if endpoint_base not in self.failure_count:
            return False
            
        failures = self.failure_count[endpoint_base]
        time_since_failure = time.time() - failures['last_failure']
        
        # If under threshold, circuit is closed (normal operation)
        if failures['count'] < self.circuit_breaker_threshold:
            return False
            
        # If enough time passed, enter "half-open" state (test with 1 request)
        if time_since_failure > self.circuit_breaker_reset_time:
            # Check if we're already testing
            if endpoint_base not in self.half_open_attempts:
                self.half_open_attempts[endpoint_base] = time.time()
                print(f"🔄 Circuit breaker HALF-OPEN for {endpoint_base} - testing...")
                return False  # Allow one test request
            
            # If test request was recent (within 10s), still wait
            if time.time() - self.half_open_attempts[endpoint_base] < 10:
                return True
            
            # Allow another test request
            self.half_open_attempts[endpoint_base] = time.time()
            print(f"🔄 Circuit breaker retry test for {endpoint_base}")
            return False
            
        # Circuit is fully open - block requests
        remaining_time = self.circuit_breaker_reset_time - time_since_failure
        print(f"🔴 Circuit breaker OPEN for {endpoint_base} - {remaining_time:.0f}s remaining")
        return True
    
    def record_failure(self, endpoint_base: str, error_type: str = None):
        """Record a failure for circuit breaker (only severe errors trigger circuit breaker)."""
        # Only trigger circuit breaker for severe server errors, not timeouts or rate limits
        severe_errors = ['server_error', 'max_retries_exceeded', 'connection_error']
        if error_type not in severe_errors:
            return  # Don't trigger circuit breaker for minor issues
            
        if endpoint_base not in self.failure_count:
            self.failure_count[endpoint_base] = {'count': 0, 'last_failure': 0}
        
        self.failure_count[endpoint_base]['count'] += 1
        self.failure_count[endpoint_base]['last_failure'] = time.time()
        
        if self.failure_count[endpoint_base]['count'] >= self.circuit_breaker_threshold:
            print(f"🔴 Circuit breaker OPEN for {endpoint_base} - {self.circuit_breaker_threshold} severe failures")
    
    def record_success(self, endpoint_base: str):
        """Record a success to reset failure count and half-open state."""
        if endpoint_base in self.failure_count:
            # Gradual recovery: reduce failure count by 2 on success
            self.failure_count[endpoint_base]['count'] = max(0, self.failure_count[endpoint_base]['count'] - 2)
            
        # Reset half-open state on success
        if endpoint_base in self.half_open_attempts:
            del self.half_open_attempts[endpoint_base]
            print(f"✅ Circuit breaker CLOSED for {endpoint_base} - API recovered")
    
    def calculate_delay(self, attempt: int, base_delay: float = None) -> float:
        """Calculate delay with exponential backoff and jitter."""
        if base_delay is None:
            base_delay = self.BASE_DELAY
            
        # Exponential backoff: 2, 4, 8, 16, 32, ... seconds (capped at MAX_DELAY)
        delay = min(base_delay * (2 ** attempt), self.MAX_DELAY)
        
        # Add jitter (±20%) to prevent thundering herd
        jitter = delay * 0.2 * (random.random() - 0.5)
        final_delay = delay + jitter
        
        return max(1, final_delay)  # Minimum 1 second
    
    def should_retry(self, status_code: int, attempt: int) -> Tuple[bool, str]:
        """Determine if request should be retried based on status code."""
        
        # Don't retry if max attempts reached
        if attempt >= self.MAX_RETRIES:
            return False, f"Max retries ({self.MAX_RETRIES}) exceeded"
        
        # Retry on server errors
        if status_code in [500, 502, 503, 504, 520, 521, 522, 523, 524]:
            return True, f"Server error {status_code} - retrying"
        
        # Retry on rate limiting
        if status_code == 429:
            return True, f"Rate limited - will wait and retry"
        
        # Retry on timeout-like errors
        if status_code in [408, 524]:
            return True, f"Timeout error {status_code} - retrying"
        
        # Don't retry on client errors (except rate limit)
        if 400 <= status_code < 500:
            return False, f"Client error {status_code} - not retrying"
        
        # Retry on other errors
        return True, f"Unexpected error {status_code} - retrying"
    
    def make_reliable_request(self, url: str, username: str, request_type: str = "profile") -> Dict:
        """
        Make a reliable API request with comprehensive error handling.
        
        Args:
            url: API endpoint URL
            username: Creator username (for logging)
            request_type: Type of request (profile/posts) for logging
        
        Returns:
            Dict with 'success', 'data', 'error_type', and 'error_message' keys
        """
        endpoint_base = url.split('?')[0]  # Base URL without parameters
        
        # Check circuit breaker
        if self.is_circuit_open(endpoint_base):
            return {
                'success': False,
                'data': None,
                'error_type': 'circuit_breaker',
                'error_message': f"Circuit breaker open for {endpoint_base} - too many recent failures"
            }
        
        # Check if we're still in rate limit cooldown
        if endpoint_base in self.last_rate_limit:
            time_since_rate_limit = time.time() - self.last_rate_limit[endpoint_base]
            if time_since_rate_limit < self.RATE_LIMIT_DELAY:
                remaining_wait = self.RATE_LIMIT_DELAY - time_since_rate_limit
                print(f"⏳ Still in rate limit cooldown for {remaining_wait:.0f}s...")
                time.sleep(remaining_wait)
        
        print(f"📡 Fetching {request_type} data for @{username}...")
        
        last_error = None
        for attempt in range(self.MAX_RETRIES):
            try:
                # Wait between attempts (except first)
                if attempt > 0:
                    delay = self.calculate_delay(attempt - 1)
                    print(f"   🔄 Retry {attempt + 1}/{self.MAX_RETRIES} for @{username} after {delay:.1f}s...")
                    time.sleep(delay)
                
                # Make the request
                start_time = time.time()
                response = requests.get(url, headers=self.headers, timeout=self.REQUEST_TIMEOUT)
                request_time = time.time() - start_time
                
                # Handle successful response
                if response.status_code == 200:
                    self.record_success(endpoint_base)
                    print(f"✅ API call successful for @{username} ({request_time:.2f}s)")
                    
                    try:
                        data = response.json()
                        return {
                            'success': True,
                            'data': data,
                            'error_type': None,
                            'error_message': None
                        }
                    except json.JSONDecodeError as e:
                        return {
                            'success': False,
                            'data': None,
                            'error_type': 'json_decode',
                            'error_message': f"Failed to decode JSON response: {e}"
                        }
                
                # Handle rate limiting
                elif response.status_code == 429:
                    self.last_rate_limit[endpoint_base] = time.time()
                    print(f"⏳ Rate limited for @{username} - waiting {self.RATE_LIMIT_DELAY}s...")
                    time.sleep(self.RATE_LIMIT_DELAY)
                    continue  # Don't count this as a retry attempt
                
                # Handle other errors
                should_retry, reason = self.should_retry(response.status_code, attempt)
                print(f"❌ Request failed for @{username}: {response.status_code} - {reason}")
                
                if not should_retry:
                    error_type = self.categorize_error(response.status_code)
                    return {
                        'success': False,
                        'data': None,
                        'error_type': error_type,
                        'error_message': f"API error {response.status_code}: {response.text[:200]}"
                    }
                
                last_error = f"HTTP {response.status_code}"
                
            except requests.exceptions.Timeout:
                print(f"⏰ Request timeout for @{username} (attempt {attempt + 1})")
                last_error = "Request timeout"
                if attempt == self.MAX_RETRIES - 1:
                    break
                continue
                
            except requests.exceptions.ConnectionError as e:
                print(f"🌐 Connection error for @{username} (attempt {attempt + 1}): {e}")
                last_error = f"Connection error: {e}"
                if attempt == self.MAX_RETRIES - 1:
                    break
                continue
                
            except requests.exceptions.RequestException as e:
                print(f"❌ Request exception for @{username} (attempt {attempt + 1}): {e}")
                last_error = f"Request exception: {e}"
                if attempt == self.MAX_RETRIES - 1:
                    break
                continue
        
        # All retries failed - this is a severe error worthy of circuit breaker
        error_type = 'max_retries_exceeded'
        self.record_failure(endpoint_base, error_type)
        return {
            'success': False,
            'data': None,
            'error_type': error_type,
            'error_message': f"All {self.MAX_RETRIES} attempts failed. Last error: {last_error}"
        }
    
    def categorize_error(self, status_code: int) -> str:
        """Categorize errors for better reporting."""
        if status_code == 404:
            return 'profile_not_found'
        elif status_code == 403:
            return 'access_denied'
        elif status_code == 429:
            return 'rate_limited'
        elif 500 <= status_code < 600:
            return 'server_error'
        elif 400 <= status_code < 500:
            return 'client_error'
        else:
            return 'unknown_error'

# Global instance
api_manager = None

def get_api_manager(api_key: str, fast_mode: bool = True) -> APIReliabilityManager:
    """Get or create global API manager instance with fast mode enabled by default."""
    global api_manager
    if api_manager is None:
        api_manager = APIReliabilityManager(api_key, fast_mode=fast_mode)
        mode_text = "FAST MODE" if fast_mode else "SAFE MODE"
        print(f"🚀 API Reliability Manager initialized in {mode_text}")
        if fast_mode:
            print(f"   ⚡ Fast settings: {api_manager.MAX_RETRIES} retries, {api_manager.circuit_breaker_reset_time}s circuit breaker")
    return api_manager

def make_instagram_api_call(username: str, api_key: str, call_type: str = "profile") -> Dict:
    """Make reliable Instagram API call."""
    manager = get_api_manager(api_key)
    
    if call_type == "profile":
        url = f"https://api.scrapecreators.com/v1/instagram/profile?handle={username}"
        return manager.make_reliable_request(url, username, "Instagram profile")
    elif call_type == "posts":
        url = f"https://api.scrapecreators.com/v2/instagram/user/posts?handle={username}"
        return manager.make_reliable_request(url, username, "Instagram posts")
    else:
        return {
            'success': False,
            'data': None,
            'error_type': 'invalid_call_type',
            'error_message': f"Unknown call type: {call_type}"
        }

def make_tiktok_api_call(username: str, api_key: str) -> Dict:
    """Make reliable TikTok API call."""
    manager = get_api_manager(api_key)
    url = f"https://api.scrapecreators.com/v3/tiktok/profile/videos?handle={username}"
    return manager.make_reliable_request(url, username, "TikTok profile+posts")

# Error reporting utilities
def format_error_summary(results: Dict) -> str:
    """Format a summary of errors for job reporting."""
    if 'failed' not in results:
        return "No errors"
    
    error_summary = {}
    for failed_item in results['failed']:
        # Extract error type from error message
        if ' - ' in failed_item:
            error_part = failed_item.split(' - ', 1)[1]
            if 'rate_limited' in error_part.lower():
                error_type = 'Rate Limited'
            elif 'timeout' in error_part.lower():
                error_type = 'Timeout'
            elif 'server_error' in error_part.lower():
                error_type = 'Server Error'
            elif 'profile_not_found' in error_part.lower():
                error_type = 'Profile Not Found'
            elif 'access_denied' in error_part.lower():
                error_type = 'Access Denied'
            elif 'circuit_breaker' in error_part.lower():
                error_type = 'Circuit Breaker'
            else:
                error_type = 'Other API Error'
        else:
            error_type = 'Unknown Error'
        
        error_summary[error_type] = error_summary.get(error_type, 0) + 1
    
    summary_parts = [f"{error_type}: {count}" for error_type, count in error_summary.items()]
    return " | ".join(summary_parts)

def get_retry_recommendations(error_type: str) -> str:
    """Get recommendations for handling specific error types."""
    recommendations = {
        'rate_limited': 'Wait 2+ hours before retrying, or reduce concurrent job count',
        'timeout': 'Network issues - retry in 30+ minutes',
        'server_error': 'ScrapeCreators server issues - retry in 1+ hours',
        'profile_not_found': 'Creator may have deleted account - remove from database',
        'access_denied': 'Creator went private - retry later or remove',
        'circuit_breaker': 'Too many API failures - wait 5+ minutes before new jobs',
        'max_retries_exceeded': 'Persistent API issues - check API status or wait longer'
    }
    return recommendations.get(error_type, 'Unknown error type - investigate manually')
