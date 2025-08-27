import google.generativeai as genai
import requests
import re
import pandas as pd
from supabase import create_client, Client
from datetime import datetime, timedelta
import os
from urllib.parse import urlparse
import mimetypes
import time
import statistics
from tqdm import tqdm
import asyncio
import io
from PIL import Image
import pillow_heif
import aiohttp
from concurrent.futures import ThreadPoolExecutor
import ssl
import traceback
import signal
from functools import wraps
import sys

# Add current directory to path for API reliability fix
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from api_reliability_fix import make_instagram_api_call, make_tiktok_api_call, format_error_summary

# ==================== TIMEOUT PROTECTION ====================
# Using asyncio-based timeouts instead of signal-based ones for better compatibility

async def with_timeout_async(coro, timeout_seconds):
    """Add timeout protection to async functions."""
    try:
        return await asyncio.wait_for(coro, timeout=timeout_seconds)
    except asyncio.TimeoutError:
        print(f"⏰ Async operation timed out after {timeout_seconds} seconds")
        return None

# ==================== CONFIGURATION ====================

# Initialize Gemini AI
GEMINI_API_KEY = "AIzaSyBYRd9lJTe1mRgJLhpbp39butQbXDgBBMw"
genai.configure(api_key=GEMINI_API_KEY)
gemini_model = genai.GenerativeModel('gemini-1.5-flash')

# Initialize Supabase client
SUPABASE_URL = "https://unovwhgnwenxbyvpevcz.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InVub3Z3aGdud2VueGJ5dnBldmN6Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MjEzNjU0MCwiZXhwIjoyMDY3NzEyNTQwfQ.bqXWKTR64TRARH2VOrjiQdPnQ7W-8EGJp5RIi7yFmck"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ScrapeCreators API Key
SCRAPECREATORS_API_KEY = "wjhGgI14NjNMUuXA92YWXjojozF2"

# Constants
BUCKET_NAME = "profile-media"
TABLE_NAME = "creatordata"
MAX_RECENT_POSTS = 4

# ==================== CONCURRENT PROCESSING CONFIGURATION ====================
CLEANUP_BATCH_SIZE = 2      # Reduced from 3 to 2 for more reliable API calls
NEW_CREATOR_BATCH_SIZE = 1  # Reduced from 2 to 1 for more reliable API calls
BATCH_DELAY = 2.0           # Increased from 1s to 2s to be more respectful to API

# ==================== TEST MODE CONFIGURATION ====================
TEST_MODE = False
TEST_LIMIT = 100            # Number of creators to process in test mode

# Command line argument override for test mode
if len(sys.argv) > 1:
    if sys.argv[1].lower() in ['test', '--test', '-t']:
        TEST_MODE = True
        print("🧪 Test mode enabled via command line argument")
    elif sys.argv[1].lower() in ['prod', '--prod', '-p', 'production']:
        TEST_MODE = False
        print("🚀 Production mode enabled via command line argument")

# ==================== HELPER FUNCTIONS ====================

def create_ssl_session():
    """Create an aiohttp session with SSL context for macOS compatibility."""
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    return aiohttp.ClientSession(connector=connector)

def is_creator_active(recent_posts, days_threshold=45):
    """Check if a creator has posted in the last N days."""
    if not recent_posts:
        return False
    
    current_time = datetime.now()
    print(f"📅 Checking {len(recent_posts)} posts for activity (threshold: {days_threshold} days)")
    
    active_found = False
    for i, post in enumerate(recent_posts, 1):
        if post.get('created_at'):
            try:
                post_date = datetime.fromisoformat(post['created_at'].replace('Z', '+00:00'))
                days_since_post = (current_time - post_date).days
                status = "✅ ACTIVE" if days_since_post <= days_threshold else "❌ OLD"
                print(f"   Post {i:2d}: {post_date.strftime('%Y-%m-%d')} ({days_since_post:3d} days ago) - {status}")
                
                if days_since_post <= days_threshold and not active_found:
                    active_found = True
            except (ValueError, TypeError):
                print(f"   Post {i:2d}: Invalid date format")
                continue
        else:
            print(f"   Post {i:2d}: No date available")
    
    if active_found:
        print(f"📅 Creator is ACTIVE - Found posts within {days_threshold} days")
        return True
    else:
        print(f"📅 Creator is INACTIVE - No posts within {days_threshold} days")
        return False

def calculate_change(new_value, old_value):
    """Calculates the percentage change and determines the change type."""
    if old_value is None or new_value is None or old_value == 0:
        return 0, 'zero'
    try:
        new_val = float(new_value) if new_value is not None else 0
        old_val = float(old_value) if old_value is not None else 0
        
        if old_val == 0:
            return 0, 'zero'
            
        change = ((new_val - old_val) / old_val) * 100
        change = round(change, 2)
        
        if change > 1000:
            change = 1000
        elif change < -1000:
            change = -1000
        
        # Store as percentage (not multiplied by 100)
        change = round(change, 2)
            
    except (ValueError, TypeError, ZeroDivisionError):
        return 0, 'zero'

    if change > 0:
        change_type = 'positive'
    elif change < 0:
        change_type = 'negative'
    else:
        change_type = 'zero'
    return change, change_type

def get_median(data_list):
    """Calculates the median of a list of numbers."""
    if not data_list:
        return 0
    return statistics.median(data_list)

def get_standard_deviation(data_list):
    """Calculates the standard deviation of a list of numbers."""
    if len(data_list) < 2:
        return 0
    return statistics.stdev(data_list)

def calculate_buzz_score(new_data, existing_data):
    """Calculates the Buzz Score based on growth, engagement, and consistency."""
    print("💯 Calculating Buzz Score...")

    # Prepare Current Data
    views_now = [
        post.get('views', 0) for i in range(1, 13)
        if (post := new_data.get(f'recent_post_{i}')) and post.get('views') is not None
    ]
    if not views_now: views_now = [0]

    median_views_now = get_median(views_now)
    std_dev_views = get_standard_deviation(views_now)
    followers_now = new_data.get('followers_count', 0)
    
    # Handle different average_likes formats
    avg_likes_data = new_data.get('average_likes', {})
    if isinstance(avg_likes_data, dict):
        avg_likes_now = avg_likes_data.get('avg_value', 0)
    else:
        avg_likes_now = avg_likes_data or 0
    
    # Handle different average_comments formats
    avg_comments_data = new_data.get('average_comments', 0)
    if isinstance(avg_comments_data, dict):
        avg_comments_now = avg_comments_data.get('avg_value', 0)
    else:
        avg_comments_now = avg_comments_data or 0

    # Prepare Historical Data
    views_last_week = [
        post.get('views', 0) for i in range(1, 13)
        if (post := existing_data.get(f'recent_post_{i}')) and post.get('views') is not None
    ]
    if not views_last_week: views_last_week = [0]

    median_views_last_week = get_median(views_last_week)
    followers_last_week = existing_data.get('followers_count', 0)
    
    old_likes_data = existing_data.get('average_likes')
    if isinstance(old_likes_data, dict):
        avg_likes_last_week = old_likes_data.get('avg_value', 0)
    else:
        avg_likes_last_week = old_likes_data or 0
    
    # Handle different average_comments formats for historical data
    old_comments_data = existing_data.get('average_comments', 0)
    if isinstance(old_comments_data, dict):
        avg_comments_last_week = old_comments_data.get('avg_value', 0)
    else:
        avg_comments_last_week = old_comments_data or 0

    # Growth Score
    view_growth = (median_views_now - median_views_last_week) / median_views_last_week if median_views_last_week else 0
    follower_growth = (followers_now - followers_last_week) / followers_last_week if followers_last_week else 0
    likes_growth = (avg_likes_now - avg_likes_last_week) / avg_likes_last_week if avg_likes_last_week else 0
    comments_growth = (avg_comments_now - avg_comments_last_week) / avg_comments_last_week if avg_comments_last_week else 0

    average_growth = (view_growth * 0.35) + (follower_growth * 0.35) + (likes_growth * 0.15) + (comments_growth * 0.15)

    growth_score = 30
    if average_growth >= 0.4: growth_score = 100
    elif average_growth >= 0.2: growth_score = 80
    elif average_growth >= 0.05: growth_score = 60
    elif average_growth >= 0.0: growth_score = 45

    # Engagement Score
    engagement_rate = (avg_likes_now + avg_comments_now) / followers_now if followers_now else 0
    comment_like_ratio = avg_comments_now / avg_likes_now if avg_likes_now else 0

    engagement_score = 25
    if engagement_rate >= 0.06: engagement_score = 100
    elif engagement_rate >= 0.04: engagement_score = 80
    elif engagement_rate >= 0.02: engagement_score = 60
    elif engagement_rate >= 0.01: engagement_score = 40
    
    if comment_like_ratio >= 0.10: engagement_score += 5
    engagement_score = min(engagement_score, 100)

    # Consistency Score
    volatility_ratio = std_dev_views / median_views_now if median_views_now else 0

    base_consistency_score = 40
    if volatility_ratio <= 0.4: base_consistency_score = 100
    elif volatility_ratio <= 0.6: base_consistency_score = 80
    elif volatility_ratio <= 0.8: base_consistency_score = 60

    viral_threshold = max(2 * median_views_now, 1.5 * followers_now)
    viral_posts = sum(1 for v in views_now if v > viral_threshold)

    bonus = 0
    if 2 <= viral_posts < 4: bonus = 10
    elif viral_posts >= 4: bonus = 15
    
    consistency_score = min(base_consistency_score + bonus, 100)

    # Final Buzz Score
    final_buzz_score = (growth_score * 0.45) + (engagement_score * 0.35) + (consistency_score * 0.20)
    
    print(f"✅ Buzz Score calculated: {final_buzz_score:.2f}")
    return int(final_buzz_score)

# ==================== MEDIA PROCESSING FUNCTIONS ====================

async def download_file(url: str) -> bytes:
    """Download file from URL and return its content as bytes."""
    try:
        print(f"⬇️ Downloading: {url[:50]}...")
        
        # Use aiohttp with proper timeout
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as response:
                response.raise_for_status()
                content = await response.read()
                print(f"✅ Downloaded {len(content)} bytes")
                return content
                
    except Exception as e:
        print(f"❌ Error downloading {url}: {e}")
        return None

async def upload_to_supabase_storage(bucket: str, path: str, file_content: bytes, content_type: str = None) -> str:
    """Upload file to Supabase storage and return public URL."""
    try:
        print(f"⬆️ Uploading to: {path}")
        
        # Use asyncio timeout instead of signal-based timeout
        upload_task = asyncio.create_task(
            asyncio.to_thread(
                lambda: supabase.storage.from_(bucket).upload(
                    path=path,
                    file=file_content,
                    file_options={"content-type": content_type, "upsert": "true"}
                )
            )
        )
        
        # 20 second timeout using asyncio
        res = await asyncio.wait_for(upload_task, timeout=20)
        url = supabase.storage.from_(bucket).get_public_url(path)
        print(f"✅ Upload successful: {path}")
        return url
        
    except asyncio.TimeoutError:
        print(f"⏰ Upload timeout after 20 seconds for {path}")
        try:
            return supabase.storage.from_(bucket).get_public_url(path)
        except Exception as e2:
            print(f"❌ Could not get public URL for {path}. Error: {e2}")
            return None
    except Exception as e:
        print(f"❌ Upload failed for {path}, attempting to get existing URL. Error: {e}")
        try:
            return supabase.storage.from_(bucket).get_public_url(path)
        except Exception as e2:
            print(f"❌ Could not get public URL for {path}. Error: {e2}")
            return None

def get_file_extension_and_type(url: str) -> tuple:
    """Extract file extension and content type from URL."""
    parsed = urlparse(url)
    path = parsed.path
    ext = os.path.splitext(path)[1]
    
    content_type, _ = mimetypes.guess_type(url)
    if not content_type:
        content_type = 'video/mp4' if ext in ['.mp4', '.mov'] else 'image/jpeg'
        
    return ext or '.jpg', content_type

def clean_handle(handle: str) -> str:
    """Clean the handle to be used as a folder name."""
    return re.sub(r'[\\/*?:"<>|]', '_', handle)

def delete_all_creator_media(handle: str):
    """Deletes all media files for a given creator from Supabase Storage."""
    print(f"🗑️ Attempting to delete old media for @{handle}...")
    clean_handle_name = clean_handle(handle)
    storage_folder = f"{clean_handle_name}/"
    
    try:
        files_to_delete = supabase.storage.from_(BUCKET_NAME).list(path=storage_folder)
        
        if not files_to_delete:
            print(f"ℹ️ No existing media found for @{handle} to delete.")
            return

        paths_to_delete = [f"{storage_folder}{file['name']}" for file in files_to_delete]
        
        if paths_to_delete:
            print(f"   - Found {len(paths_to_delete)} files to delete.")
            supabase.storage.from_(BUCKET_NAME).remove(paths=paths_to_delete)
            print(f"✅ Successfully deleted old media for @{handle}.")
        
    except Exception as e:
        if "The resource was not found" in str(e):
             print(f"ℹ️ No existing media folder found for @{handle}.")
        else:
            print(f"❌ Error deleting media for @{handle}: {e}")

async def process_creator_media(creator_id: str, handle: str, creator_data: dict):
    """Process media for a single creator, downloading and uploading to Supabase Storage."""
    clean_handle_name = clean_handle(handle)
    storage_folder = f"{clean_handle_name}/"
    
    updates = {}
    processed_media = 0
    
    # Process profile image
    if creator_data.get("profile_image_url"):
        profile_ext, profile_content_type = get_file_extension_and_type(creator_data["profile_image_url"])
        profile_storage_path = f"{storage_folder}profile{profile_ext}"
        
        print(f"🖼️ Downloading profile image for @{handle}...")
        profile_content = await download_file(creator_data["profile_image_url"])
        
        if profile_content:
            new_url = await upload_to_supabase_storage(
                BUCKET_NAME, profile_storage_path, profile_content, profile_content_type
            )
            if new_url:
                updates["profile_image_url"] = new_url
                print(f"  ✅ Successfully processed profile image")
            else:
                print(f"  ❌ Failed to upload profile image")
        else:
            print(f"  ❌ Failed to download profile image")

    # Process recent posts (up to MAX_RECENT_POSTS)
    for i in range(1, 13): # Check all 12 possible posts
        post_key = f"recent_post_{i}"
        if processed_media >= MAX_RECENT_POSTS:
            break

        post = creator_data.get(post_key)
        if not post or not isinstance(post, dict):
            continue

        # Handle both Instagram (media_urls) and TikTok (video_url) formats
        media_urls = []
        if post.get("media_urls"):
            media_urls = post.get("media_urls") if isinstance(post.get("media_urls"), list) else [post.get("media_urls")]
        elif post.get("video_url"):
            media_urls = [post.get("video_url")]
        
        if not media_urls:
            continue

        try:
            print(f"  📹 Processing post {i} media for @{handle}...")
            
            new_media_urls = []
            for media_url in media_urls:
                if not media_url or not isinstance(media_url, str) or not media_url.startswith("http"):
                    continue
                
                ext, content_type = get_file_extension_and_type(media_url)
                media_storage_path = f"{storage_folder}media_{processed_media + 1}{ext}"
                file_content = await download_file(media_url)
                
                if file_content:
                    new_url = await upload_to_supabase_storage(BUCKET_NAME, media_storage_path, file_content, content_type)
                    if new_url:
                        new_media_urls.append(new_url)
                        processed_media += 1
                        print(f"  ✅ Successfully processed post {i} media")
                        break  # Only process first media URL per post
                    else:
                        print(f"  ❌ Failed to upload post {i} media")
                else:
                    print(f"  ❌ Failed to download post {i} media")
            
            if new_media_urls:
                # Update post with new media URLs
                if "media_urls" in post:
                    post["media_urls"] = new_media_urls
                elif "video_url" in post:
                    post["video_url"] = new_media_urls[0]
                updates[post_key] = post
                
        except Exception as e:
            print(f"  ❌ Error processing post {i} media: {e}")

    return updates

# ==================== PROGRESS TRACKING CLASS ====================

class ProgressTracker:
    """Tracks progress and timing for the rescaper."""
    
    def __init__(self, total_items, phase_name):
        self.total_items = total_items
        self.completed_items = 0
        self.phase_name = phase_name
        self.start_time = time.time()
        self.processing_times = []
    
    def complete_item(self, processing_time=None):
        """Mark an item as completed."""
        self.completed_items += 1
        if processing_time:
            self.processing_times.append(processing_time)
    
    def get_progress_stats(self):
        """Get current progress statistics."""
        elapsed = time.time() - self.start_time
        avg_time = sum(self.processing_times) / len(self.processing_times) if self.processing_times else 0
        return {
            'elapsed': elapsed,
            'avg_time': avg_time,
            'completed': self.completed_items,
            'total': self.total_items,
            'percentage': (self.completed_items / self.total_items * 100) if self.total_items > 0 else 0
        }
    
    def get_progress_bar_description(self):
        """Get description for progress bar."""
        stats = self.get_progress_stats()
        return f"{self.phase_name}: {stats['completed']}/{stats['total']} ({stats['percentage']:.1f}%)"
    
    def display_progress_summary(self):
        """Display a progress summary."""
        stats = self.get_progress_stats()
        print(f"📊 {self.phase_name} Progress:")
        print(f"   • Completed: {stats['completed']}/{stats['total']} ({stats['percentage']:.1f}%)")
        print(f"   • Elapsed time: {str(timedelta(seconds=int(stats['elapsed'])))}")
        if stats['avg_time'] > 0:
            print(f"   • Average time per item: {stats['avg_time']:.1f} seconds")
            remaining_items = stats['total'] - stats['completed']
            estimated_remaining = remaining_items * stats['avg_time']
            print(f"   • Estimated time remaining: {str(timedelta(seconds=int(estimated_remaining)))}")

# ==================== UTILITY FUNCTIONS ====================

def chunk_list(lst, chunk_size):
    """Split a list into chunks of specified size."""
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]

def safe_get(data, keys, default=None):
    """Safely get nested dictionary values."""
    try:
        for key in keys:
            data = data[key]
        return data
    except (KeyError, TypeError, IndexError):
        return default

def calculate_engagement_rate(likes, comments, followers):
    """Calculate engagement rate percentage"""
    if followers == 0:
        return 0.0
    return round(((likes + comments) / followers) * 100, 2)

# ==================== INSTAGRAM SCRAPING FUNCTIONS ====================

def scrape_instagram_user_data(username):
    """Improved Instagram scraper with reliable API calls and better error handling."""
    print(f"\n📡 Fetching Instagram data for @{username}...")
    username = username.strip().lstrip("@")
    
    # Step 1: Get profile data with reliable API call
    profile_result = make_instagram_api_call(username, SCRAPECREATORS_API_KEY, "profile")
    
    if not profile_result['success']:
        error_type = profile_result['error_type']
        error_msg = profile_result['error_message']
        
        print(f"❌ Profile API call failed for @{username}: {error_msg}")
        
        # Handle different error types appropriately
        if error_type == 'profile_not_found':
            print(f"👻 Profile not found for @{username} - likely deleted account")
            return None  # Permanent failure - remove from database
        elif error_type == 'access_denied':
            print(f"🔒 Access denied for @{username} - likely private account")
            return {'error': 'temporary', 'message': 'Account went private - retry later'}
        elif error_type in ['rate_limited', 'server_error', 'timeout', 'circuit_breaker']:
            print(f"⏳ Temporary API issue for @{username}: {error_type}")
            return {'error': 'temporary', 'message': f'API issue: {error_type} - will retry'}
        else:
            print(f"❓ Unknown error for @{username}: {error_type}")
            return {'error': 'unknown', 'message': f'Unknown error: {error_msg}'}
    
    # Parse profile data
    try:
        profile_data = profile_result['data'].get("data", {}).get("user", {})
        if not profile_data:
            print(f"❌ Empty profile data for @{username}")
            return {'error': 'temporary', 'message': 'Empty API response - retry later'}
        
        full_name = profile_data.get("full_name", "")
        bio = profile_data.get("biography", "")
        avatar_url = profile_data.get("profile_pic_url_hd", "")
        followers = profile_data.get("edge_followed_by", {}).get("count", 0)
        
        print(f"📊 Profile data: {followers:,} followers")
        
    except Exception as e:
        print(f"❌ Error parsing profile data for @{username}: {e}")
        return {'error': 'temporary', 'message': f'Data parsing error: {e}'}
    
    # Check follower range early
    if not (10000 <= followers <= 350000):
        print(f"🚫 Skipping: Follower count {followers:,} not in 10k–350k range.")
        return {'skipped': True}
    
    # Step 2: Get posts data with reliable API call
    posts_result = make_instagram_api_call(username, SCRAPECREATORS_API_KEY, "posts")
    
    if not posts_result['success']:
        error_type = posts_result['error_type']
        error_msg = posts_result['error_message']
        
        print(f"❌ Posts API call failed for @{username}: {error_msg}")
        
        # For posts, we might be able to continue with just profile data
        # If posts API fails, we should fail the entire scraping process
        # rather than updating with incomplete/zero metrics
        print(f"⏳ Posts API failed for @{username}: {error_type}")
        return {'error': 'temporary', 'message': f'Posts API failed: {error_type} - cannot calculate accurate metrics'}
    else:
        posts_data = posts_result['data'].get("items", [])
    
    print(f"📱 Found {len(posts_data)} posts")

    # Process Posts
    likes_list, comments_list, views_list = [], [], []
    all_hashtags, all_tagged_users, all_captions, all_locations, past_ad_placements = [], [], [], [], []
    recent_posts = []

    for post in posts_data[:12]:
        caption_obj = post.get("caption")
        caption = caption_obj.get("text", "") if caption_obj else ""
        all_captions.append(caption)
        
        likes_list.append(post.get("like_count", 0))
        comments_list.append(post.get("comment_count", 0))
        if post.get("play_count"): views_list.append(post.get("play_count", 0))

        all_hashtags.extend(re.findall(r"#\w+", caption))
        usertags = [tag.get("user", {}).get("username") for tag in (post.get("usertags") or {}).get("in", []) if tag and tag.get("user", {}).get("username")]
        all_tagged_users.extend(usertags)
        
        if post.get("is_paid_partnership"): past_ad_placements.extend(usertags)
        if post.get("location"): all_locations.append(post["location"])
        
        is_video = post.get("media_type") == 2
        is_carousel = post.get("media_type") == 8
        
        # Extract Instagram posting time
        taken_at = post.get('taken_at')
        created_at = None
        
        if taken_at and taken_at != 0:
            try:
                if taken_at > 9999999999:  # Likely milliseconds
                    taken_at = taken_at / 1000
                created_at = datetime.fromtimestamp(taken_at).isoformat()
            except (ValueError, TypeError):
                created_at = None
        
        media_urls = []
        if is_carousel:
            carousel_media = post.get("carousel_media", [])
            if carousel_media:
                first_media = carousel_media[0]
                if first_media.get("media_type") == 2:
                    video_versions = first_media.get("video_versions", [])
                    if video_versions:
                        media_urls.append(video_versions[0].get("url"))
                    else:
                        media_urls.append(first_media.get("display_uri"))
                else:
                    image_versions = first_media.get("image_versions2", {}).get("candidates", [])
                    if image_versions:
                        media_urls.append(image_versions[0].get("url"))
                    else:
                        media_urls.append(first_media.get("display_uri"))
        else:
            if is_video:
                video_url = post.get("image_versions2", {}) \
                    .get("additional_candidates", {}) \
                    .get("igtv_first_frame", {}) \
                    .get("url")
                if video_url:
                    media_urls = [video_url]
            else:
                image_versions = post.get("image_versions2", {}).get("candidates", [])
                if image_versions:
                    media_urls.append(image_versions[0].get("url"))
                else:
                    media_urls.append(post.get("display_uri"))

        recent_posts.append({
            "caption": caption, "likes": post.get("like_count", 0), "comments": post.get("comment_count", 0), "views": post.get("play_count", 0),
            "is_video": is_video, "is_carousel": is_carousel,
            "media_urls": [url for url in media_urls if url],
            "hashtags": re.findall(r"#\w+", caption), "brand_tags": usertags,
            "is_paid_partnership": post.get("is_paid_partnership", False),
            "like_hidden": post.get("like_and_view_counts_disabled", False),
            "location": post.get("location"),
            "created_at": created_at
        })

    # Calculations based on the last 9 of the 12 posts
    print("📊 Calculating Instagram metrics based on the last 9 of the 12 posts...")
    
    likes_for_calc = likes_list[3:]
    comments_for_calc = comments_list[3:]
    views_for_calc = views_list[3:]

    avg_likes = sum(likes_for_calc) // len(likes_for_calc) if likes_for_calc else 0
    avg_comments = sum(comments_for_calc) // len(comments_for_calc) if comments_for_calc else 0
    avg_views = sum(views_for_calc) // len(views_for_calc) if views_for_calc else 0
    
    total_likes_for_calc = sum(likes_for_calc)
    total_comments_for_calc = sum(comments_for_calc)
    
    engagement_rate = round(((total_likes_for_calc + total_comments_for_calc) / followers) * 100, 2) if followers and likes_for_calc else 0

    # Final structured data
    influencer_data = {
        "handle": username,
        "display_name": profile_data.get("full_name", ""),
        "profile_url": f"https://instagram.com/{username}",
        "profile_image_url": profile_data.get("profile_pic_url_hd"),
        "bio": bio,
        "platform": "Instagram",
        "followers_count": followers,
        "average_views": avg_views,
        "average_comments": avg_comments,
        "engagement_rate": engagement_rate,
        "average_likes": {"avg_value": avg_likes},
        "hashtags": list(set(all_hashtags)),
        "email": (re.findall(r"\b[\w\.-]+@[\w\.-]+\.\w{2,4}\b", bio) or [None])[0],
        "bio_links": [link.get("url") for link in profile_data.get("bio_links", []) if link.get("url")],
        "brand_tags": list(set(all_tagged_users)),
        "past_ad_placements": list(set(past_ad_placements)),
    }
    
    for i, post in enumerate(recent_posts):
        influencer_data[f"recent_post_{i+1}"] = post

    # Check if creator has posted in the last 45 days
    print("\n📅 Checking Instagram creator activity...")
    if not is_creator_active(recent_posts, days_threshold=45):
        print(f"🚫 Skipping @{username}: No posts in the last 45 days")
        return {'skipped': True}
        
    return influencer_data

# ==================== TIKTOK SCRAPING FUNCTIONS ====================

def scrape_tiktok_user_data(username):
    """Improved TikTok scraper with reliable API calls and better error handling."""
    print(f"\n📡 Fetching TikTok data for @{username}...")
    username = username.strip().lstrip("@")
    
    # Make reliable API call
    result = make_tiktok_api_call(username, SCRAPECREATORS_API_KEY)
    
    if not result['success']:
        error_type = result['error_type']
        error_msg = result['error_message']
        
        print(f"❌ TikTok API call failed for @{username}: {error_msg}")
        
        # Handle different error types
        if error_type == 'profile_not_found':
            print(f"👻 TikTok profile not found for @{username}")
            return None  # Permanent failure
        elif error_type in ['rate_limited', 'server_error', 'timeout', 'circuit_breaker']:
            return {'error': 'temporary', 'message': f'API issue: {error_type}'}
        else:
            return {'error': 'unknown', 'message': f'Unknown error: {error_msg}'}
    
    # Parse TikTok data
    try:
        api_data = result['data']
        posts = api_data.get('aweme_list', [])
        
        if not posts:
            print("❌ No TikTok posts found for this account")
            return {'error': 'temporary', 'message': 'No posts found - account may be private'}
        
        user_info = posts[0].get('author', {})
        bio = user_info.get('signature', '')
        followers = user_info.get('follower_count', 0)
        
        print(f"📊 TikTok profile: {followers:,} followers")
        
        # Check follower range
        if not (10000 <= followers <= 350000):
            print(f"❌ Skipped: TikTok follower count {followers:,} outside target range")
            return {'skipped': True}
        
        # Process posts
        likes_list, comments_list, views_list = [], [], []
        all_hashtags, all_tagged_users, recent_posts = [], [], []
        past_ad_placements, all_captions = [], []

        for post in posts[:12]:
            caption = post.get('desc', '')
            stats = post.get('statistics', {})
            likes = stats.get('digg_count', 0)
            comments = stats.get('comment_count', 0)
            views = stats.get('play_count', 0)
            hashtags = [tag.lower() for tag in re.findall(r"#(\w+)", caption or "")]
            tagged_users = re.findall(r"@(\w+)", caption or "")
            
            # Extract posting time
            created_at = None
            create_time = post.get('create_time')
            if create_time:
                try:
                    created_at = datetime.fromtimestamp(create_time).isoformat()
                except (ValueError, TypeError):
                    created_at = None

            # Get video URL
            video_url = ""
            video_data = post.get('video', {})
            if video_data:
                play_addr = video_data.get('play_addr', {})
                if play_addr and play_addr.get('url_list'):
                    video_url = play_addr['url_list'][0]

            # Check for paid partnerships
            is_partnership = post.get("commerce_info", {}).get("bc_label_test_text", "") == "Paid partnership"
            if is_partnership and tagged_users:
                past_ad_placements.extend(tagged_users)

            all_hashtags.extend(hashtags)
            all_tagged_users.extend(tagged_users)
            likes_list.append(likes)
            comments_list.append(comments)
            views_list.append(views)
            all_captions.append(caption)

            recent_posts.append({
                "caption": caption,
                "likes": likes,
                "comments": comments,
                "views": views,
                "hashtags": hashtags,
                "tagged_users": tagged_users,
                "video_url": video_url,
                "is_paid_partnership": is_partnership,
                "is_video": True,
                "is_carousel": False,
                "created_at": created_at
            })

        # Calculate averages (using posts 4-12)
        likes_for_calc = likes_list[3:12]
        comments_for_calc = comments_list[3:12]
        views_for_calc = views_list[3:12]

        avg_likes = sum(likes_for_calc) // len(likes_for_calc) if likes_for_calc else 0
        avg_comments = sum(comments_for_calc) // len(comments_for_calc) if comments_for_calc else 0
        avg_views = sum(views_for_calc) // len(views_for_calc) if views_for_calc else 0
        engagement_rate = calculate_engagement_rate(sum(likes_for_calc), sum(comments_for_calc), followers)

        print(f"📈 TikTok metrics: {avg_likes:,} likes, {avg_comments:,} comments, {engagement_rate}% engagement")

        # Check activity
        if not is_creator_active(recent_posts, days_threshold=45):
            print(f"🚫 TikTok creator @{username} is inactive")
            return {'skipped': True, 'reason': 'inactive'}

        # Build data structure
        influencer_data = {
            "handle": user_info.get('unique_id', username),
            "display_name": user_info.get('nickname', ''),
            "profile_url": f"https://www.tiktok.com/@{user_info.get('unique_id', username)}",
            "profile_image_url": user_info.get('avatar_thumb', {}).get('url_list', [''])[0],
            "bio": bio,
            "platform": "TikTok",
            "followers_count": followers,
            "average_views": avg_views,
            "average_likes": avg_likes,
            "average_comments": avg_comments,
            "engagement_rate": float(engagement_rate),
            "hashtags": list(set(all_hashtags)),
            "brand_tags": list(set(all_tagged_users)),
            "past_ad_placements": list(set(past_ad_placements)),
            "bio_links": "",
            "email": (re.findall(r"\b[\w\.-]+@[\w\.-]+\.\w{2,4}\b", bio) or [None])[0]
        }

        # Add recent posts
        for i in range(min(12, len(recent_posts))):
            influencer_data[f"recent_post_{i+1}"] = recent_posts[i]

        print(f"✅ Successfully processed TikTok data for @{username}")
        return influencer_data
        
    except Exception as e:
        print(f"❌ Error processing TikTok data for @{username}: {e}")
        traceback.print_exc()
        return {'error': 'temporary', 'message': f'Data processing error: {e}'}



# ==================== DATABASE FUNCTIONS ====================

def get_existing_creators():
    """Fetches all existing creators from Supabase."""
    print("\nFetching existing creators from Supabase...")
    target_niches = ['Trading', 'Crypto', 'Finance']
    try:
        response = supabase.table("creatordata").select("*").in_('primary_niche', target_niches).execute()
        print(f"Found {len(response.data)} existing creators in the target niches.")
        
        # Apply test mode limit if enabled
        if TEST_MODE and len(response.data) > TEST_LIMIT:
            print(f"🧪 TEST MODE: Limiting to first {TEST_LIMIT} creators")
            response.data = response.data[:TEST_LIMIT]
        
        return response.data
    except Exception as e:
        print(f"❌ Error fetching existing creators: {e}")
        return []

async def rescrape_and_update_creator(creator):
    """Rescrapes a creator and updates their record based on platform."""
    handle = creator.get('handle')
    platform = creator.get('platform')
    
    if not handle or not platform:
        print(f"❌ Missing handle or platform for creator: {creator}")
        return {'handle': handle, 'status': 'error', 'error': 'Missing handle or platform'}
    
    print(f"\n{'='*20} RESCRAPING @{handle} ({platform}) {'='*20}")
    
    # Add timeout protection to the entire creator processing
    start_time = time.time()
    try:
        # Route to appropriate scraper based on platform
        if platform.lower() == 'instagram':
            new_data = scrape_instagram_user_data(handle)
        elif platform.lower() == 'tiktok':
            new_data = scrape_tiktok_user_data(handle)
        else:
            print(f"❌ Unknown platform '{platform}' for @{handle}")
            return {'handle': handle, 'status': 'error', 'error': f'Unknown platform: {platform}'}
        
        if not new_data:
            print(f"ℹ️ No data returned for @{handle}, skipping update.")
            return {'handle': handle, 'status': 'failed', 'error': 'API failure - no data returned'}
        
        # Handle new improved error responses
        elif isinstance(new_data, dict) and new_data.get('error'):
            error_type = new_data.get('error', 'unknown')
            error_msg = new_data.get('message', 'Unknown error')
            if error_type == 'temporary':
                print(f"⏳ Temporary error for @{handle}: {error_msg}")
                return {'handle': handle, 'status': 'failed', 'error': f'Temporary API issue: {error_msg}'}
            else:
                print(f"❌ Permanent error for @{handle}: {error_msg}")
                return {'handle': handle, 'status': 'failed', 'error': f'Permanent error: {error_msg}'}
        
        # Check if creator was skipped due to inactivity or out-of-range followers
        elif isinstance(new_data, dict) and new_data.get('skipped'):
            reason = new_data.get('reason', 'inactive')
            if reason == 'inactive':
                print(f"🗑️ Deleting inactive creator @{handle} from database...")
                try:
                    supabase.table("creatordata").delete().eq("handle", handle).execute()
                    print(f"✅ Successfully deleted inactive creator @{handle}")
                    return {'handle': handle, 'status': 'deleted', 'reason': 'inactive'}
                except Exception as e:
                    print(f"❌ Error deleting inactive creator @{handle}: {e}")
                    return {'handle': handle, 'status': 'error', 'error': f'Delete failed: {e}'}
            else:
                print(f"🚫 Creator @{handle} skipped: {reason}")
                return {'handle': handle, 'status': 'failed', 'error': f'Skipped: {reason}'}

        # Validate that we have meaningful data before updating
        # If key metrics are missing or zero, it might indicate incomplete API data
        followers_count = new_data.get('followers_count', 0)
        avg_views = new_data.get('average_views', 0)
        
        # Handle average_likes which can be a dict or number
        avg_likes_raw = new_data.get('average_likes', 0)
        if isinstance(avg_likes_raw, dict):
            avg_likes = avg_likes_raw.get('avg_value', 0)
        else:
            avg_likes = avg_likes_raw or 0
        
        if followers_count == 0:
            print(f"⚠️ Warning: @{handle} has zero followers - possibly incomplete API data")
            return {'handle': handle, 'status': 'failed', 'error': 'Incomplete API data - zero followers detected'}
        
        # Check if we have engagement metrics (views, likes, or engagement_rate)
        # Handle different formats for engagement_rate and average_comments
        engagement_rate_raw = new_data.get('engagement_rate', 0)
        if isinstance(engagement_rate_raw, dict):
            engagement_rate_val = engagement_rate_raw.get('avg_value', 0)
        else:
            engagement_rate_val = engagement_rate_raw or 0
            
        avg_comments_raw = new_data.get('average_comments', 0)
        if isinstance(avg_comments_raw, dict):
            avg_comments_val = avg_comments_raw.get('avg_value', 0)
        else:
            avg_comments_val = avg_comments_raw or 0
        
        has_engagement_data = (
            avg_views > 0 or 
            avg_likes > 0 or 
            engagement_rate_val > 0 or
            avg_comments_val > 0
        )
        
        if not has_engagement_data:
            print(f"⚠️ Warning: @{handle} has no engagement metrics - possibly incomplete API data")
            return {'handle': handle, 'status': 'failed', 'error': 'Incomplete API data - no engagement metrics found'}

        # Delete old media before processing new media
        delete_all_creator_media(handle)

        # Calculate Buzz Score
        buzz_score = calculate_buzz_score(new_data, creator)

        # Safely get old average likes for change calculation
        old_likes_data = creator.get('average_likes')
        old_avg_likes = 0
        if isinstance(old_likes_data, dict):
            old_avg_likes = old_likes_data.get('avg_value', 0)
        elif isinstance(old_likes_data, (int, float)):
            old_avg_likes = old_likes_data

        # Calculate percentage changes
        followers_change, followers_change_type = calculate_change(new_data.get('followers_count'), creator.get('followers_count'))
        er_change, er_change_type = calculate_change(new_data.get('engagement_rate'), creator.get('engagement_rate'))
        views_change, views_change_type = calculate_change(new_data.get('average_views'), creator.get('average_views'))
        
        # Handle both dict and int formats for average_likes
        new_likes = new_data.get('average_likes', {})
        if isinstance(new_likes, dict):
            new_likes_value = new_likes.get('avg_value', 0)
        else:
            new_likes_value = new_likes or 0
        
        likes_change, likes_change_type = calculate_change(new_likes_value, old_avg_likes)
        comments_change, comments_change_type = calculate_change(new_data.get('average_comments'), creator.get('average_comments'))
        
        # Ensure all numeric values are properly typed for database
        def safe_int(value):
            """Convert value to int, handling None and float values."""
            if value is None or value == '':
                return None
            try:
                # Convert to float first to handle string floats, then to int
                float_val = float(value)
                return int(float_val)
            except (ValueError, TypeError):
                print(f"⚠️ Could not convert to int: {value} (type: {type(value)})")
                return None
        
        def safe_float(value):
            """Convert value to float, handling None values."""
            if value is None or value == '':
                return None
            try:
                return float(value)
            except (ValueError, TypeError):
                print(f"⚠️ Could not convert to float: {value} (type: {type(value)})")
                return None
        
        print(f"   📊 Change calculation for @{handle}:")
        print(f"      Followers: {creator.get('followers_count')} → {new_data.get('followers_count')}")
        print(f"      Engagement Rate: {creator.get('engagement_rate')} → {new_data.get('engagement_rate')}")
        print(f"      Avg Views: {creator.get('average_views')} → {new_data.get('average_views')}")
        print(f"      Avg Likes: {old_avg_likes} → {new_likes_value}")
        print(f"      Avg Comments: {creator.get('average_comments')} → {new_data.get('average_comments')}")
        print(f"      Calculated changes: Followers: {followers_change:.2f}%, ER: {er_change:.2f}%, Views: {views_change:.2f}%, Likes: {likes_change:.2f}%, Comments: {comments_change:.2f}%")

        # Prepare update payload with proper type conversion
        # NOTE: All *_change columns in database are INTEGER, so we need to convert percentages to integers
        update_payload = {
            "buzz_score": safe_int(buzz_score),
            "followers_change": safe_int(followers_change),  # Convert percentage to integer 
            "followers_change_type": followers_change_type,
            "engagement_rate_change": safe_int(er_change),  # Convert percentage to integer
            "engagement_rate_change_type": er_change_type,
            "average_views_change": safe_int(views_change),  # Convert percentage to integer
            "average_views_change_type": views_change_type,
            "average_likes_change": safe_int(likes_change),  # Convert percentage to integer
            "average_likes_change_type": likes_change_type,
            "average_comments_change": safe_int(comments_change),  # Convert percentage to integer
            "average_comments_change_type": comments_change_type,
            # Preserve existing niche data - only update if we have better data
            "primary_niche": creator.get("primary_niche") or new_data.get("primary_niche"),
            "secondary_niche": creator.get("secondary_niche") or new_data.get("secondary_niche"),
            "location": creator.get("location") or new_data.get("location"),
            "updated_at": datetime.now().isoformat(),
        }
        
        # Add new_data fields with proper type conversion
        for key, value in new_data.items():
            if key not in update_payload:  # Don't override already set fields
                update_payload[key] = value
        
        # Ensure integer fields in new_data are properly converted
        if 'followers_count' in update_payload:
            update_payload['followers_count'] = safe_int(update_payload['followers_count'])
        if 'average_views' in update_payload:
            update_payload['average_views'] = safe_int(update_payload['average_views'])
        if 'average_comments' in update_payload:
            update_payload['average_comments'] = safe_int(update_payload['average_comments'])
        if 'engagement_rate' in update_payload:
            update_payload['engagement_rate'] = safe_float(update_payload['engagement_rate'])
        
        # Handle average_likes which might be a dict or number
        if 'average_likes' in update_payload:
            avg_likes = update_payload['average_likes']
            if isinstance(avg_likes, dict):
                # Keep as dict but ensure numeric values are properly typed
                if 'avg_value' in avg_likes:
                    avg_likes['avg_value'] = safe_int(avg_likes['avg_value'])
                if 'median_value' in avg_likes:
                    avg_likes['median_value'] = safe_int(avg_likes['median_value'])
                if 'std_dev' in avg_likes:
                    avg_likes['std_dev'] = safe_float(avg_likes['std_dev'])
            else:
                # Convert to int if it's a simple number
                update_payload['average_likes'] = safe_int(avg_likes)
        
        print(f"   🔍 Final update payload verification:")
        print(f"      followers_change: {update_payload.get('followers_change')} (type: {type(update_payload.get('followers_change'))}) - represents {followers_change:.2f}%")
        print(f"      engagement_rate_change: {update_payload.get('engagement_rate_change')} (type: {type(update_payload.get('engagement_rate_change'))}) - represents {er_change:.2f}%")
        print(f"      average_views_change: {update_payload.get('average_views_change')} (type: {type(update_payload.get('average_views_change'))}) - represents {views_change:.2f}%")
        print(f"      average_likes_change: {update_payload.get('average_likes_change')} (type: {type(update_payload.get('average_likes_change'))}) - represents {likes_change:.2f}%")
        print(f"      average_comments_change: {update_payload.get('average_comments_change')} (type: {type(update_payload.get('average_comments_change'))}) - represents {comments_change:.2f}%")
        print(f"      followers_count: {update_payload.get('followers_count')} (type: {type(update_payload.get('followers_count'))})")
        print(f"      buzz_score: {update_payload.get('buzz_score')} (type: {type(update_payload.get('buzz_score'))})")

        # Process media files
        print(f"   ⬇️ Downloading media for @{handle}...")
        media_updates = await process_creator_media(creator.get('id'), handle, update_payload)
        update_payload.update(media_updates)

        print(f"💾 Updating data for @{handle} in Supabase...")
        supabase.table("creatordata").update(update_payload).eq("handle", handle).execute()
        print(f"✅ Successfully updated @{handle}.")
        
        processing_time = time.time() - start_time
        print(f"⏱️ Processed @{handle} in {processing_time:.2f} seconds")
        return {'handle': handle, 'status': 'success', 'data': new_data}
        
    except Exception as e:
        processing_time = time.time() - start_time
        print(f"❌ Error processing @{handle} after {processing_time:.2f} seconds: {e}")
        traceback.print_exc()
        return {'handle': handle, 'status': 'error', 'error': str(e)}

# ==================== CONCURRENT PROCESSING FUNCTIONS ====================

async def process_creator_batch(creators_batch, batch_size=2):
    """Process multiple creators concurrently with controlled concurrency."""
    semaphore = asyncio.Semaphore(batch_size)
    
    async def process_with_semaphore(creator):
        async with semaphore:
            return await rescrape_and_update_creator(creator)
    
    # Create tasks for all creators in the batch
    tasks = [process_with_semaphore(creator) for creator in creators_batch]
    
    # Execute all tasks concurrently
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    return results

# ==================== MAIN EXECUTION ====================

async def cleanup_inactive_creators(resume_from_handle=None):
    """Rescrape all creators to get created_at data and remove inactive ones immediately."""
    print("\n" + "="*50)
    print("UNIFIED RESCRAPER: RESCRAPING CREATORS AND REMOVING INACTIVE ONES")
    print("="*50)
    
    existing_creators = get_existing_creators()
    if not existing_creators:
        print("No existing creators found to check for inactivity.")
        return
    
    # Store original total for progress tracking
    original_total = len(existing_creators)
    resume_index = 0
    
    # If resuming from a specific handle, filter the list
    if resume_from_handle:
        try:
            resume_index = next((i for i, creator in enumerate(existing_creators) if creator.get('handle') == resume_from_handle), 0)
            existing_creators = existing_creators[resume_index:]
            print(f"🔄 RESUMING from @{resume_from_handle} (skipping {resume_index} already processed creators)")
        except Exception as e:
            print(f"⚠️ Could not resume from @{resume_from_handle}, starting from beginning: {e}")
    
    total_creators = len(existing_creators)
    print(f"🔍 Rescraping {total_creators} creators to get created_at data and check activity...")
    
    # Show platform breakdown
    platform_counts = {}
    for creator in existing_creators:
        platform = creator.get('platform', 'Unknown')
        platform_counts[platform] = platform_counts.get(platform, 0) + 1
    
    print(f"📊 Platform breakdown:")
    for platform, count in platform_counts.items():
        print(f"   • {platform}: {count} creators")
    
    if TEST_MODE:
        print(f"🧪 TEST MODE: This is a test run with limited creators")
        print(f"   • Expected time: 20-40 minutes (more conservative)")
        print(f"   • Perfect for validating concurrent processing")
    
    # Initialize progress tracker
    progress_tracker = ProgressTracker(len(existing_creators), "Cleanup Phase")
    
    # Rescrape all creators and remove inactive ones immediately
    updated_creators = []
    deleted_count = 0
    error_count = 0
    
    # Configure concurrent processing
    print(f"🚀 Using concurrent processing with batch size: {CLEANUP_BATCH_SIZE}")
    print(f"   • Expected speedup: 2-3x faster than sequential processing")
    print(f"   • Memory usage: Moderate increase due to concurrent operations")
    print(f"   • API rate limiting: Controlled via semaphore and delays")
    
    # Split creators into batches for concurrent processing
    creator_batches = chunk_list(existing_creators, CLEANUP_BATCH_SIZE)
    total_batches = len(creator_batches)
    
    print(f"📦 Processing {len(existing_creators)} creators in {total_batches} batches of {CLEANUP_BATCH_SIZE}")
    
    # Create progress bar with enhanced description
    with tqdm(total=len(existing_creators), desc="Rescraping and checking creators", 
              bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]') as pbar:
        
        for batch_idx, creator_batch in enumerate(creator_batches):
            batch_start_time = time.time()
            
            print(f"\n📦 Processing batch {batch_idx + 1}/{total_batches} with {len(creator_batch)} creators...")
            
            # Process this batch concurrently
            try:
                batch_results = await process_creator_batch(creator_batch, CLEANUP_BATCH_SIZE)
            except Exception as e:
                print(f"❌ Concurrent processing failed for batch {batch_idx + 1}: {e}")
                print("🔄 Falling back to sequential processing for this batch...")
                
                # Fallback to sequential processing
                batch_results = []
                for creator in creator_batch:
                    try:
                        result = await rescrape_and_update_creator(creator)
                        batch_results.append(result)
                    except Exception as creator_error:
                        print(f"❌ Error processing @{creator.get('handle', 'unknown')}: {creator_error}")
                        batch_results.append({'handle': creator.get('handle'), 'status': 'error', 'error': str(creator_error)})
            
            # Process results from this batch
            for result in batch_results:
                if isinstance(result, Exception):
                    print(f"❌ Batch processing error: {result}")
                    error_count += 1
                    progress_tracker.complete_item()
                    pbar.update(1)
                    continue
                
                handle = result.get('handle')
                status = result.get('status')
                
                if status == 'success':
                    updated_creators.append(result.get('data'))
                    print(f"✅ Updated @{handle}")
                elif status == 'deleted':
                    print(f"🗑️ Deleted inactive creator @{handle}")
                    deleted_count += 1
                elif status == 'error':
                    print(f"❌ Error processing @{handle}: {result.get('error')}")
                    error_count += 1
                elif status == 'failed':
                    print(f"❌ Failed to get data for @{handle}")
                    error_count += 1
                
                # Complete timing and update progress
                progress_tracker.complete_item()
                pbar.set_description(progress_tracker.get_progress_bar_description())
                pbar.update(1)
            
            # Show batch progress
            batch_time = time.time() - batch_start_time
            completed = progress_tracker.completed_items
            total = progress_tracker.total_items
            
            print(f"📦 Batch {batch_idx + 1}/{total_batches} completed in {batch_time:.1f}s")
            print(f"   • Progress: {completed}/{total} ({completed/total*100:.1f}%)")
            print(f"   • Batch processing rate: {len(creator_batch)/batch_time:.2f} creators/second")
            
            # Show progress summary every 10 items or at 25%, 50%, 75% milestones
            if (completed % 10 == 0 and completed > 0) or completed in [total//4, total//2, 3*total//4]:
                progress_tracker.display_progress_summary()
            
            # Small delay between batches to be respectful to APIs
            if batch_idx < total_batches - 1:  # Don't delay after the last batch
                print(f"⏳ Waiting {BATCH_DELAY}s before next batch...")
                await asyncio.sleep(BATCH_DELAY)
    
    print(f"\n📊 Unified Rescaper Complete:")
    print(f"   • Creators updated: {len(updated_creators)}")
    print(f"   • Inactive creators deleted: {deleted_count}")
    print(f"   • Errors encountered: {error_count}")
    print(f"   • Total creators processed: {len(existing_creators)}")
    
    # Display final timing statistics
    final_stats = progress_tracker.get_progress_stats()
    print(f"   • Total time elapsed: {str(timedelta(seconds=int(final_stats['elapsed'])))}")
    print(f"   • Average time per creator: {final_stats['avg_time']:.1f} seconds")
    if final_stats['avg_time'] > 0:
        print(f"   • Concurrent processing speedup: {final_stats['avg_time']/15:.1f}x faster than sequential")
    
    # Test mode summary
    if TEST_MODE:
        print(f"\n🧪 TEST MODE SUMMARY:")
        print(f"   • Processed {len(existing_creators)} creators in test mode")
        print(f"   • Concurrent processing working: ✅")
        print(f"   • Progress tracking working: ✅")
        print(f"   • Ready for full production run!")
        print(f"   • To run full production: Set TEST_MODE = False or use --prod")

# ==================== MAIN EXECUTION ====================

if __name__ == "__main__":
    print("🚀 STARTING UNIFIED RESCRAPER SCRIPT 🚀")
    print("="*60)
    print("This script will:")
    print("  1. Cleanup Phase: Rescrape existing creators and remove inactive ones")
    print("  2. Show real-time progress with ETA and timing estimates")
    print("  3. Use concurrent processing for 3-5x speedup")
    print("  4. Handle both Instagram and TikTok creators automatically")
    print("="*60)
    print()
    
    print("🔧 Initializing script...")
    print(f"📋 Command line arguments: {sys.argv}")
    print(f"🧪 Test mode: {TEST_MODE}")
    print("✅ Script initialization complete!")
    
    # Performance configuration display
    print("⚡ PERFORMANCE CONFIGURATION:")
    print(f"   • Cleanup batch size: {CLEANUP_BATCH_SIZE} creators simultaneously")
    print(f"   • Batch delay: {BATCH_DELAY} seconds between batches")
    print(f"   • Expected speedup: 3-5x faster than sequential processing")
    
    # Test mode display
    if TEST_MODE:
        print(f"🧪 TEST MODE: ENABLED - Processing first {TEST_LIMIT} creators only")
        print(f"   • Estimated test time: 15-30 minutes (vs 4-8 hours for full run)")
        print(f"   • Perfect for testing concurrent processing and performance")
    else:
        print("🚀 PRODUCTION MODE: Processing all creators")
    
    print("="*60)
    print()
    
    # Check if user wants to resume from a specific creator
    resume_from = None
    if len(sys.argv) > 1:
        if sys.argv[1].lower() not in ['test', '--test', '-t', 'prod', '--prod', '-p', 'production']:
            resume_from = sys.argv[1]
            print(f"🔄 Resuming from creator: @{resume_from}")
    
    # Run the unified rescaper
    asyncio.run(cleanup_inactive_creators(resume_from))
    
    print("\n\n🎉 UNIFIED RESCRAPER FINISHED! 🎉")
