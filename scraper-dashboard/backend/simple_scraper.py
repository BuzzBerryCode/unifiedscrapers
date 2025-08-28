"""
Simple Unified Scraper - Reliable scraping without complexity
Optimized for Render deployment with minimal resource usage
"""
import os
import sys
import json
import time
import asyncio
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import traceback

# Add parent directory to path for API reliability
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

# Import API reliability functions
try:
    from api_reliability_fix import make_instagram_api_call, make_tiktok_api_call
    print("✅ Imported API reliability functions")
except ImportError as e:
    print(f"⚠️ Could not import API reliability: {e}")
    # Fallback to basic requests
    make_instagram_api_call = None
    make_tiktok_api_call = None

# Import existing calculation functions
try:
    from UnifiedRescaper import calculate_buzz_score, calculate_percentage_change
    print("✅ Imported calculation functions")
except ImportError:
    # Fallback implementations
    def calculate_buzz_score(new_data, existing_data):
        return 50  # Default score
    def calculate_percentage_change(old_val, new_val):
        if not old_val or old_val == 0:
            return 0, 'zero'
        change = ((new_val - old_val) / old_val) * 100
        change_type = 'positive' if change > 0 else 'negative' if change < 0 else 'zero'
        return round(change, 2), change_type

# Configuration
SCRAPECREATORS_API_KEY = os.getenv("SCRAPECREATORS_API_KEY", "")
MAX_RETRIES = 3
TIMEOUT_SECONDS = 30

class SimpleScraper:
    """Simple, reliable scraper focused on core functionality"""
    
    def __init__(self):
        self.api_key = SCRAPECREATORS_API_KEY
        if not self.api_key:
            print("⚠️ WARNING: No SCRAPECREATORS_API_KEY found")
    
    def scrape_instagram_creator(self, username: str) -> Optional[Dict]:
        """
        Scrape Instagram creator data with reliable API calls
        
        Args:
            username: Instagram username (without @)
            
        Returns:
            Creator data dict or None if failed
        """
        username = username.strip().lstrip('@')
        print(f"\n📱 Scraping Instagram: @{username}")
        
        try:
            # Get profile data
            if make_instagram_api_call:
                profile_result = make_instagram_api_call(username, self.api_key, "profile")
                if not profile_result['success']:
                    error_type = profile_result['error_type']
                    print(f"❌ Profile failed for @{username}: {error_type}")
                    if error_type == 'profile_not_found':
                        return None  # Permanent failure
                    else:
                        return {'error': 'temporary', 'username': username}
                profile_data = profile_result['data']
            else:
                print("❌ API reliability not available")
                return None
            
            # Get posts data
            posts_result = make_instagram_api_call(username, self.api_key, "posts")
            if not posts_result['success']:
                error_type = posts_result['error_type']
                print(f"⚠️ Posts failed for @{username}: {error_type}")
                if error_type in ['rate_limited', 'server_error', 'timeout']:
                    return {'error': 'temporary', 'username': username}
                posts_data = []  # Continue with empty posts
            else:
                posts_data = posts_result.get('data', [])
            
            print(f"✅ Got profile + {len(posts_data)} posts for @{username}")
            
            # Process data
            return self._process_instagram_data(username, profile_data, posts_data)
            
        except Exception as e:
            print(f"❌ Error scraping @{username}: {e}")
            traceback.print_exc()
            return None
    
    def scrape_tiktok_creator(self, username: str) -> Optional[Dict]:
        """
        Scrape TikTok creator data with reliable API calls
        
        Args:
            username: TikTok username (without @)
            
        Returns:
            Creator data dict or None if failed
        """
        username = username.strip().lstrip('@')
        print(f"\n🎵 Scraping TikTok: @{username}")
        
        try:
            if make_tiktok_api_call:
                result = make_tiktok_api_call(username, self.api_key)
                if not result['success']:
                    error_type = result['error_type']
                    print(f"❌ TikTok failed for @{username}: {error_type}")
                    if error_type == 'profile_not_found':
                        return None  # Permanent failure
                    else:
                        return {'error': 'temporary', 'username': username}
                tiktok_data = result['data']
            else:
                print("❌ API reliability not available")
                return None
            
            print(f"✅ Got TikTok data for @{username}")
            
            # Process data
            return self._process_tiktok_data(username, tiktok_data)
            
        except Exception as e:
            print(f"❌ Error scraping @{username}: {e}")
            traceback.print_exc()
            return None
    
    def _process_instagram_data(self, username: str, profile_data: Dict, posts_data: List) -> Dict:
        """Process Instagram data into creator record"""
        try:
            # Extract basic info
            followers = profile_data.get('follower_count', 0)
            following = profile_data.get('following_count', 0)
            bio = profile_data.get('biography', '') or ''
            profile_pic = profile_data.get('profile_pic_url', '')
            
            # Process posts for metrics
            recent_posts = {}
            views_list = []
            likes_list = []
            comments_list = []
            hashtags_list = []
            
            for i, post in enumerate(posts_data[:12], 1):  # Max 12 posts
                views = post.get('view_count', 0) or 0
                likes = post.get('like_count', 0) or 0
                comments = post.get('comment_count', 0) or 0
                timestamp = post.get('taken_at_timestamp', 0)
                
                # Store post data
                recent_posts[f'recent_post_{i}'] = {
                    'views': views,
                    'likes': likes,
                    'comments': comments,
                    'timestamp': timestamp,
                    'media_urls': post.get('display_url', '')
                }
                
                # Collect for averages
                if views > 0: views_list.append(views)
                if likes > 0: likes_list.append(likes)
                if comments > 0: comments_list.append(comments)
                
                # Extract hashtags from caption
                caption = post.get('caption', {})
                if isinstance(caption, dict):
                    caption_text = caption.get('text', '') or ''
                else:
                    caption_text = str(caption) if caption else ''
                
                hashtags = [tag.strip('#') for tag in caption_text.split() if tag.startswith('#')]
                hashtags_list.extend(hashtags)
            
            # Calculate averages
            avg_views = sum(views_list) / len(views_list) if views_list else 0
            avg_likes = sum(likes_list) / len(likes_list) if likes_list else 0
            avg_comments = sum(comments_list) / len(comments_list) if comments_list else 0
            engagement_rate = (avg_likes + avg_comments) / followers if followers > 0 else 0
            
            # Check activity (simple version)
            is_active = len(posts_data) > 0
            if posts_data:
                # Check if any post is from last 45 days
                cutoff = time.time() - (45 * 24 * 60 * 60)
                recent_posts_count = sum(1 for post in posts_data if post.get('taken_at_timestamp', 0) > cutoff)
                is_active = recent_posts_count > 0
            
            # Create creator data
            creator_data = {
                'handle': username,
                'platform': 'Instagram',
                'followers_count': int(followers),
                'following_count': int(following),
                'biography': bio[:500],  # Limit bio length
                'profile_image_url': profile_pic,
                'average_views': int(avg_views),
                'average_likes': {'avg_value': int(avg_likes)},  # Instagram format
                'average_comments': int(avg_comments),
                'engagement_rate': round(engagement_rate, 4),
                'hashtags': list(set(hashtags_list))[:20],  # Limit hashtags
                'is_active': is_active,
                'activity_status': 'active' if is_active else 'inactive',
                'created_at': datetime.utcnow().isoformat(),
                'updated_at': datetime.utcnow().isoformat(),
                **recent_posts
            }
            
            print(f"✅ Processed Instagram data: {followers:,} followers, {len(posts_data)} posts")
            return creator_data
            
        except Exception as e:
            print(f"❌ Error processing Instagram data for @{username}: {e}")
            traceback.print_exc()
            return None
    
    def _process_tiktok_data(self, username: str, tiktok_data: Dict) -> Dict:
        """Process TikTok data into creator record"""
        try:
            # Extract basic info
            followers = tiktok_data.get('followerCount', 0) or 0
            following = tiktok_data.get('followingCount', 0) or 0
            bio = tiktok_data.get('signature', '') or ''
            profile_pic = tiktok_data.get('avatarThumb', '')
            
            # Process videos
            videos = tiktok_data.get('videos', [])
            recent_posts = {}
            views_list = []
            likes_list = []
            comments_list = []
            hashtags_list = []
            
            for i, video in enumerate(videos[:12], 1):  # Max 12 videos
                views = video.get('playCount', 0) or 0
                likes = video.get('diggCount', 0) or 0
                comments = video.get('commentCount', 0) or 0
                shares = video.get('shareCount', 0) or 0
                timestamp = video.get('createTime', 0)
                
                # Store video data
                recent_posts[f'recent_post_{i}'] = {
                    'views': views,
                    'likes': likes,
                    'comments': comments,
                    'shares': shares,
                    'timestamp': timestamp,
                    'video_url': video.get('video', {}).get('downloadAddr', '') if isinstance(video.get('video'), dict) else ''
                }
                
                # Collect for averages
                if views > 0: views_list.append(views)
                if likes > 0: likes_list.append(likes)
                if comments > 0: comments_list.append(comments)
                
                # Extract hashtags from challenges
                challenges = video.get('challenges', [])
                if isinstance(challenges, list):
                    for challenge in challenges:
                        if isinstance(challenge, dict):
                            hashtag = challenge.get('hashtagName', '')
                            if hashtag:
                                hashtags_list.append(hashtag)
            
            # Calculate averages
            avg_views = sum(views_list) / len(views_list) if views_list else 0
            avg_likes = sum(likes_list) / len(likes_list) if likes_list else 0
            avg_comments = sum(comments_list) / len(comments_list) if comments_list else 0
            engagement_rate = (avg_likes + avg_comments) / followers if followers > 0 else 0
            
            # Activity check
            is_active = len(videos) > 0
            
            # Create creator data
            creator_data = {
                'handle': username,
                'platform': 'TikTok',
                'followers_count': int(followers),
                'following_count': int(following),
                'biography': bio[:500],  # Limit bio length
                'profile_image_url': profile_pic,
                'average_views': int(avg_views),
                'average_likes': int(avg_likes),  # TikTok format (direct number)
                'average_comments': int(avg_comments),
                'engagement_rate': round(engagement_rate, 4),
                'hashtags': list(set(hashtags_list))[:20],  # Limit hashtags
                'is_active': is_active,
                'activity_status': 'active' if is_active else 'inactive',
                'created_at': datetime.utcnow().isoformat(),
                'updated_at': datetime.utcnow().isoformat(),
                **recent_posts
            }
            
            print(f"✅ Processed TikTok data: {followers:,} followers, {len(videos)} videos")
            return creator_data
            
        except Exception as e:
            print(f"❌ Error processing TikTok data for @{username}: {e}")
            traceback.print_exc()
            return None
    
    def update_existing_creator(self, handle: str, new_data: Dict, existing_data: Dict) -> Dict:
        """Update existing creator with new data and calculate changes"""
        try:
            print(f"📊 Updating existing creator: @{handle}")
            
            # Validate new data quality
            followers_count = new_data.get('followers_count', 0)
            avg_views = new_data.get('average_views', 0)
            
            # Handle average_likes format differences
            avg_likes_raw = new_data.get('average_likes', 0)
            if isinstance(avg_likes_raw, dict):
                avg_likes = avg_likes_raw.get('avg_value', 0)
            else:
                avg_likes = avg_likes_raw or 0
            
            # Basic validation
            if followers_count == 0:
                print(f"⚠️ @{handle}: Zero followers detected - skipping update")
                return {'handle': handle, 'status': 'failed', 'error': 'Zero followers - incomplete data'}
            
            if avg_views == 0 and avg_likes == 0:
                print(f"⚠️ @{handle}: No engagement data - skipping update")
                return {'handle': handle, 'status': 'failed', 'error': 'No engagement data'}
            
            # Calculate percentage changes
            metrics_to_track = ['followers_count', 'following_count', 'average_views', 'average_likes', 'average_comments', 'engagement_rate']
            
            for metric in metrics_to_track:
                old_value = existing_data.get(metric, 0)
                new_value = new_data.get(metric, 0)
                
                # Handle dictionary formats
                if isinstance(old_value, dict):
                    old_value = old_value.get('avg_value', 0)
                if isinstance(new_value, dict):
                    new_value = new_value.get('avg_value', 0)
                
                if old_value is not None and new_value is not None:
                    try:
                        change, change_type = calculate_percentage_change(old_value, new_value)
                        new_data[f'{metric}_change'] = change
                        new_data[f'{metric}_change_type'] = change_type
                    except:
                        new_data[f'{metric}_change'] = 0
                        new_data[f'{metric}_change_type'] = 'zero'
            
            # Calculate Buzz Score
            try:
                buzz_score = calculate_buzz_score(new_data, existing_data)
                new_data['buzz_score'] = buzz_score
                print(f"📊 Buzz Score: {buzz_score}")
            except Exception as e:
                print(f"⚠️ Buzz Score calculation failed: {e}")
                new_data['buzz_score'] = existing_data.get('buzz_score', 50)
            
            # Preserve important existing data
            new_data['primary_niche'] = existing_data.get('primary_niche') or new_data.get('primary_niche', '')
            new_data['secondary_niche'] = existing_data.get('secondary_niche') or new_data.get('secondary_niche', '')
            new_data['created_at'] = existing_data.get('created_at')  # Preserve creation date
            new_data['updated_at'] = datetime.utcnow().isoformat()  # Update timestamp
            
            print(f"✅ Updated @{handle} successfully")
            return {'handle': handle, 'status': 'updated', 'buzz_score': new_data.get('buzz_score', 50)}
            
        except Exception as e:
            print(f"❌ Error updating @{handle}: {e}")
            traceback.print_exc()
            return {'handle': handle, 'status': 'error', 'error': str(e)}
    
    def create_new_creator(self, creator_data: Dict, additional_info: Dict = None) -> Dict:
        """Create a new creator record with validation"""
        try:
            handle = creator_data.get('handle', '')
            if not handle:
                return {'status': 'error', 'error': 'No handle provided'}
            
            # Add additional info if provided (from CSV upload)
            if additional_info:
                creator_data.update({
                    'primary_niche': additional_info.get('primary_niche', ''),
                    'secondary_niche': additional_info.get('secondary_niche', ''),
                    'notes': additional_info.get('notes', '')
                })
            
            # Set initial buzz score
            creator_data['buzz_score'] = 50  # Default score for new creators
            
            # Ensure timestamps
            creator_data['created_at'] = datetime.utcnow().isoformat()
            creator_data['updated_at'] = datetime.utcnow().isoformat()
            
            print(f"✅ Created new creator record for @{handle}")
            return {'handle': handle, 'status': 'created', 'data': creator_data}
            
        except Exception as e:
            print(f"❌ Error creating creator record: {e}")
            traceback.print_exc()
            return {'status': 'error', 'error': str(e)}

# Global instance
scraper = SimpleScraper()

def get_scraper() -> SimpleScraper:
    """Get the global scraper instance"""
    return scraper
