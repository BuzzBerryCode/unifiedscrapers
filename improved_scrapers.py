"""
Improved Scrapers with API Reliability Fix
==========================================

This module contains the updated scraper functions that use the new
API reliability system to handle errors gracefully and reduce failures.
"""

import sys
import os

# Add the current directory to path so we can import our API reliability fix
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from api_reliability_fix import make_instagram_api_call, make_tiktok_api_call, format_error_summary
import time
import re
from datetime import datetime, timedelta
from typing import Optional, Dict, List
import traceback

# Constants (these should be imported from your main scraper files)
SCRAPECREATORS_API_KEY = "wjhGgI14NjNMUuXA92YWXjojozF2"  # Replace with your actual key

def is_creator_active(recent_posts, days_threshold=45):
    """
    Check if a creator has posted in the last N days.
    """
    if not recent_posts:
        return False
    
    # Get the most recent post date
    valid_dates = []
    for post in recent_posts:
        if post.get('created_at'):
            try:
                # Parse the ISO format timestamp
                post_date = datetime.fromisoformat(post['created_at'].replace('Z', '+00:00'))
                valid_dates.append(post_date)
            except (ValueError, TypeError):
                continue
    
    if not valid_dates:
        return False
    
    # Get the most recent post date
    most_recent_post = max(valid_dates)
    current_time = datetime.now(most_recent_post.tzinfo) if most_recent_post.tzinfo else datetime.now()
    
    # Calculate the difference
    days_since_last_post = (current_time - most_recent_post).days
    
    print(f"📅 Most recent post: {most_recent_post.strftime('%Y-%m-%d')} ({days_since_last_post} days ago)")
    
    return days_since_last_post <= days_threshold

def safe_get(data, keys, default=None):
    """Safely get nested dictionary values"""
    if not isinstance(data, dict):
        return default
    current = data
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return default
    return current

def extract_hashtags(text):
    """Extract hashtags from text"""
    if not isinstance(text, str):
        return []
    return re.findall(r"#\w+", text)

def calculate_engagement_rate(likes, comments, followers):
    """Calculate engagement rate percentage"""
    if followers == 0:
        return 0.0
    return round(((likes + comments) / followers) * 100, 2)

def improved_scrape_instagram_user_data(username: str) -> Optional[Dict]:
    """
    Improved Instagram scraper with reliable API calls and better error handling.
    
    Returns:
        Dict: Creator data if successful
        Dict with 'error' key if failed but recoverable  
        None: If permanently failed (account deleted, etc.)
    """
    print(f"\n{'='*50}")
    print(f"🔄 Processing Instagram: @{username}")
    print(f"{'='*50}")
    
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
        return None  # Permanent skip - out of target range
    
    # Step 2: Get posts data with reliable API call
    posts_result = make_instagram_api_call(username, SCRAPECREATORS_API_KEY, "posts")
    
    if not posts_result['success']:
        error_type = posts_result['error_type']
        error_msg = posts_result['error_message']
        
        print(f"❌ Posts API call failed for @{username}: {error_msg}")
        
        # For posts, we might be able to continue with just profile data
        if error_type in ['rate_limited', 'server_error', 'timeout', 'circuit_breaker']:
            print(f"⏳ Using profile-only data due to posts API issue: {error_type}")
            # We'll create a minimal data set with just profile info
            posts_data = []
        else:
            return {'error': 'temporary', 'message': f'Posts API failed: {error_type}'}
    else:
        posts_data = posts_result['data'].get("items", [])
    
    print(f"📱 Found {len(posts_data)} posts")
    
    # Process posts data
    try:
        likes_list, comments_list, views_list = [], [], []
        all_hashtags, tagged_users, recent_posts = [], [], []
        past_ad_placements, all_captions, all_locations = [], [], []
        all_tagged_users_in_posts = []
        
        for idx, post in enumerate(posts_data[:12], start=1):
            caption_obj = post.get("caption", {})
            caption = caption_obj.get("text", "") if isinstance(caption_obj, dict) else str(caption_obj)
            all_captions.append(caption)
            
            usertags = post.get("usertags", {}).get("in", [])
            brand_usernames = [tag.get("user", {}).get("username") for tag in usertags if tag.get("user", {}).get("username")]
            all_tagged_users_in_posts.extend(brand_usernames)
            
            if post.get("is_paid_partnership", False) and brand_usernames:
                past_ad_placements.extend(brand_usernames)
            
            # Get engagement metrics
            like_hidden = post.get("like_and_view_counts_disabled", True)
            likes = post.get("like_count") if not like_hidden else None
            if not like_hidden and likes is not None:
                likes_list.append(likes)
            
            comments = post.get("comment_count", 0)
            comments_list.append(comments)
            
            play_count = post.get("play_count")
            if play_count is not None:
                views_list.append(play_count)
            
            # Extract media info
            is_video = post.get("media_type") in [2, 8]
            is_carousel = post.get("carousel_media_count", 0) > 0
            
            # Location data
            location = post.get("location", {})
            location_info = {
                "name": location.get("name"), 
                "id": location.get("id"), 
                "slug": location.get("slug"), 
                "address": location.get("address"), 
                "city": location.get("city")
            } if location.get("name") else None
            
            if location_info:
                all_locations.append(location_info)
            
            # Extract posting time
            taken_at = post.get("taken_at")
            created_at = None
            if taken_at and taken_at != 0:
                try:
                    if taken_at > 9999999999:  # Likely milliseconds
                        taken_at = taken_at / 1000
                    created_at = datetime.fromtimestamp(taken_at).isoformat()
                except (ValueError, TypeError):
                    created_at = None
            
            # Extract media URLs (simplified)
            media_urls = []
            if is_carousel:
                carousel_media = post.get("carousel_media", [])
                if carousel_media:
                    first_media = carousel_media[0]
                    if first_media.get("media_type") == 2:  # Video
                        video_versions = first_media.get("video_versions", [])
                        if video_versions:
                            media_urls.append(video_versions[0].get("url"))
                    else:  # Image
                        image_versions = first_media.get("image_versions2", {}).get("candidates", [])
                        if image_versions:
                            media_urls.append(image_versions[0].get("url"))
            elif is_video:
                video_versions = post.get("video_versions", [])
                if video_versions:
                    media_urls.append(video_versions[0].get("url"))
            else:
                image_versions = post.get("image_versions2", {}).get("candidates", [])
                if image_versions:
                    media_urls.append(image_versions[0].get("url"))
            
            # Extract hashtags
            hashtags = extract_hashtags(caption)
            all_hashtags.extend(hashtags)
            tagged_users.extend(brand_usernames)
            
            recent_posts.append({
                "caption": caption,
                "likes": likes,
                "comments": comments,
                "views": play_count,
                "is_video": is_video,
                "is_carousel": is_carousel,
                "media_urls": media_urls,
                "hashtags": hashtags,
                "brand_tags": brand_usernames,
                "is_paid_partnership": post.get("is_paid_partnership", False),
                "like_hidden": like_hidden,
                "location": location_info,
                "created_at": created_at
            })
        
        # Calculate averages (using posts 4-12 as before)
        likes_for_calc = [l for l in likes_list[3:12] if l is not None]
        comments_for_calc = [c for c in comments_list[3:12] if c is not None]
        views_for_calc = [v for v in views_list[3:12] if v is not None]
        
        avg_likes = sum(likes_for_calc) // len(likes_for_calc) if likes_for_calc else 0
        avg_comments = sum(comments_for_calc) // len(comments_for_calc) if comments_for_calc else 0
        avg_views = sum(views_for_calc) // len(views_for_calc) if views_for_calc else 0
        engagement_rate = calculate_engagement_rate(sum(likes_for_calc), sum(comments_for_calc), followers)
        
        print(f"📈 Metrics: {avg_likes:,} likes, {avg_comments:,} comments, {engagement_rate}% engagement")
        
        # Check if creator is active (posted in last 45 days)
        if not is_creator_active(recent_posts, days_threshold=45):
            print(f"🚫 Creator @{username} is inactive (no posts in 45+ days)")
            return {'skipped': True, 'reason': 'inactive'}
        
        # Build final data structure
        influencer_data = {
            "handle": username,
            "display_name": full_name or "",
            "profile_url": f"https://instagram.com/{username}",
            "profile_image_url": avatar_url or "",
            "bio": bio or "",
            "platform": "Instagram",
            "followers_count": followers,
            "average_views": avg_views,
            "average_likes": avg_likes,
            "average_comments": avg_comments,
            "engagement_rate": float(engagement_rate) if engagement_rate else 0.0,
            "hashtags": list(set(all_hashtags)) if all_hashtags else [],
            "brand_tags": list(set(tagged_users)) if tagged_users else [],
            "past_ad_placements": list(set(past_ad_placements)) if past_ad_placements else []
        }
        
        # Add recent posts
        for i in range(min(12, len(recent_posts))):
            influencer_data[f"recent_post_{i+1}"] = recent_posts[i]
        
        print(f"✅ Successfully processed Instagram data for @{username}")
        return influencer_data
        
    except Exception as e:
        print(f"❌ Error processing Instagram data for @{username}: {e}")
        traceback.print_exc()
        return {'error': 'temporary', 'message': f'Data processing error: {e}'}

def improved_scrape_tiktok_user_data(username: str) -> Optional[Dict]:
    """
    Improved TikTok scraper with reliable API calls and better error handling.
    """
    print(f"\n{'='*50}")
    print(f"🔄 Processing TikTok: @{username}")
    print(f"{'='*50}")
    
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
            return None  # Permanent skip
        
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
                else:
                    # Try alternative video URL structure
                    ai_dynamic_cover = video_data.get('ai_dynamic_cover', {})
                    if ai_dynamic_cover and ai_dynamic_cover.get('url_list'):
                        video_url = ai_dynamic_cover['url_list'][0]
            
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
            "bio_links": ""
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

# Wrapper functions for backward compatibility
def scrape_instagram_user_data(username: str) -> Optional[Dict]:
    """Wrapper for backward compatibility - improved version."""
    result = improved_scrape_instagram_user_data(username)
    
    # Convert error responses to None for backward compatibility if needed
    if isinstance(result, dict) and 'error' in result:
        # For temporary errors, we return None so the job will mark it as failed
        # but the error details are lost. In the future, the calling code should
        # be updated to handle these error responses properly.
        print(f"🔄 Converting error response to None for backward compatibility")
        return None
    
    return result

def scrape_tiktok_user_data(username: str) -> Optional[Dict]:
    """Wrapper for backward compatibility - improved version."""
    result = improved_scrape_tiktok_user_data(username)
    
    # Convert error responses to None for backward compatibility
    if isinstance(result, dict) and 'error' in result:
        print(f"🔄 Converting error response to None for backward compatibility")
        return None
    
    return result

if __name__ == "__main__":
    # Test the improved scrapers
    print("🧪 Testing improved scrapers...")
    
    # Test Instagram
    test_instagram_user = "levelupcreditconsulting"  # From your failed list
    result = improved_scrape_instagram_user_data(test_instagram_user)
    print(f"Instagram test result: {type(result)} - {result.get('handle') if result else 'Failed'}")
    
    # Test TikTok  
    test_tiktok_user = "investingcameronscrubs"  # From your failed list
    result = improved_scrape_tiktok_user_data(test_tiktok_user)
    print(f"TikTok test result: {type(result)} - {result.get('handle') if result else 'Failed'}")
