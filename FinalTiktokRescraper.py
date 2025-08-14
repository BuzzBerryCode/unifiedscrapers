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


# --- Configuration ---
# IMPORTANT: It is recommended to use environment variables for keys.
SUPABASE_URL = "https://unovwhgnwenxbyvpevcz.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InVub3Z3aGdud2VueGJ5dnBldmN6Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MjEzNjU0MCwiZXhwIjoyMDY3NzEyNTQwfQ.bqXWKTR64TRARH2VOrjiQdPnQ7W-8EGJp5RIi7yFmck"
GEMINI_API_KEY = "AIzaSyBYRd9lJTe1mRgJLhpbp39butQbXDgBBMw"
SCRAPECREATORS_API_KEY = "wjhGgI14NjNMUuXA92YWXjojozF2"

# --- Initialization ---
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
genai.configure(api_key=GEMINI_API_KEY)
gemini_model = genai.GenerativeModel('gemini-1.5-flash')

BUCKET_NAME = "profile-media"
TABLE_NAME = "creatordata"
MAX_RECENT_POSTS = 4

# --- Niche Definitions ---
PRESET_TRADING_NICHES = [
    "Forex Trading", "Stock Market", "Options Trading", "Futures Trading",
    "Crypto Trading", "Technical Analysis", "Fundamental Analysis",
    "Trading Education", "Trading Signals Provider", "General Trading", "Non-Trading"
]

PRESET_CRYPTO_NICHES = [
    "Altcoins", "DeFi", "NFTs", "Crypto Airdrops", "Web3", 
    "Crypto Trading", "Crypto News", "Market Analysis", "Meme Coins", "General Crypto"
]

PRESET_FINANCE_NICHES = [
    "Personal Finance", "Investing", "Stock Market", "Real Estate", 
    "Financial News", "Budgeting", "Credit & Debt", "Retirement Planning", "General Finance"
]

# ==================== Helper & Calculation Functions ====================

def is_creator_active(recent_posts, days_threshold=45):
    """
    Check if a creator has posted in the last N days.
    A creator is considered active if ANY of their posts are within the threshold.
    
    Args:
        recent_posts: List of post dictionaries with 'created_at' timestamps
        days_threshold: Number of days to check (default: 45)
    
    Returns:
        bool: True if creator has posted within the threshold, False otherwise
    """
    if not recent_posts:
        return False
    
    # Check if ANY post is within the threshold
    current_time = datetime.now()
    
    for post in recent_posts:
        if post.get('created_at'):
            try:
                # Parse the ISO format timestamp
                post_date = datetime.fromisoformat(post['created_at'].replace('Z', '+00:00'))
                
                # Calculate the difference
                days_since_post = (current_time - post_date).days
                
                # If ANY post is within the threshold, creator is active
                if days_since_post <= days_threshold:
                    print(f"📅 Found active post: {post_date.strftime('%Y-%m-%d')} ({days_since_post} days ago)")
                    return True
                    
            except (ValueError, TypeError):
                continue
    
    # If we get here, no posts are within the threshold
    print(f"📅 No posts found within {days_threshold} days")
    return False

def get_median(data_list):
    """Calculates the median of a list of numbers."""
    if not data_list: return 0
    return statistics.median(data_list)

def get_standard_deviation(data_list):
    """Calculates the standard deviation of a list of numbers."""
    if len(data_list) < 2: return 0
    return statistics.stdev(data_list)

def calculate_buzz_score(new_data, existing_data):
    """Calculates the Buzz Score based on growth, engagement, and consistency."""
    print("💯 Calculating Buzz Score...")

    # --- Prepare Current Data ---
    views_now = [p.get('views', 0) for i in range(1, 13) if (p := new_data.get(f'recent_post_{i}')) and p.get('views') is not None] or [0]
    median_views_now = get_median(views_now)
    std_dev_views = get_standard_deviation(views_now)
    followers_now = new_data.get('followers_count', 0)
    avg_likes_now = new_data.get('average_likes', 0)
    avg_comments_now = new_data.get('average_comments', 0)

    # --- Prepare Historical Data ---
    views_last_week = [p.get('views', 0) for i in range(1, 13) if (p := existing_data.get(f'recent_post_{i}')) and p.get('views') is not None] or [0]
    median_views_last_week = get_median(views_last_week)
    followers_last_week = existing_data.get('followers_count', 0)
    old_likes_data = existing_data.get('average_likes')
    avg_likes_last_week = old_likes_data if isinstance(old_likes_data, int) else (old_likes_data or {}).get('avg_value', 0)
    avg_comments_last_week = existing_data.get('average_comments', 0)

    # --- 1. Growth Score ---
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

    # --- 2. Engagement Score ---
    engagement_rate = (avg_likes_now + avg_comments_now) / followers_now if followers_now else 0
    comment_like_ratio = avg_comments_now / avg_likes_now if avg_likes_now else 0
    engagement_score = 25
    if engagement_rate >= 0.06: engagement_score = 100
    elif engagement_rate >= 0.04: engagement_score = 80
    elif engagement_rate >= 0.02: engagement_score = 60
    elif engagement_rate >= 0.01: engagement_score = 40
    if comment_like_ratio >= 0.10: engagement_score += 5
    engagement_score = min(engagement_score, 100)

    # --- 3. Consistency Score ---
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

    # --- Final Buzz Score ---
    final_buzz_score = (growth_score * 0.45) + (engagement_score * 0.35) + (consistency_score * 0.20)
    print(f"✅ Buzz Score calculated: {final_buzz_score:.2f}")
    return int(final_buzz_score)

def calculate_change(new_value, old_value):
    """Calculates the percentage change and determines the change type."""
    if old_value is None or new_value is None or old_value == 0:
        return 0, 'zero'
    try:
        # Calculate percentage change: ((new - old) / old) * 100
        change = int(((float(new_value) - float(old_value)) / float(old_value)) * 100)
    except (ValueError, TypeError):
        return 0, 'zero'

    if change > 0:
        change_type = 'positive'
    elif change < 0:
        change_type = 'negative'
    else:
        change_type = 'zero'
    return change, change_type



def safe_get(data, keys, default=None):
    """Safely get nested dictionary values"""
    if not isinstance(data, dict): return default
    current = data
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return default
    return current

def calculate_engagement_rate(likes, comments, followers):
    """Calculate engagement rate percentage"""
    if followers == 0: return 0.0
    return round(((likes + comments) / followers) * 100, 2)

# ==================== Media Handling Functions ====================
def convert_heic_bytes_to_jpg(heic_bytes: bytes) -> bytes:
    """Convert HEIC image bytes to JPEG using pillow-heif."""
    heif_file = pillow_heif.read_heif(io.BytesIO(heic_bytes))
    image = Image.frombytes(heif_file.mode, heif_file.size, heif_file.data)
    output = io.BytesIO()
    image.save(output, format="JPEG")
    return output.getvalue()

def download_file(url: str) -> bytes:
    """Download file from URL and return its content as bytes. Converts .heic to .jpg if needed."""
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        content_type = response.headers.get("Content-Type", "")
        if "heic" in content_type or url.lower().endswith(".heic"):
            print(f"Converting HEIC image to JPEG: {url}")
            return convert_heic_bytes_to_jpg(response.content)
        return response.content
    except requests.RequestException as e:
        print(f"Error downloading {url}: {e}")
        return None
    except Exception as e:
        print(f"Error processing file {url}: {e}")
        return None

def upload_to_supabase_storage(bucket: str, path: str, file_content: bytes, content_type: str = None) -> str:
    """Upload file to Supabase storage and return public URL."""
    try:
        supabase.storage.from_(bucket).upload(path=path, file=file_content, file_options={"content-type": content_type, "upsert": "true"})
        return supabase.storage.from_(bucket).get_public_url(path)
    except Exception as e:
        print(f"Error uploading to Supabase storage: {e}")
        return None

def get_file_extension_and_type(url: str) -> tuple:
    """Extract file extension and content type from URL."""
    path = urlparse(url).path
    ext = os.path.splitext(path)[1] or '.jpg'
    content_type, _ = mimetypes.guess_type(url)
    if not content_type:
        content_type = 'video/mp4' if ext in ['.mp4', '.mov'] else 'image/jpeg'
    return ext, content_type

def clean_handle(handle: str) -> str:
    """Clean the handle for use as a folder name."""
    return re.sub(r'[\\/*?:"<>|]', '_', handle)


# ==================== AI & Data Extraction Functions ====================
def is_crypto_influencer(bio: str, username: str = "") -> bool:
    """Uses Gemini AI to decide if a user is a crypto influencer."""
    print("🧠 Determining if user is a crypto influencer...")
    prompt = f'Analyze the TikTok username and bio. Is this user primarily a crypto-related influencer (mentions crypto, bitcoin, blockchain, NFTs, DeFi, Web3, etc.)? Respond with ONLY "Yes" or "No".\nUSERNAME: {username}\nBIO: {bio}'
    try:
        response = gemini_model.generate_content(prompt)
        return "yes" in response.text.strip().lower()
    except Exception as e:
        print(f"⚠️ Error in is_crypto_influencer: {e}")
        return False

def predict_crypto_secondary_niche(bio, all_hashtags, all_tagged_users):
    """Predict secondary crypto niche using Gemini AI."""
    print("🔮 Predicting secondary crypto niche...")
    prompt = f'Analyze this TikTok account and classify into ONE crypto niche from this list: {PRESET_CRYPTO_NICHES}. Consider the bio, hashtags, and tagged users. Return ONLY one niche name. If unsure, return "General Crypto".\nBIO: "{bio}"\nHASHTAGS: {list(set(all_hashtags))}\nTAGGED USERS: {list(set(all_tagged_users))}'
    try:
        response = gemini_model.generate_content(prompt)
        niche = response.text.strip()
        return niche if niche in PRESET_CRYPTO_NICHES else "General Crypto"
    except Exception as e:
        print(f"⚠️ Crypto niche prediction failed: {e}")
        return "General Crypto"

def is_trading_influencer(bio: str, username: str = "") -> bool:
    """Uses Gemini AI to decide if a user is a trading influencer."""
    print("🧠 Determining if user is a trading influencer...")
    prompt = f'Analyze the TikTok username and bio. Is this user a trading-related influencer (mentions trading, stocks, forex, options, etc.)? Respond with ONLY "Yes" or "No".\nUSERNAME: {username}\nBIO: {bio}'
    try:
        response = gemini_model.generate_content(prompt)
        return "yes" in response.text.strip().lower()
    except Exception as e:
        print(f"⚠️ Error in is_trading_influencer: {e}")
        return False

def predict_trading_secondary_niche(bio, all_hashtags, all_tagged_users):
    """Predict secondary trading niche using Gemini AI."""
    print("🔮 Predicting secondary trading niche...")
    prompt = f'Analyze this TikTok account and classify into ONE trading niche from this list: {PRESET_TRADING_NICHES}. Return ONLY one niche name. If unsure, return "General Trading".\nBIO: "{bio}"\nHASHTAGS: {list(set(all_hashtags))}\nTAGGED USERS: {list(set(all_tagged_users))}'
    try:
        response = gemini_model.generate_content(prompt)
        niche = response.text.strip()
        return niche if niche in PRESET_TRADING_NICHES else "General Trading"
    except Exception as e:
        print(f"⚠️ Trading niche prediction failed: {e}")
        return "General Trading"

def is_finance_influencer(bio: str, username: str = "") -> bool:
    """Uses Gemini AI to decide if a user is a finance influencer."""
    print("🧠 Determining if user is a finance influencer...")
    prompt = f'Analyze the TikTok username and bio. Is this user a general/personal finance influencer (mentions investing, budgeting, saving, credit, etc.)? Exclude users ONLY focused on active trading or crypto. Respond with ONLY "Yes" or "No".\nUSERNAME: {username}\nBIO: {bio}'
    try:
        response = gemini_model.generate_content(prompt)
        return "yes" in response.text.strip().lower()
    except Exception as e:
        print(f"⚠️ Error in is_finance_influencer: {e}")
        return False

def predict_finance_secondary_niche(bio, all_hashtags, all_tagged_users):
    """Predict secondary finance niche using Gemini AI."""
    print("🔮 Predicting secondary finance niche...")
    prompt = f'Analyze this TikTok account and classify into ONE finance niche from this list: {PRESET_FINANCE_NICHES}. Return ONLY one niche name. If unsure, return "General Finance".\nBIO: "{bio}"\nHASHTAGS: {list(set(all_hashtags))}\nTAGGED USERS: {list(set(all_tagged_users))}'
    try:
        response = gemini_model.generate_content(prompt)
        niche = response.text.strip()
        return niche if niche in PRESET_FINANCE_NICHES else "General Finance"
    except Exception as e:
        print(f"⚠️ Finance niche prediction failed: {e}")
        return "General Finance"

def predict_freeform_location(bio, all_captions, region):
    """Predict location in free-form text using Gemini AI."""
    print("🌍 Predicting location...")
    prompt = f'Predict the most likely location (City, Country) for this user. Prioritize: 1. Profile Region, 2. Bio, 3. Captions. If uncertain, return "Global".\nBIO: "{bio}"\nCAPTIONS: {" ".join(all_captions[:5])}\nREGION: {region}\nReturn ONLY the location.'
    try:
        response = gemini_model.generate_content(prompt)
        return response.text.strip()
    except Exception as e:
        print(f"⚠️ Location prediction failed: {e}")
        return "Global"

def extract_hashtags(text): return [tag.lower() for tag in re.findall(r"#(\w+)", text or "")]
def extract_tagged_users(text): return re.findall(r"@(\w+)", text or "")
def extract_emails(text): return re.findall(r"\b[\w\.-]+@[\w\.-]+\.\w{2,4}\b", text or "")



def process_tiktok_account(username, is_update=False):
    """Process a single TikTok account and return structured data. Skips AI analysis if is_update is True."""
    print(f"\n📡 Fetching all data for @{username}...")
    api_url = f"https://api.scrapecreators.com/v3/tiktok/profile/videos?handle={username}"
    headers = {"x-api-key": SCRAPECREATORS_API_KEY}
    response = requests.get(api_url, headers=headers)
    
    if response.status_code != 200:
        print(f"❌ Failed to fetch data: {response.status_code}")
        return None
    
    posts = response.json().get('aweme_list', [])
    if not posts:
        print("❌ No posts found for this account")
        return None
    
    user_info = posts[0].get('author', {})
    bio = user_info.get('signature', '')
    followers = user_info.get('follower_count', 0)

    # --- Niche Validation (only for new creators) ---
    primary_niche = None
    if not is_update:
        if is_crypto_influencer(bio=bio, username=username):
            primary_niche = "Crypto"
        elif is_trading_influencer(bio=bio, username=username):
            primary_niche = "Trading"
        elif is_finance_influencer(bio=bio, username=username):
            primary_niche = "Finance"

        if not primary_niche:
            print(f"🚫 Skipping @{username}: Not identified as a Trading, Crypto, or Finance influencer.")
            return {'skipped': True}
        print(f"✅ Primary Niche Identified: {primary_niche}")

    if not (10000 <= followers <= 350000):
        print(f"🚫 Skipped: Follower count {followers} outside 10k-350k range")
        return {'skipped': True}

    likes_list, comments_list, views_list = [], [], []
    all_hashtags, all_tagged_users, all_captions, past_ad_placements = [], [], [], []
    recent_posts_data = []

    for post in posts[:12]:
        caption = post.get('desc', '')
        stats = post.get('statistics', {})
        all_captions.append(caption)
        likes_list.append(stats.get('digg_count', 0))
        comments_list.append(stats.get('comment_count', 0))
        views_list.append(stats.get('play_count', 0))
        all_hashtags.extend(extract_hashtags(caption))
        tagged_users = extract_tagged_users(caption)
        all_tagged_users.extend(tagged_users)
        
        is_paid = post.get("commerce_info", {}).get("bc_label_test_text", "") == "Paid partnership"
        if is_paid: past_ad_placements.extend(tagged_users)

        # Extract TikTok posting time
        created_at = None
        create_time = post.get('create_time')
        if create_time:
            try:
                created_at = datetime.fromtimestamp(create_time).isoformat()
            except (ValueError, TypeError):
                created_at = None

        recent_posts_data.append({
            "caption": caption,
            "likes": stats.get('digg_count', 0),
            "comments": stats.get('comment_count', 0),
            "views": stats.get('play_count', 0),
            "video_url": post.get('video', {}).get('ai_dynamic_cover', {}).get('url_list', [''])[0],
            "is_video": True,
            "is_carousel": False,
            "created_at": created_at
        })

    # --- Calculations based on the last 9 of the 12 posts ---
    print("📊 Calculating metrics based on the last 9 of the 12 posts...")
    likes_for_calc = likes_list[3:]
    comments_for_calc = comments_list[3:]
    views_for_calc = views_list[3:]

    avg_likes = sum(likes_for_calc) // len(likes_for_calc) if likes_for_calc else 0
    avg_comments = sum(comments_for_calc) // len(comments_for_calc) if comments_for_calc else 0
    avg_views = sum(views_for_calc) // len(views_for_calc) if views_for_calc else 0
    
    engagement_rate = calculate_engagement_rate(sum(likes_for_calc), sum(comments_for_calc), followers)
    
    # --- Base influencer data with metrics ---
    influencer_data = {
        "handle": user_info.get('unique_id'),
        "display_name": user_info.get('nickname'),
        "profile_url": f"https://www.tiktok.com/@{user_info.get('unique_id')}",
        "profile_image_url": user_info.get('avatar_thumb', {}).get('url_list', [''])[0],
        "bio": bio,
        "platform": "TikTok",
        "followers_count": followers,
        "average_views": avg_views,
        "average_likes": avg_likes,
        "average_comments": avg_comments,
        "engagement_rate": engagement_rate,
        "hashtags": list(set(all_hashtags)),
        "email": (extract_emails(bio) or [None])[0],
        "past_ad_placements": list(set(past_ad_placements)),
        "brand_tags": list(set(all_tagged_users)),
    }
    
    # --- Add AI Predictions ONLY for new creators ---
    if not is_update:
        secondary_niche = "General"
        if primary_niche == "Trading":
            secondary_niche = predict_trading_secondary_niche(bio, all_hashtags, all_tagged_users)
        elif primary_niche == "Crypto":
            secondary_niche = predict_crypto_secondary_niche(bio, all_hashtags, all_tagged_users)
        elif primary_niche == "Finance":
            secondary_niche = predict_finance_secondary_niche(bio, all_hashtags, all_tagged_users)
        
        influencer_data["primary_niche"] = primary_niche
        influencer_data["secondary_niche"] = secondary_niche
        influencer_data["location"] = predict_freeform_location(bio, all_captions, posts[0].get('region', ''))

    for i, post in enumerate(recent_posts_data):
        influencer_data[f"recent_post_{i+1}"] = post

    # Check if creator has posted in the last 45 days
    print("\n📅 Checking creator activity...")
    if not is_creator_active(recent_posts_data, days_threshold=45):
        print(f"🚫 Skipping @{username}: No posts in the last 45 days")
        return {'skipped': True}
        
    return influencer_data


# ==================== Supabase Interaction & Main Flow ====================

def get_existing_creators():
    """Fetches all existing Instagram Trading, Crypto, and Finance creator data from Supabase using pagination."""
    print("\nFetching existing 'TikTok' & ('Trading', 'Crypto', 'Finance') creators from Supabase...")
    target_niches = ['Trading', 'Crypto', 'Finance']
    all_creators = []
    offset = 0
    batch_size = 1000  # Supabase's default max limit

    while True:
        try:
            # Fetch a batch of records
            response = supabase.table("creatordata").select(
                "*"
            ).in_('primary_niche', target_niches).eq('platform', 'TikTok').range(offset, offset + batch_size - 1).execute()

            if response.data:
                # Add the fetched creators to our main list
                all_creators.extend(response.data)
                # If we got fewer records than our batch size, it means we've reached the end
                if len(response.data) < batch_size:
                    break
                # Otherwise, prepare to fetch the next batch
                offset += batch_size
            else:
                # No more data to fetch
                break
        except Exception as e:
            print(f"❌ Error fetching existing creators: {e}")
            return [] # Return empty list on error

    print(f"Found {len(all_creators)} existing creators in the target niches.")
    return all_creators

async def cleanup_inactive_creators():
    """Rescrape all creators to get created_at data and remove inactive ones immediately."""
    print("\n" + "="*50)
    print("CLEANUP: RESCRAPING CREATORS AND REMOVING INACTIVE ONES")
    print("="*50)
    
    existing_creators = get_existing_creators()
    if not existing_creators:
        print("No existing creators found to check for inactivity.")
        return
    
    print(f"🔍 Rescraping {len(existing_creators)} creators to get created_at data and check activity...")
    
    # Rescrape all creators and remove inactive ones immediately
    updated_creators = []
    deleted_count = 0
    
    for creator in tqdm(existing_creators, desc="Rescraping and checking creators"):
        handle = creator.get('handle')
        if not handle:
            continue
            
        # Rescrape the creator to get fresh data with created_at timestamps
        fresh_data = process_tiktok_account(handle, is_update=True)
        
        if fresh_data and not fresh_data.get('skipped'):
            # Update the creator with fresh data including created_at timestamps
            try:
                # First update the database with fresh data
                supabase.table(TABLE_NAME).update(fresh_data).eq("handle", handle).execute()
                
                # Then download and upload media files
                print(f"⬇️ Downloading media for @{handle}...")
                media_updates = await process_creator_media(creator.get('id'), handle, fresh_data)
                
                # Update with media URLs if any were processed
                if media_updates:
                    supabase.table(TABLE_NAME).update(media_updates).eq("handle", handle).execute()
                    print(f"✅ Updated @{handle} with fresh data and media")
                else:
                    print(f"✅ Updated @{handle} with fresh data (no media processed)")
                
                updated_creators.append(fresh_data)
            except Exception as e:
                print(f"❌ Error updating @{handle}: {e}")
        elif fresh_data and fresh_data.get('skipped'):
            # Delete inactive creator immediately
            print(f"🗑️ Deleting inactive creator @{handle} from database...")
            try:
                supabase.table(TABLE_NAME).delete().eq("handle", handle).execute()
                print(f"✅ Successfully deleted inactive creator @{handle}")
                deleted_count += 1
            except Exception as e:
                print(f"❌ Error deleting inactive creator @{handle}: {e}")
        else:
            print(f"❌ Failed to get data for @{handle}")
    
    print(f"\n📊 Cleanup Complete:")
    print(f"   • Creators updated: {len(updated_creators)}")
    print(f"   • Inactive creators deleted: {deleted_count}")
    print(f"   • Total creators processed: {len(existing_creators)}")

def delete_all_creator_media(handle: str):
    """Deletes all media files for a given creator from Supabase Storage."""
    print(f"🗑️ Attempting to delete old media for @{handle}...")
    storage_folder = f"{clean_handle(handle)}/"
    try:
        files = supabase.storage.from_(BUCKET_NAME).list(path=storage_folder)
        if files:
            paths = [f"{storage_folder}{file['name']}" for file in files]
            supabase.storage.from_(BUCKET_NAME).remove(paths=paths)
            print(f"✅ Successfully deleted {len(paths)} old media files.")
    except Exception as e:
        if "The resource was not found" not in str(e):
            print(f"❌ Error deleting media: {e}")




async def process_creator_media(creator_id: str, handle: str, creator_data: dict):
    """Process media for a single creator and return a dictionary of updates."""
    clean_handle_name = clean_handle(handle)
    storage_folder = f"{clean_handle_name}/"
    updates = {}
    processed_media = 0

    # Process profile image
    if creator_data.get("profile_image_url"):
        profile_ext, profile_content_type = get_file_extension_and_type(creator_data["profile_image_url"])
        profile_storage_path = f"{storage_folder}profile{profile_ext}"
        
        print(f"🖼️ Downloading profile image for @{handle}...")
        profile_content = download_file(creator_data["profile_image_url"])
        
        if profile_content:
            new_url = upload_to_supabase_storage(
                BUCKET_NAME, 
                profile_storage_path, 
                profile_content, 
                profile_content_type
            )
            if new_url:
                updates["profile_image_url"] = new_url
                print(f"  ✅ Successfully processed profile image")
            else:
                print(f"  ❌ Failed to upload profile image")
        else:
            print(f"  ❌ Failed to download profile image")

    # Process recent posts
    for i in range(1, MAX_RECENT_POSTS + 1):
        post_key = f"recent_post_{i}"
        if processed_media >= MAX_RECENT_POSTS: break
        
        post = creator_data.get(post_key)
        if not (post and isinstance(post, dict) and post.get("video_url")): continue
        
        media_url = post.get("video_url")
        if not (media_url and isinstance(media_url, str) and media_url.startswith("http")): continue

        try:
            print(f"  📹 Processing post {i} media for @{handle}...")
            ext, content_type = get_file_extension_and_type(media_url)
            media_storage_path = f"{storage_folder}media_{processed_media + 1}{ext}"
            file_content = download_file(media_url)
            if file_content:
                new_url = upload_to_supabase_storage(BUCKET_NAME, media_storage_path, file_content, content_type)
                if new_url:
                    post["video_url"] = new_url
                    updates[post_key] = post
                    processed_media += 1
                    print(f"  ✅ Successfully processed post {i} media")
                else:
                    print(f"  ❌ Failed to upload post {i} media")
            else:
                print(f"  ❌ Failed to download post {i} media")
        except Exception as e:
            print(f"  ❌ Error processing post {i} media: {e}")
            continue

    # Return the dictionary of updates instead of calling the database here
    print(f"  📊 Media processing complete: {processed_media} posts processed")
    return updates



async def rescrape_and_update_creator(handle, existing_data):
    """Rescrapes a TikTok creator and updates their record."""
    print(f"\n{'='*20} RESCRAPING @{handle} {'='*20}")
    
    # Pass is_update=True to skip AI analysis for existing users
    new_data = process_tiktok_account(handle, is_update=True)
    
    if not new_data:
        print(f"ℹ️ No data returned for @{handle}, skipping update.")
        return
    
    # Check if creator was skipped due to inactivity (no posts in 45 days)
    if new_data.get('skipped'):
        print(f"🗑️ Deleting inactive creator @{handle} from database...")
        try:
            supabase.table(TABLE_NAME).delete().eq("handle", handle).execute()
            print(f"✅ Successfully deleted inactive creator @{handle}")
        except Exception as e:
            print(f"❌ Error deleting inactive creator @{handle}: {e}")
        return

    delete_all_creator_media(handle)
    buzz_score = calculate_buzz_score(new_data, existing_data)
    
    followers_change, followers_change_type = calculate_change(new_data.get('followers_count'), existing_data.get('followers_count'))
    er_change, er_change_type = calculate_change(new_data.get('engagement_rate'), existing_data.get('engagement_rate'))
    views_change, views_change_type = calculate_change(new_data.get('average_views'), existing_data.get('average_views'))
    likes_change, likes_change_type = calculate_change(new_data.get('average_likes'), (existing_data.get('average_likes') or 0))
    comments_change, comments_change_type = calculate_change(new_data.get('average_comments'), existing_data.get('average_comments'))

    update_payload = {
        **new_data,
        "buzz_score": buzz_score,
        "followers_change": followers_change, "followers_change_type": followers_change_type,
        "engagement_rate_change": er_change, "engagement_rate_change_type": er_change_type,
        "average_views_change": views_change, "average_views_change_type": views_change_type,
        "average_likes_change": likes_change, "average_likes_change_type": likes_change_type,
        "average_comments_change": comments_change, "average_comments_change_type": comments_change_type,
    }
    
    # Get the media updates and merge them into the main payload
    media_updates = await process_creator_media(existing_data['id'], handle, update_payload)
    update_payload.update(media_updates)

    print(f"💾 Updating data for @{handle} in Supabase...")
    try:
        supabase.table(TABLE_NAME).update(update_payload).eq("handle", handle).execute()
        print(f"✅ Successfully updated @{handle}.")
    except Exception as e:
        print(f"❌ Supabase update error for @{handle}: {e}")



async def process_new_creator(username):
    """Processes a new TikTok creator and saves their data."""
    print(f"\n{'='*20} PROCESSING NEW @{username} {'='*20}")
    
    influencer_data = process_tiktok_account(username)
    if not influencer_data or influencer_data.get('skipped'):
        return False

    print(f"💾 Saving new creator @{username} to Supabase...")
    try:
        response = supabase.table(TABLE_NAME).upsert(influencer_data, on_conflict='handle').execute()
        
        if response.data:
            creator_id = response.data[0]['id']
            print("⬇️ Starting media download/upload for new creator...")
            await process_creator_media(creator_id, username, influencer_data)
            print(f"✅ Successfully saved @{username}.")
            return True
        else:
            print(f"❌ Supabase save error for @{username}: {response.error or 'No data returned'}")
            return False
    except Exception as e:
        print(f"❌ Supabase save error for @{username}: {e}")
        return False

# ==================== Main Execution ====================
async def main():
    print("🚀 STARTING TIKTOK RE-SCRAPER SCRIPT 🚀")
    
    # --- PHASE 1: CLEANUP INACTIVE CREATORS (includes rescraping) ---
    await cleanup_inactive_creators()
    
    # --- PHASE 2: ADD NEW CREATORS FROM EXCEL ---
    print("\n" + "="*50)
    print("PHASE 2: ADDING NEW CREATORS FROM EXCEL FILE")
    print("="*50)
    
    # Get updated list of existing creators after cleanup
    existing_creators = get_existing_creators()
    existing_handles = {creator['handle'] for creator in existing_creators}
    
    excel_file = "abc.xlsx"
    if not os.path.exists(excel_file):
        print(f"❌ File '{excel_file}' not found. Skipping Phase 2.")
    else:
        try:
            df = pd.read_excel(excel_file)
            if 'Usernames' not in df.columns:
                print("❌ Excel file must have a 'Usernames' column.")
            else:
                all_usernames = [str(u).strip().lstrip('@') for u in df['Usernames'].dropna()]
                new_usernames = [user for user in all_usernames if user not in existing_handles]
                
                print(f"🔍 Found {len(all_usernames)} total usernames in Excel.")
                print(f"✨ Found {len(new_usernames)} new usernames to process.")
                
                if new_usernames:
                    success_count = 0
                    for username in tqdm(new_usernames, desc="Processing new creators"):
                        if await process_new_creator(username):
                            success_count += 1
                        await asyncio.sleep(1) # Rate limit between new accounts
                    print(f"\nNew Creator Processing Complete: {success_count}/{len(new_usernames)} successful.")
                else:
                    print("✅ No new usernames to add.")
        except Exception as e:
            print(f"❌ Error processing Excel file: {e}")

    print("\n\n🎉 SCRIPT FINISHED! 🎉")

if __name__ == "__main__":
    asyncio.run(main())
