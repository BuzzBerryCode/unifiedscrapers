import google.generativeai as genai
import requests
import re
import pandas as pd
from supabase import create_client, Client
from datetime import datetime, timedelta
import os
from urllib.parse import urlparse
import mimetypes
import asyncio
import traceback
from tqdm import tqdm
import pillow_heif
from PIL import Image
import io
from typing import Optional
import json
import time

# ==============================================================================
# --- INITIALIZATION ---
# ==============================================================================

# Initialize Gemini AI
GEMINI_API_KEY = "AIzaSyBYRd9lJTe1mRgJLhpbp39butQbXDgBBMw"
genai.configure(api_key=GEMINI_API_KEY)
gemini_model = genai.GenerativeModel('gemini-2.0-flash')

# Initialize Supabase client
SUPABASE_URL = "https://unovwhgnwenxbyvpevcz.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InVub3Z3aGdud2VueGJ5dnBldmN6Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MjEzNjU0MCwiZXhwIjoyMDY3NzEyNTQwfQ.bqXWKTR64TRARH2VOrjiQdPnQ7W-8EGJp5RIi7yFmck"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ScrapeCreators API Key
SCRAPECREATORS_API_KEY = "wjhGgI14NjNMUuXA92YWXjojozF2"

# --- Constants ---
BUCKET_NAME = "profile-media"
MAX_RECENT_POSTS = 4
PRESET_TRADING_NICHES = [
    "Forex Trading", "Stock Market", "Options Trading", "Futures Trading",
    "Crypto Trading", "Technical Analysis", "Fundamental Analysis",
    "Trading Education", "Trading Signals Provider", "General Trading", "Non-Trading"
]
PRESET_CRYPTO_TRADING_NICHES = [
    "Altcoins", "DeFi", "NFTs", "Crypto Airdrops", "Web3", "Crypto Trading",
    "Crypto News", "Market Analysis", "Meme Coins"
]


# ==============================================================================
# --- HELPER FUNCTIONS (FROM BOTH SCRIPTS) ---
# ==============================================================================

def is_creator_active(recent_posts, days_threshold=45):
    """
    Check if a creator has posted in the last N days.
    
    Args:
        recent_posts: List of post dictionaries with 'created_at' timestamps
        days_threshold: Number of days to check (default: 45)
    
    Returns:
        bool: True if creator has posted within the threshold, False otherwise
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
        supabase.storage.from_(bucket).upload(
            path=path, file=file_content,
            file_options={"content-type": content_type} if content_type else {}
        )
        return supabase.storage.from_(bucket).get_public_url(path)
    except Exception as e:
        print(f"Error uploading to Supabase storage: {e}")
        return None

def get_file_extension_and_type(url: str) -> tuple:
    """Extract file extension and content type from URL."""
    parsed = urlparse(url)
    path = parsed.path
    ext = os.path.splitext(path)[1]
    if not ext:
        ext = '.mp4' if 'video' in path.lower() else '.jpg'
    content_type, _ = mimetypes.guess_type(url)
    if not content_type:
        content_type = 'video/mp4' if ext == '.mp4' else 'image/jpeg'
    return ext, content_type

def clean_handle(handle: str) -> str:
    """Clean the handle to be used as a folder name."""
    return handle.replace('/', '_').replace('\\', '_').strip()

def extract_hashtags(text):
    if not isinstance(text, str):
        return []
    return re.findall(r"#\w+", text)

def extract_emails(text):
    if not isinstance(text, str):
        return []
    return re.findall(r"\b[\w\.-]+@[\w\.-]+\.\w{2,4}\b", text)

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

def calculate_engagement_rate(likes, comments, followers):
    """Calculate engagement rate percentage"""
    if followers == 0:
        return 0.0
    return round(((likes + comments) / followers) * 100, 2)


# ==============================================================================
# --- MEDIA PROCESSING FUNCTION (COMMON) ---
# ==============================================================================

async def process_creator_media(creator_id: str, handle: str, creator_data: dict):
    """Process media for a single creator. This function is used by both scrapers."""
    clean_handle_name = clean_handle(handle)
    storage_folder = f"{clean_handle_name}/"
    updates = {}
    processed_media = 0

    # Ensure bucket exists
    try:
        supabase.storage.get_bucket(BUCKET_NAME)
    except Exception:
        print(f"Bucket '{BUCKET_NAME}' not found. Creating it...")
        supabase.storage.create_bucket(BUCKET_NAME, public=True)

    # Process profile image
    if creator_data.get("profile_image_url"):
        profile_ext, profile_content_type = get_file_extension_and_type(creator_data["profile_image_url"])
        profile_storage_path = f"{storage_folder}profile{profile_ext}"
        profile_content = download_file(creator_data["profile_image_url"])
        if profile_content:
            new_url = upload_to_supabase_storage(BUCKET_NAME, profile_storage_path, profile_content, profile_content_type)
            if new_url:
                updates["profile_image_url"] = new_url

    # Process recent posts
    for i in range(1, MAX_RECENT_POSTS + 1):
        post_key = f"recent_post_{i}"
        if processed_media >= MAX_RECENT_POSTS:
            break
        post = creator_data.get(post_key)
        if not post or not isinstance(post, dict):
            continue

        post = post.copy()
        # The key for media might be "media_urls" (Insta) or "video_url" (TikTok)
        media_urls = post.get("media_urls") or post.get("video_url")

        if not media_urls:
            continue
        if isinstance(media_urls, str):
            media_urls = [media_urls]
        if not isinstance(media_urls, list):
            continue

        new_media_urls = []
        for media_url in media_urls:
            if processed_media >= MAX_RECENT_POSTS:
                break
            if not (media_url and isinstance(media_url, str) and media_url.startswith("http")):
                continue

            ext, content_type = get_file_extension_and_type(media_url)
            media_storage_path = f"{storage_folder}media_{processed_media + 1}{ext}"
            file_content = download_file(media_url)
            if file_content:
                new_url = upload_to_supabase_storage(BUCKET_NAME, media_storage_path, file_content, content_type)
                if new_url:
                    new_media_urls.append(new_url)
                    processed_media += 1

        if new_media_urls:
            # Update the correct key based on platform
            if "media_urls" in post:
                post["media_urls"] = new_media_urls
            elif "video_url" in post:
                # Keep as a single string if only one URL was processed
                post["video_url"] = new_media_urls[0] if len(new_media_urls) == 1 else new_media_urls
            updates[post_key] = post

    if updates:
        supabase.table("creatordata").update(updates).eq("id", creator_id).execute()
        print(f"✅ Updated {processed_media} media URLs for creator {handle}")


# ==============================================================================
# --- INSTAGRAM SCRAPER LOGIC (UNCHANGED) ---
# ==============================================================================


# --- Niche Definitions ---
PRESET_TRADING_NICHES = [
    "Forex Trading", "Stock Market", "Options Trading", "Futures Trading",
    "Crypto Trading", "Technical Analysis", "Fundamental Analysis",
    "Trading Education", "Trading Signals Provider", "General Trading"
]
PRESET_CRYPTO_NICHES = [
    "Altcoins", "DeFi", "NFTs", "Crypto Airdrops", "Web3",
    "Crypto Trading", "Crypto News", "Market Analysis", "Meme Coins", "General Crypto"
]
PRESET_FINANCE_NICHES = [
    "Personal Finance", "Investing", "Stock Market", "Real Estate",
    "Financial News", "Budgeting", "Credit & Debt", "Retirement Planning", "General Finance"
]


# --- AI Helper Functions ---

def safe_gemini_call(prompt: str, timeout_seconds: int = 30, default_response: str = ""):
    """Helper function to make Gemini API calls with timeout protection."""
    import threading
    
    result = None
    exception = None
    
    def ai_call():
        nonlocal result, exception
        try:
            response = gemini_model.generate_content(prompt)
            result = response.text.strip()
        except Exception as e:
            exception = e
    
    thread = threading.Thread(target=ai_call)
    thread.daemon = True
    thread.start()
    thread.join(timeout=timeout_seconds)
    
    if thread.is_alive():
        print(f"⏰ Gemini AI timeout ({timeout_seconds}s limit)")
        return default_response
    
    if exception:
        print(f"⚠️ Gemini AI error: {exception}")
        return default_response
        
    return result if result is not None else default_response

def is_niche_influencer(niche_name: str, criteria: str, bio: str, username: str, display_name: str = "") -> bool:
    """Generic Gemini AI function to classify a user's primary niche."""
    print(f"🧠 Checking if @{username} is a {niche_name} influencer...")
    prompt = f'Analyze the social media username, display name, and bio. Is this user a {niche_name}-related influencer (mentions {criteria})? Respond with ONLY "Yes" or "No".\nUSERNAME: {username}\nDISPLAY NAME: {display_name}\nBIO: {bio}'
    response_text = safe_gemini_call(prompt, timeout_seconds=30, default_response="No")
    return "yes" in response_text.lower()

def predict_secondary_niche(primary_niche: str, niche_list: list, bio: str, hashtags: list, tagged_users: list) -> str:
    """Generic Gemini AI function to predict a secondary niche."""
    print(f"🔮 Predicting secondary {primary_niche} niche...")
    prompt = f'Analyze this social media account and classify it into ONE specific niche from the provided list. Base your decision on the bio, hashtags, and tagged users.\n\nPRIMARY NICHE: {primary_niche}\nPRESET OPTIONS: {niche_list}\n\nBIO: "{bio}"\nHASHTAGS: {list(set(hashtags))}\nTAGGED USERS: {list(set(tagged_users))}\n\nInstructions:\n- Return ONLY the most appropriate niche name from the preset list.\n- If no specific niche fits well, return "General {primary_niche}".'
    response_text = safe_gemini_call(prompt, timeout_seconds=30, default_response=f"General {primary_niche}")
    return response_text if response_text in niche_list else f"General {primary_niche}"


def is_trading_influencer(bio: str, all_captions: list = [], all_hashtags: list = [], username: str = "") -> bool:
    print("🧠 Determining if user is a trading influencer...")
    prompt = f"""
    You are an AI that classifies social media users. Analyze the following Instagram username and bio.
    Decide if the user is likely a **trading-related influencer**.
    CRITERIA:
    - If they mention trading, stocks, forex, options, crypto trading, chart analysis, technical indicators, signals, PnL, or financial markets in general — classify as a trading influencer.
    - Only use the username and bio for this decision.
    Respond with ONLY one word: "Yes" or "No".
    USERNAME: {username}
    BIO: {bio}
    """
    response_text = safe_gemini_call(prompt, timeout_seconds=30, default_response="No")
    decision = "yes" in response_text.lower()
    print(f"🤖 AI preliminary classification: {'Trading Influencer' if decision else 'Not a Trading Influencer'}")
    return decision

def predict_secondary_niche_instagram(all_hashtags, bio, all_tagged_users):
    print("🔮 Predicting secondary trading niche for Instagram...")
    unique_hashtags = list(set(all_hashtags))
    unique_tagged_users = list(set(all_tagged_users))
    prompt = f"""
    You are an AI system that classifies Instagram accounts based on their trading-related content.
    Your task is to predict the **most appropriate** niche for the user within the trading space.
    Analyze the following:
    1. Bio content
    2. All hashtags used
    3. All users/brands tagged in posts
    Choose **one niche** from the list below that best describes this user:
    {PRESET_TRADING_NICHES}
    BIO: "{bio}"
    HASHTAGS: {unique_hashtags}
    TAGGED USERS: {unique_tagged_users}
    Instructions:
    - Only return ONE niche from the preset list (exact wording).
    - Choose based on the dominant pattern.
    - If trading-related but no clear match, return "General Trading".
    - If not trading-related at all, return "Non-Trading".
    """
    response_text = safe_gemini_call(prompt, timeout_seconds=30, default_response="General Trading")
    print(f"✅ Predicted trading niche: {response_text}")
    return response_text if response_text in PRESET_TRADING_NICHES else "General Trading"

def predict_freeform_location_instagram(all_captions, bio, all_locations):
    print("🌍 Predicting location for Instagram...")
    location_text = "\n".join([f"- {loc['name']} (Address: {loc.get('address', 'N/A')}, City: {loc.get('city', 'N/A')}" for loc in all_locations if loc is not None]) if all_locations else "No location tags in posts"
    combined_captions = "\n".join([f"Post {i+1}: {caption}" for i, caption in enumerate(all_captions)])
    prompt = f"""
        You are an AI agent that specializes in user profiling. Based on the following Instagram bio,
        all post captions, and location tags from their posts, predict the most likely location
        (city and country) where the user is based. Be specific and follow the priority order below:
        PRIORITY ORDER:
        1. Highest priority: Location explicitly mentioned in the BIO.
        2. If not found in bio, use LOCATION TAGS from posts (most frequent or recent).
        3. If not found in location tags, use cues from POST CAPTIONS (e.g., place names, cultural references).
        4. If no location is clearly identifiable, return "Global".
        BIO:
        {bio}
        ALL POST CAPTIONS:
        {combined_captions}
        LOCATION TAGS FROM POSTS:
        {location_text}
        Return ONLY the most relevant location in "City, Country" format. No explanations.
        Example: "Paris, France" or "Global" if uncertain.
    """
    response_text = safe_gemini_call(prompt, timeout_seconds=30, default_response="Global")
    print(f"✅ Predicted location: {response_text}")
    return response_text

def get_bio_urls(user_data):
    bio_urls = []
    if user_data.get("bio_links"):
        for link in user_data["bio_links"]:
            if link.get("url"):
                bio_urls.append(link["url"])
    if user_data.get("external_url"):
        bio_urls.append(user_data["external_url"])
    return bio_urls


def predict_freeform_location_tiktok(all_captions, bio, region):
    """Predicts location for a TikTok user using Gemini AI."""
    print("🌍 Predicting location for TikTok...")
    
    # Combine the first few captions to provide context to the AI
    combined_captions = "\n".join(all_captions[:5])
    
    prompt = f"""
    You are an AI that profiles social media users. Predict the most likely location 
    (City, Country) for the user based on the data below.

    Follow this priority:
    1.  The user's self-selected Profile Region (highest priority).
    2.  Any location mentioned in their Bio.
    3.  Hints from their recent Post Captions.
    4.  If no location can be determined, return "Global".

    DATA:
    - Profile Region: "{region}"
    - Bio: "{bio}"
    - Post Captions: "{combined_captions}"

    RESPONSE FORMAT:
    Return ONLY the location (e.g., "Dubai, UAE" or "Global"). Do not add any explanation.
    """
    return safe_gemini_call(prompt, timeout_seconds=30, default_response="Global")



def process_instagram_user(username_input):
    """Process a single Instagram username with multi-niche validation."""
    username_input = username_input.strip().lstrip("@")
    print(f"\n{'='*50}")
    print(f"🔄 Processing Instagram: @{username_input}")
    print(f"{'='*50}")
    
    print("\n📡 Fetching profile data from ScrapeCreators API...")
    profile_url = f"https://api.scrapecreators.com/v1/instagram/profile?handle={username_input}"
    headers = {"x-api-key": SCRAPECREATORS_API_KEY}
    
    # Enhanced retry logic with better error handling
    max_retries = 5  # Increased retries
    retry_delay = 1
    
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                wait_time = min(retry_delay * (2 ** (attempt - 1)), 30)  # Cap at 30s
                print(f"   🔄 Retry {attempt + 1}/{max_retries} for @{username_input} after {wait_time}s...")
                time.sleep(wait_time)
            
            response = requests.get(profile_url, headers=headers, timeout=45)  # Increased timeout
            
            if response.status_code == 200:
                break  # Success
            elif response.status_code == 429:
                print(f"⏳ Rate limited for @{username_input}, waiting 90s...")
                time.sleep(90)  # Longer wait for rate limits
                continue
            elif response.status_code == 404:
                print(f"👻 Profile not found for @{username_input}")
                return {"error": "not_found", "message": "Profile not found"}
            elif response.status_code in [500, 502, 503, 504, 520, 521, 522, 523, 524]:
                print(f"🔄 Server error {response.status_code} for @{username_input}, retrying...")
                time.sleep(5)  # Brief pause for server errors
                continue
            elif response.status_code == 403:
                print(f"🔒 Access forbidden for @{username_input} (private/blocked)")
                return {"error": "access_denied", "message": "Profile is private or access denied"}
            else:
                print(f"❌ Unexpected status code {response.status_code} for @{username_input}")
                if attempt == max_retries - 1:
                    return {"error": "api_error", "message": f"API error: {response.status_code}"}
                continue
                
        except requests.exceptions.Timeout:
            print(f"⏰ Request timeout for @{username_input} (attempt {attempt + 1})")
            if attempt == max_retries - 1:
                return {"error": "timeout", "message": "Request timed out after multiple attempts"}
            continue
        except requests.exceptions.ConnectionError:
            print(f"🌐 Connection error for @{username_input} (attempt {attempt + 1})")
            if attempt == max_retries - 1:
                return {"error": "connection_error", "message": "Connection failed after multiple attempts"}
            time.sleep(5)  # Wait before retrying connection errors
            continue
        except requests.RequestException as e:
            print(f"❌ Profile API request failed (attempt {attempt + 1}): {e}")
            if attempt == max_retries - 1:
                return {"error": "api_error", "message": f"Profile API request failed after {max_retries} attempts: {str(e)}"}
            continue
    else:
        print(f"❌ All {max_retries} attempts failed for @{username_input}")
        return {"error": "api_error", "message": f"Profile API failed after {max_retries} attempts"}

    try:
        response_data = response.json()
        
        # Better error handling for missing data structure
        if "data" not in response_data:
            print(f"❌ API response missing 'data' key for @{username_input}")
            print(f"   Response keys: {list(response_data.keys())}")
            return {"error": "api_error", "message": "API response missing 'data' key"}
        
        if "user" not in response_data["data"]:
            print(f"❌ API response missing 'user' key for @{username_input}")
            print(f"   Data keys: {list(response_data['data'].keys())}")
            return {"error": "api_error", "message": "API response missing 'user' key"}
        
        data = response_data["data"]["user"]
        full_name = data.get("full_name")
        bio = data.get("biography")
        avatar_url = data.get("profile_pic_url_hd")
        followers = data.get("edge_followed_by", {}).get("count", 0)
        
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON response for @{username_input}: {e}")
        return {"error": "api_error", "message": f"Invalid JSON response: {str(e)}"}
    except Exception as e:
        print(f"❌ Error parsing ScrapeCreators profile data for @{username_input}: {str(e)}")
        print(f"   Response content: {response.text[:500]}...")
        return {"error": "api_error", "message": f"Data parsing error: {str(e)}"}

    if followers < 10_000 or followers > 350_000:
        print(f"🚫 Skipping: Follower count {followers} not in 10k–350k range.")
        return {"error": "filtered", "message": f"Follower count {followers:,} not in 10k-350k range"}
    
    # --- START: Multi-Niche Classification Logic ---
    primary_niche = None
    if is_niche_influencer("Crypto", "crypto, bitcoin, blockchain, NFTs, DeFi", bio, username_input, full_name):
        primary_niche = "Crypto"
    elif is_niche_influencer("Trading", "trading, stocks, forex, options, signals", bio, username_input, full_name):
        primary_niche = "Trading"
    elif is_niche_influencer("Finance", "investing, budgeting, saving, credit", bio, username_input, full_name):
        primary_niche = "Finance"

    if not primary_niche:
        print(f"🚫 Skipping @{username_input}: Not a Crypto, Trading, or Finance influencer.")
        return {"error": "filtered", "message": "Not a Crypto, Trading, or Finance influencer"}
    print(f"✅ Primary Niche Identified: {primary_niche}")
    # --- END: Multi-Niche Classification Logic ---

    print("\n📡 Fetching post data from ScrapeCreators API...")
    scrapecreators_url = f"https://api.scrapecreators.com/v2/instagram/user/posts?handle={username_input}"
    
    # Enhanced retry logic for posts API
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                wait_time = min(retry_delay * (2 ** (attempt - 1)), 30)
                print(f"   🔄 Posts retry {attempt + 1}/{max_retries} for @{username_input} after {wait_time}s...")
                time.sleep(wait_time)
            
            posts_response = requests.get(scrapecreators_url, headers={"x-api-key": SCRAPECREATORS_API_KEY}, timeout=45)
            
            if posts_response.status_code == 200:
                break  # Success
            elif posts_response.status_code == 429:
                print(f"⏳ Rate limited on posts for @{username_input}, waiting 90s...")
                time.sleep(90)
                continue
            elif posts_response.status_code == 404:
                print(f"👻 No posts found for @{username_input}")
                return {"error": "no_posts", "message": "No posts found"}
            elif posts_response.status_code in [500, 502, 503, 504, 520, 521, 522, 523, 524]:
                print(f"🔄 Posts server error {posts_response.status_code} for @{username_input}, retrying...")
                time.sleep(5)
                continue
            elif posts_response.status_code == 403:
                print(f"🔒 Posts access forbidden for @{username_input}")
                return {"error": "access_denied", "message": "Posts access denied"}
            else:
                print(f"❌ Unexpected posts status code {posts_response.status_code} for @{username_input}")
                if attempt == max_retries - 1:
                    return {"error": "api_error", "message": f"Posts API error: {posts_response.status_code}"}
                continue
                
        except requests.exceptions.Timeout:
            print(f"⏰ Posts request timeout for @{username_input} (attempt {attempt + 1})")
            if attempt == max_retries - 1:
                return {"error": "timeout", "message": "Posts request timed out after multiple attempts"}
            continue
        except requests.exceptions.ConnectionError:
            print(f"🌐 Posts connection error for @{username_input} (attempt {attempt + 1})")
            if attempt == max_retries - 1:
                return {"error": "connection_error", "message": "Posts connection failed after multiple attempts"}
            time.sleep(5)
            continue
        except requests.RequestException as e:
            print(f"❌ Posts API request failed (attempt {attempt + 1}): {e}")
            if attempt == max_retries - 1:
                return {"error": "api_error", "message": f"Posts API request failed after {max_retries} attempts: {str(e)}"}
            continue
    else:
        print(f"❌ All {max_retries} posts attempts failed for @{username_input}")
        return {"error": "api_error", "message": f"Posts API failed after {max_retries} attempts"}

    try:
        posts_data = posts_response.json().get("items", [])
        likes_list, comments_list, views_list = [], [], []
        all_hashtags, tagged_users, recent_posts, past_ad_placements, all_captions, all_locations, all_tagged_users_in_posts = [], [], [], [], [], [], []

        for idx, post in enumerate(posts_data[:12], start=1):
            caption_obj = post.get("caption", {})
            caption = caption_obj.get("text", "") if isinstance(caption_obj, dict) else str(caption_obj)
            all_captions.append(caption)
            usertags = post.get("usertags", {}).get("in", [])
            brand_usernames = [tag.get("user", {}).get("username") for tag in usertags if tag.get("user", {}).get("username")]
            all_tagged_users_in_posts.extend(brand_usernames)
        
            if post.get("is_paid_partnership", False) and brand_usernames:
                past_ad_placements.extend(brand_usernames)
            
            like_hidden = post.get("like_and_view_counts_disabled", True)
            likes = post.get("like_count") if not like_hidden else None
            if not like_hidden:
                likes_list.append(likes or 0)

            comments = post.get("comment_count")
            comments_list.append(comments or 0)
            
            play_count = post.get("play_count")
            if play_count is not None:
                views_list.append(play_count)

            is_video = post.get("media_type") in [2, 8]
            is_carousel = post.get("carousel_media_count", 0) > 0
            
            location = post.get("location", {})
            location_info = { "name": location.get("name"), "id": location.get("id"), "slug": location.get("slug"), "address": location.get("address"), "city": location.get("city") } if location.get("name") else None
            if location_info:
                all_locations.append(location_info)
            
            # Extract Instagram posting time
            taken_at = post.get("taken_at")
            created_at = None
            
            if taken_at and taken_at != 0:
                try:
                    # Handle both seconds and milliseconds
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
                            media_urls.append(first_media.getk("display_uri"))

            else:
                if is_video:
                    media_urls = post.get("image_versions2", {}) \
                        .get("additional_candidates", {}) \
                        .get("igtv_first_frame", {}) \
                        .get("url")
                    
            
            hashtags = extract_hashtags(caption)
            all_hashtags.extend(hashtags)
            tagged_users.extend(brand_usernames)

            recent_posts.append({
                "caption": caption, "likes": likes, "comments": comments, "views": play_count, "is_video": is_video,
                "is_carousel": is_carousel, "media_urls": media_urls, "hashtags": hashtags, "brand_tags": brand_usernames,
                "is_paid_partnership": post.get("is_paid_partnership", False), "like_hidden": like_hidden, "location": location_info,
                "created_at": created_at
            })


        likes_for_calc = [l for l in likes_list[3:12] if l is not None]
        comments_for_calc = [c for c in comments_list[3:12] if c is not None]
        views_for_calc = [v for v in views_list[3:12] if v is not None]


        avg_likes = sum(likes_for_calc) // len(likes_for_calc) if likes_for_calc else 0
        avg_comments = sum(comments_for_calc) // len(comments_for_calc) if comments_for_calc else 0
        avg_views = sum(views_for_calc) // len(views_for_calc) if views_for_calc else 0
        engagement_rate = calculate_engagement_rate(sum(likes_for_calc), sum(comments_for_calc), followers)
        
        # --- START: Dynamic Secondary Niche Prediction ---
        niche_map = {
            "Crypto": PRESET_CRYPTO_NICHES,
            "Trading": PRESET_TRADING_NICHES,
            "Finance": PRESET_FINANCE_NICHES
        }
        secondary_niche = predict_secondary_niche(primary_niche, niche_map[primary_niche], bio, all_hashtags, all_tagged_users_in_posts)
        # --- END: Dynamic Secondary Niche Prediction ---
        
        location = predict_freeform_location_instagram(bio, all_captions, all_locations)

        influencer_data = {
            "handle": username_input, "display_name": full_name or "", "profile_url": f"https://instagram.com/{username_input}",
            "profile_image_url": avatar_url or "", "bio": bio or "", "platform": "Instagram",
            "primary_niche": primary_niche, # Changed from hardcoded "Trading"
            "secondary_niche": secondary_niche,
            "brand_tags": list(set(tagged_users)) if tagged_users else [], "location": location,
            "followers_count": followers or 0, "bio_links": get_bio_urls(data), "average_views": avg_views or 0,
            "average_likes": avg_likes or 0, "average_comments": avg_comments or 0,
            "engagement_rate": float(engagement_rate) if engagement_rate else 0.0,
            "hashtags": list(set(all_hashtags)) if all_hashtags else [],
            "email": (extract_emails(bio) or [None])[0],
            "past_ad_placements": list(set(past_ad_placements)) if past_ad_placements else []
        }

        for i in range(min(12, len(recent_posts))):
            influencer_data[f"recent_post_{i+1}"] = recent_posts[i]

        # Check if creator has posted in the last 45 days
        print("\n📅 Checking creator activity...")
        if not is_creator_active(recent_posts, days_threshold=45):
            print(f"🚫 Skipping @{username_input}: No posts in the last 45 days")
            return None

        print("\n💾 Saving Instagram data to Supabase...")
        insert_response = supabase.table("creatordata").insert(influencer_data).execute()
        
        if hasattr(insert_response, 'error') and insert_response.error:
            print(f"❌ Error inserting into Supabase: {insert_response.error}")
            return None
        
        print("\n✅ Successfully saved influencer data to Supabase!")
        if hasattr(insert_response, 'data') and insert_response.data:
            creator_id = insert_response.data[0].get('id')
            if creator_id:
                print("\n⬇️ Starting media download and upload process...")
                return {'creator_id': creator_id, 'data': influencer_data}
        return influencer_data
        
    except Exception as e:
        print("❌ Error parsing ScrapeCreators post data:", e)
        traceback.print_exc()
        return None


# ==============================================================================
# --- TIKTOK SCRAPER LOGIC (UNCHANGED) ---
# ==============================================================================

def process_tiktok_account(username, api_key):
    """Process a single TikTok account with multi-niche validation."""
    print(f"\n{'='*50}")
    print(f"🔄 Processing TikTok: @{username}")
    print(f"{'='*50}")

    api_url = f"https://api.scrapecreators.com/v3/tiktok/profile/videos?handle={username}"
    headers = {"x-api-key": api_key}
    
    # Add same retry logic for TikTok API
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                wait_time = retry_delay * (2 ** (attempt - 1))
                print(f"   🔄 TikTok retry {attempt + 1}/{max_retries} for @{username} after {wait_time}s...")
                time.sleep(wait_time)
            
            response = requests.get(api_url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                break  # Success
            elif response.status_code == 429:
                print(f"⏳ Rate limited on TikTok for @{username}, waiting longer...")
                time.sleep(60)
                continue
            elif response.status_code in [500, 502, 503, 504, 520]:
                print(f"🔄 TikTok server error {response.status_code} for @{username}, retrying...")
                continue
            else:
                print(f"❌ Failed to fetch TikTok data: {response.status_code}")
                return {"error": "api_error", "message": f"TikTok API error: {response.status_code}"}
                
        except requests.RequestException as e:
            print(f"❌ TikTok API request failed (attempt {attempt + 1}): {e}")
            if attempt == max_retries - 1:
                return {"error": "api_error", "message": f"TikTok API request failed after {max_retries} attempts: {str(e)}"}
            continue
    else:
        print(f"❌ All {max_retries} TikTok attempts failed for @{username}")
        return {"error": "api_error", "message": f"TikTok API failed after {max_retries} attempts"}

    posts = safe_get(response.json(), ['aweme_list'], [])
    if not posts:
        print("❌ No posts found for this account")
        return None

    user_info = safe_get(posts[0], ['author'], {})
    bio = safe_get(user_info, ['signature'], '')
    followers = user_info.get('follower_count', 0)

    if not (10000 <= followers <= 350000):
        print(f"❌ Skipped: Follower count {followers} outside target range")
        return None

    # --- START: Multi-Niche Classification Logic ---
    primary_niche = None
    display_name = user_info.get('nickname', '')
    if is_niche_influencer("Crypto", "crypto, bitcoin, blockchain, NFTs, DeFi", bio, username, display_name):
        primary_niche = "Crypto"
    elif is_niche_influencer("Trading", "trading, stocks, forex, options, signals", bio, username, display_name):
        primary_niche = "Trading"
    elif is_niche_influencer("Finance", "investing, budgeting, saving, credit", bio, username, display_name):
        primary_niche = "Finance"

    if not primary_niche:
        print(f"🚫 Skipping @{username}: Not a Crypto, Trading, or Finance influencer.")
        return None
    print(f"✅ Primary Niche Identified: {primary_niche}")
    # --- END: Multi-Niche Classification Logic ---

    likes_list, comments_list, views_list, all_hashtags, all_tagged_users, recent_posts, past_ad_placements, all_captions = [], [], [], [], [], [], [], []

    for post in posts[:12]:
        caption = post.get('desc', '')
        stats = post.get('statistics', {})
        likes = stats.get('digg_count', 0)
        comments = stats.get('comment_count', 0)
        views = stats.get('play_count', 0)
        hashtags = [tag.lower() for tag in re.findall(r"#(\w+)", caption or "")]
        tagged_users = re.findall(r"@(\w+)", caption or "")
        video_url = post.get('video', {}).get('ai_dynamic_cover', {}).get('url_list', [''])[0]

        # Extract TikTok posting time
        created_at = None
        create_time = post.get('create_time')
        if create_time:
            try:
                created_at = datetime.fromtimestamp(create_time).isoformat()
            except (ValueError, TypeError):
                created_at = None

        if post.get("commerce_info", {}).get("bc_label_test_text", "") == "Paid partnership" and tagged_users:
            past_ad_placements.extend(tagged_users)

        all_hashtags.extend(hashtags)
        all_tagged_users.extend(tagged_users)
        likes_list.append(likes)
        comments_list.append(comments)
        views_list.append(views)
        all_captions.append(caption)

        recent_posts.append({
            "caption": caption, "likes": likes, "comments": comments, "views": views,
            "hashtags": hashtags, "tagged_users": tagged_users, "video_url": video_url,
            "is_paid_partnership": post.get("commerce_info", {}).get("bc_label_test_text", "") == "Paid partnership",
            "is_video": True, "is_carousel": False, "created_at": created_at
        })

    # --- Calculation based on last 9 of 12 posts ---
    likes_for_calc = likes_list[3:12]
    comments_for_calc = comments_list[3:12]
    views_for_calc = views_list[3:12]

    avg_likes = sum(likes_for_calc) // len(likes_for_calc) if likes_for_calc else 0
    avg_comments = sum(comments_for_calc) // len(comments_for_calc) if comments_for_calc else 0
    avg_views = sum(views_for_calc) // len(views_for_calc) if views_for_calc else 0
    engagement_rate = calculate_engagement_rate(sum(likes_for_calc), sum(comments_for_calc), followers)


    
    # --- START: Dynamic Secondary Niche Prediction ---
    niche_map = {
        # Note: The original TikTok file used PRESET_CRYPTO_TRADING_NICHES, we use the specific ones now
        "Crypto": PRESET_CRYPTO_NICHES,
        "Trading": PRESET_TRADING_NICHES,
        "Finance": PRESET_FINANCE_NICHES
    }
    secondary_niche = predict_secondary_niche(primary_niche, niche_map[primary_niche], bio, all_hashtags, all_tagged_users)
    # --- END: Dynamic Secondary Niche Prediction ---

    profile_region = posts[0].get('region', "Global")
    location = predict_freeform_location_tiktok(all_captions, bio, profile_region) # Changed to use generic location prediction

    influencer_data = {
        "handle": user_info.get('unique_id', ''),
        "display_name": user_info.get('nickname', ''),
        "profile_url": f"https://www.tiktok.com/@{user_info.get('unique_id', '')}",
        "profile_image_url": user_info.get('avatar_thumb', {}).get('url_list', [''])[0],
        "bio": bio,
        "platform": "TikTok",
        "primary_niche": primary_niche, # Changed from hardcoded "Crypto"
        "secondary_niche": secondary_niche,
        "brand_tags": list(set(all_tagged_users)),
        "location": location,
        "followers_count": followers,
        "average_views": avg_views,
        "average_likes": avg_likes,
        "average_comments": avg_comments,
        "engagement_rate": float(engagement_rate),
        "hashtags": list(set(all_hashtags)),
        "email": (extract_emails(bio) or [None])[0],
        "past_ad_placements": list(set(past_ad_placements)),
        "bio_links": "",
    }

    for i in range(min(12, len(recent_posts))):
        influencer_data[f"recent_post_{i+1}"] = recent_posts[i]

    # Check if creator has posted in the last 45 days
    print("\n📅 Checking creator activity...")
    if not is_creator_active(recent_posts, days_threshold=45):
        print(f"🚫 Skipping @{username}: No posts in the last 45 days")
        return None

    return influencer_data



# ==============================================================================
# --- MAIN EXECUTION LOGIC ---
# ==============================================================================

async def main():
    """Main function to read Excel and trigger platform-specific scrapers."""
    excel_file = "CreatorList.xlsx"
    if not os.path.exists(excel_file):
        print(f"❌ Error: File '{excel_file}' not found. Please create it with 'Usernames' and 'Platform' columns.")
        return

    try:
        df = pd.read_excel(excel_file)
        if 'Usernames' not in df.columns or 'Platform' not in df.columns:
            print("❌ Error: Excel file must have 'Usernames' and 'Platform' columns.")
            return
    except Exception as e:
        print(f"❌ Error reading Excel file: {e}")
        return

    usernames_to_process = df.dropna(subset=['Usernames', 'Platform']).to_dict('records')
    print(f"🔍 Found {len(usernames_to_process)} creators to process from {excel_file}")
    
    success_count = 0
    failure_count = 0

    for record in tqdm(usernames_to_process, desc="Processing creators"):
        username = str(record['Usernames']).strip()
        platform = str(record['Platform']).strip().lower()
        
        result = None
        if platform == 'instagram':
            # Instagram processing is synchronous
            result = process_instagram_user(username)
            if result and isinstance(result, dict) and 'creator_id' in result:
                # The function returns a dict with id and data needed for the async media processing
                await process_creator_media(result['creator_id'], username, result['data'])
                success_count += 1
            elif result:
                success_count += 1
            else:
                failure_count += 1

        elif platform == 'tiktok':
            # TikTok processing is synchronous but the DB insert and media part is best handled here
            influencer_data = process_tiktok_account(username, SCRAPECREATORS_API_KEY)
            if influencer_data:
                try:
                    response = supabase.table("creatordata").insert(influencer_data).execute()
                    if hasattr(response, 'error') and response.error:
                        print(f"❌ Supabase error for @{username}: {response.error}")
                        failure_count += 1
                    else:
                        success_count += 1
                        if hasattr(response, 'data') and response.data:
                            creator_id = response.data[0].get('id')
                            if creator_id:
                                await process_creator_media(creator_id, username, influencer_data)
                except Exception as e:
                    print(f"❌ Supabase error for @{username}: {e}")
                    failure_count += 1
            else:
                failure_count += 1
            await asyncio.sleep(1) # Rate limit between TikTok API calls

        else:
            print(f"⚠️ Invalid platform '{record['Platform']}' for user {username}. Please use 'Instagram' or 'TikTok'.")
            failure_count += 1

    print("\n✅ Processing complete!")
    print(f"Total processed: {len(usernames_to_process)}")
    print(f"Successful: {success_count}")
    print(f"Failed or Skipped: {failure_count}")

if __name__ == "__main__":
    asyncio.run(main())