import google.generativeai as genai
import requests
import re
import pandas as pd
from supabase import create_client, Client
from datetime import datetime, timedelta
import os
from urllib.parse import urlparse
import mimetypes
import statistics

# Initialize Gemini AI
GEMINI_API_KEY = "AIzaSyBYRd9lJTe1mRgJLhpbp39butQbXDgBBMw"
genai.configure(api_key=GEMINI_API_KEY)
gemini_model = genai.GenerativeModel('gemini-1.5-flash')

# Initialize Supabase client
SUPABASE_URL = "https://unovwhgnwenxbyvpevcz.supabase.co" 
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InVub3Z3aGdud2VueGJ5dnBldmN6Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MjEzNjU0MCwiZXhwIjoyMDY3NzEyNTQwfQ.bqXWKTR64TRARH2VOrjiQdPnQ7W-8EGJp5RIi7yFmck" # Replace with your actual service key
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BUCKET_NAME = "profile-media"
MAX_RECENT_POSTS = 4

# ==================== Helper Functions ====================

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
    print(f"📅 Checking {len(recent_posts)} posts for activity (threshold: {days_threshold} days)")
    
    active_found = False
    for i, post in enumerate(recent_posts, 1):
        if post.get('created_at'):
            try:
                # Parse the ISO format timestamp
                post_date = datetime.fromisoformat(post['created_at'].replace('Z', '+00:00'))
                
                # Calculate the difference
                days_since_post = (current_time - post_date).days
                
                # Display each post's date
                status = "✅ ACTIVE" if days_since_post <= days_threshold else "❌ OLD"
                print(f"   Post {i:2d}: {post_date.strftime('%Y-%m-%d')} ({days_since_post:3d} days ago) - {status}")
                
                # If ANY post is within the threshold, creator is active
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

    # --- Prepare Current Data ---
    views_now = [
        post.get('views', 0) for i in range(1, 13)
        if (post := new_data.get(f'recent_post_{i}')) and post.get('views') is not None
    ]
    if not views_now: views_now = [0] # Avoid errors on empty lists

    median_views_now = get_median(views_now)
    std_dev_views = get_standard_deviation(views_now)
    followers_now = new_data.get('followers_count', 0)
    avg_likes_now = new_data.get('average_likes', {}).get('avg_value', 0)
    avg_comments_now = new_data.get('average_comments', 0)

    # --- Prepare Historical Data (from last week's scrape) ---
    views_last_week = [
        post.get('views', 0) for i in range(1, 13)
        if (post := existing_data.get(f'recent_post_{i}')) and post.get('views') is not None
    ]
    if not views_last_week: views_last_week = [0]

    median_views_last_week = get_median(views_last_week)
    followers_last_week = existing_data.get('followers_count', 0)
    
    old_likes_data = existing_data.get('average_likes')
    avg_likes_last_week = old_likes_data.get('avg_value') if isinstance(old_likes_data, dict) else old_likes_data or 0
    
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


# ==================== Media Downloading Functions ====================
def download_file(url: str) -> bytes:
    """Download file from URL and return its content as bytes."""
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        return response.content
    except requests.RequestException as e:
        print(f"Error downloading {url}: {e}")
        return None

def upload_to_supabase_storage(bucket: str, path: str, file_content: bytes, content_type: str = None) -> str:
    """Upload file to Supabase storage and return public URL."""
    try:
        # Use upsert to avoid errors on re-uploading the same file
        res = supabase.storage.from_(bucket).upload(
            path=path,
            file=file_content,
            file_options={"content-type": content_type, "upsert": "true"}
        )
        url = supabase.storage.from_(bucket).get_public_url(path)
        return url
    except Exception as e:
        # If upload fails, try to get the public URL of the existing file
        print(f"Upload failed for {path}, attempting to get existing URL. Error: {e}")
        try:
            return supabase.storage.from_(bucket).get_public_url(path)
        except Exception as e2:
            print(f"Could not get public URL for {path}. Error: {e2}")
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

def process_creator_media(creator_id: str, handle: str, creator_data: dict):
    """Process media for a single creator, downloading and uploading to Supabase Storage."""
    clean_handle_name = clean_handle(handle)
    storage_folder = f"{clean_handle_name}/"
    
    updates = {}
    
    # Process profile image
    if creator_data.get("profile_image_url"):
        profile_ext, profile_content_type = get_file_extension_and_type(creator_data["profile_image_url"])
        profile_storage_path = f"{storage_folder}profile{profile_ext}"
        
        profile_content = download_file(creator_data["profile_image_url"])
        if profile_content:
            new_url = upload_to_supabase_storage(
                BUCKET_NAME, profile_storage_path, profile_content, profile_content_type
            )
            if new_url:
                updates["profile_image_url"] = new_url

    # Process recent posts (up to MAX_RECENT_POSTS)
    processed_media_count = 0
    for i in range(1, 13): # Check all 12 possible posts
        post_key = f"recent_post_{i}"
        if processed_media_count >= MAX_RECENT_POSTS:
            break

        post = creator_data.get(post_key)
        if not post or not isinstance(post, dict):
            continue

        media_urls = post.get("media_urls")
        if not media_urls:
            continue
            
        if isinstance(media_urls, str):
            media_urls = [media_urls]

        new_media_urls = []
        for media_url in media_urls:
            if not media_url or not isinstance(media_url, str) or not media_url.startswith("http"):
                continue

            try:
                ext, content_type = get_file_extension_and_type(media_url)
                # Use a unique name for each media file to prevent overwrites
                media_filename = os.path.basename(urlparse(media_url).path)
                if not media_filename:
                    media_filename = f"media_{processed_media_count + 1}{ext}"
                
                media_storage_path = f"{storage_folder}{media_filename}"
                file_content = download_file(media_url)

                if file_content:
                    new_url = upload_to_supabase_storage(
                        BUCKET_NAME, media_storage_path, file_content, content_type
                    )
                    if new_url:
                        new_media_urls.append(new_url)
                        processed_media_count += 1
            except Exception as e:
                print(f"❌ Error processing media {media_url}: {e}")

        if new_media_urls:
            post["media_urls"] = new_media_urls
            updates[post_key] = post

    return updates


# ==================== AI & Data Extraction Functions ====================

def is_crypto_influencer(bio: str, all_captions: list = [], all_hashtags: list = [], username: str = "") -> bool:
    """Uses Gemini AI to decide if a user is a crypto influencer."""
    print("🧠 Determining if user is a crypto influencer...")

    prompt = f"""
    You are an AI that classifies social media users. Analyze the following Instagram username and bio.
    Decide if the user is likely a **crypto-related influencer**.

    CRITERIA:
    - If they mention crypto, bitcoin, blockchain, NFTs, DeFi, Web3, coins, trading, airdrops, etc., classify as a crypto influencer.
    - Only use the username and bio for this decision.

    Respond with ONLY one word: "Yes" or "No".

    USERNAME: {username}
    BIO: {bio}
    """

    try:
        response = gemini_model.generate_content(prompt)
        answer = response.text.strip().lower()
        decision = "yes" in answer
        print(f"🤖 AI preliminary classification: {'Crypto Influencer' if decision else 'Not a Crypto Influencer'}")
        return decision
    except Exception as e:
        print(f"⚠️ Error determining influencer type: {e}")
        return False

PRESET_CRYPTO_TRADING_NICHES = [
    "Altcoins", "DeFi",
    "NFTs", "Crypto Airdrops", "Web3", "Crypto Trading",
    "Crypto News","Market Analysis","Meme Coins"
]

def predict_crypto_secondary_niche(all_hashtags, bio, all_tagged_users):
    """Predict secondary niche using Gemini AI (from crypto/trading preset list)"""
    print("🔮 Predicting secondary niche...")
    unique_hashtags = list(set(all_hashtags))
    unique_tagged_users = list(set(all_tagged_users))
    
    prompt = f"""
    Analyze this Instagram account to classify into ONE crypto/trading niche.Do not bias towards a specific niche, try to assign the most appropiate niche to the profile amongst the options not only one niche again and again.
    Consider these factors:
    1. Bio content
    2. All hashtags used
    3. All users/brands tagged in posts
    
    Preset Options: {PRESET_CRYPTO_TRADING_NICHES}
    
    BIO: "{bio}"
    ALL HASHTAGS USED: {unique_hashtags}
    ALL TAGGED USERS/BRANDS: {unique_tagged_users}
    
    Instructions:
    - Return ONLY one niche name from the preset list
    - Focus on the most dominant pattern
    - If crypto-related but no clear match, return "General Crypto"
    - If not crypto-related at all, return "Non-Crypto"
    """
    try:
        response = gemini_model.generate_content(prompt)
        niche = response.text.strip()
        print(f"✅ Predicted niche: {niche}")
        return niche if niche in PRESET_CRYPTO_TRADING_NICHES else "General Crypto"
    except Exception as e:
        print(f"⚠️ Niche prediction failed: {str(e)}")
        return "General Crypto"


def is_trading_influencer(bio: str, username: str = "") -> bool:
    """Uses Gemini AI to decide if a user is a trading influencer."""
    print("🧠 Determining if user is a trading influencer...")
    prompt = f"""
    Analyze the Instagram username and bio to determine if the user is a trading-related influencer.
    Criteria: Mention of trading, stocks, forex, options, crypto, chart analysis, signals, PnL, or financial markets indicates a trading influencer.
    Respond with ONLY "Yes" or "No".

    USERNAME: {username}
    BIO: {bio}
    """
    try:
        response = gemini_model.generate_content(prompt)
        return "yes" in response.text.strip().lower()
    except Exception as e:
        print(f"⚠️ Error in is_trading_influencer: {e}")
        return False

PRESET_TRADING_NICHES = [
    "Forex Trading", "Stock Market", "Options Trading", "Futures Trading",
    "Crypto Trading", "Technical Analysis", "Fundamental Analysis",
    "Trading Education", "Trading Signals Provider", "General Trading", "Non-Trading"
]

def predict_trading_secondary_niche(bio, all_hashtags, all_tagged_users):
    """Predict secondary trading niche using Gemini AI."""
    print("🔮 Predicting secondary trading niche...")
    prompt = f"""
    You are an AI that classifies Instagram trading accounts. Predict the MOST appropriate niche from the list based on the user's bio, hashtags, and tagged users.
    Niches: {PRESET_TRADING_NICHES}

    BIO: "{bio}"
    HASHTAGS: {list(set(all_hashtags))}
    TAGGED USERS: {list(set(all_tagged_users))}

    Instructions:
    - Return ONLY ONE niche from the list.
    - If trading-related but no clear match, return "General Trading".
    - If not trading-related, return "Non-Trading".
    """
    try:
        response = gemini_model.generate_content(prompt)
        niche = response.text.strip()
        return niche if niche in PRESET_TRADING_NICHES else "General Trading"
    except Exception as e:
        print(f"⚠️ Niche prediction failed: {e}")
        return "General Trading"
    

def is_finance_influencer(bio: str, all_captions: list = [], all_hashtags: list = [], username: str = "") -> bool:
    """Uses Gemini AI to decide if a user is a finance influencer."""
    print("🧠 Determining if user is a finance influencer...")

    prompt = f"""
    You are an AI that classifies social media users. Analyze the following Instagram username and bio.
    Decide if the user is likely a **finance-related influencer**.

    CRITERIA:
    - Classify as a finance influencer if they mention personal finance, investing, wealth management, budgeting, financial planning, financial freedom, FIRE, credit, savings, fintech, or similar topics.
    - Only use the username and bio for this decision.

    Respond with ONLY one word: "Yes" or "No".

    USERNAME: {username}
    BIO: {bio}
    """

    try:
        response = gemini_model.generate_content(prompt)
        answer = response.text.strip().lower()
        decision = "yes" in answer
        print(f"🤖 AI preliminary classification: {'Finance Influencer' if decision else 'Not a Finance Influencer'}")
        return decision
    except Exception as e:
        print(f"⚠️ Error determining influencer type: {e}")
        return False



PRESET_FINANCE_NICHES = [
    "Personal Finance",
    "Investing",
    "Wealth Management",
    "Budgeting",
    "Financial Education",
    "Credit and Loans",
    "Tax Planning",
    "Savings and Retirement",
    "Financial Independence (FIRE)",
    "Fintech and Apps",
    "General Finance",
    "Non-Finance"
]



def predict_finance_secondary_niche(all_hashtags, bio, all_tagged_users):
    """Predict secondary finance niche using Gemini AI."""
    print("🔮 Predicting secondary finance niche...")
    unique_hashtags = list(set(all_hashtags))
    unique_tagged_users = list(set(all_tagged_users))
    
    prompt = f"""
    You are an AI system that classifies Instagram accounts based on their finance-related content.

    Your task is to predict the **most appropriate** niche for the user within the finance space.
    Analyze the following:
    1. Bio content
    2. All hashtags used
    3. All users/brands tagged in posts

    Choose **one niche** from the list below that best describes this user:
    {PRESET_FINANCE_NICHES}

    BIO: "{bio}"
    HASHTAGS: {unique_hashtags}
    TAGGED USERS: {unique_tagged_users}

    Instructions:
    - Return ONLY one niche from the preset list (match the exact wording).
    - Choose based on the dominant theme.
    - If finance-related but no clear match, return "General Finance".
    - If not finance-related at all, return "Non-Finance".
    """

    try:
        response = gemini_model.generate_content(prompt)
        niche = response.text.strip()
        print(f"✅ Predicted finance niche: {niche}")
        return niche if niche in PRESET_FINANCE_NICHES else "General Finance"
    except Exception as e:
        print(f"⚠️ Niche prediction failed: {str(e)}")
        return "General Finance"
    





def predict_freeform_location(bio, all_captions, all_locations):
    """Predict location in free-form text using Gemini AI."""
    print("🌍 Predicting location...")
    location_text = "\n".join([f"- {loc.get('name', 'N/A')}" for loc in all_locations if loc])
    prompt = f"""
    You are an AI user profiling agent. Predict the user's location (City, Country) based on their Instagram bio, post captions, and location tags.
    Priority: 1. Bio, 2. Location Tags, 3. Captions.
    If no location is identifiable, return "Global".

    BIO: {bio}
    CAPTIONS: {" ".join(all_captions[:5])}
    LOCATION TAGS: {location_text}

    Return ONLY the location in "City, Country" format or "Global".
    """
    try:
        response = gemini_model.generate_content(prompt)
        return response.text.strip()
    except Exception as e:
        print(f"⚠️ Location prediction failed: {e}")
        return "Global"

def extract_hashtags(text):
    return re.findall(r"#\w+", text or "")

def get_bio_urls(user_data):
    urls = [link.get("url") for link in user_data.get("bio_links", []) if link.get("url")]
    if user_data.get("external_url"):
        urls.append(user_data["external_url"])
    return list(set(urls))

def extract_emails(text):
    return re.findall(r"\b[\w\.-]+@[\w\.-]+\.\w{2,4}\b", text or "")

def scrape_user_data(username):
    """Scrapes profile and post data for a given username and returns a structured dict."""
    print(f"\n📡 Fetching all data for @{username}...")
    
    # --- Profile Data ---
    profile_url = f"https://api.scrapecreators.com/v1/instagram/profile?handle={username}"
    headers = {"x-api-key": "wjhGgI14NjNMUuXA92YWXjojozF2"} # Replace with your key
    profile_response = requests.get(profile_url, headers=headers)
    if profile_response.status_code != 200:
        print(f"❌ Failed to fetch profile data for @{username}: {profile_response.text}")
        return None
    
    profile_data = profile_response.json().get("data", {}).get("user", {})
    if not profile_data:
        print(f"❌ No profile data found for @{username}")
        return None

    # --- Posts Data ---
    posts_url = f"https://api.scrapecreators.com/v2/instagram/user/posts?handle={username}"
    posts_response = requests.get(posts_url, headers=headers)
    posts_data = posts_response.json().get("items", []) if posts_response.status_code == 200 else []

    print(f"✅ Fetched profile and {len(posts_data)} posts for @{username}.")
    
    # --- Process and Structure Data ---
    followers = profile_data.get("edge_followed_by", {}).get("count", 0)
    bio = profile_data.get("biography", "")
    
    # Skip niche validation - keep existing data
    print(f"ℹ️ Skipping niche validation - keeping existing data for @{username}")

    if not (10_000 <= followers <= 350_000):
        print(f"🚫 Skipping @{username}: Follower count {followers} out of range (10k-350k).")
        return {'skipped': True}

    # --- Process Posts ---
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

        all_hashtags.extend(extract_hashtags(caption))
        usertags = [tag.get("user", {}).get("username") for tag in post.get("usertags", {}).get("in", []) if tag.get("user", {}).get("username")]
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
                        # Corrected typo from getk to get
                        media_urls.append(first_media.get("display_uri"))
        else:
            if is_video:
                # This is the corrected part
                video_url = post.get("image_versions2", {}) \
                    .get("additional_candidates", {}) \
                    .get("igtv_first_frame", {}) \
                    .get("url")
                if video_url:
                    # Instead of replacing the list, we create a new list containing the URL
                    media_urls = [video_url]
            else: # Handle single image post
                 image_versions = post.get("image_versions2", {}).get("candidates", [])
                 if image_versions:
                     media_urls.append(image_versions[0].get("url"))
                 else:
                     media_urls.append(post.get("display_uri"))

        recent_posts.append({
            "caption": caption, "likes": post.get("like_count", 0), "comments": post.get("comment_count", 0), "views": post.get("play_count", 0),
            "is_video": is_video, "is_carousel": is_carousel,
            "media_urls": [url for url in media_urls if url],
            "hashtags": extract_hashtags(caption), "brand_tags": usertags,
            "is_paid_partnership": post.get("is_paid_partnership", False),
            "like_hidden": post.get("like_and_view_counts_disabled", False),
            "location": post.get("location"),
            "created_at": created_at
        })

    # --- Calculations based on the last 9 of the 12 posts ---
    print("📊 Calculating metrics based on the last 9 of the 12 posts...")
    
    # Slice the lists to get data from the 4th to the 12th post (index 3 onwards)
    likes_for_calc = likes_list[3:]
    print(likes_for_calc)
    comments_for_calc = comments_list[3:]
    print(comments_for_calc)
    views_for_calc = views_list[3:]
    print(views_for_calc)

    avg_likes = sum(likes_for_calc) // len(likes_for_calc) if likes_for_calc else 0
    avg_comments = sum(comments_for_calc) // len(comments_for_calc) if comments_for_calc else 0
    avg_views = sum(views_for_calc) // len(views_for_calc) if views_for_calc else 0
    
    total_likes_for_calc = sum(likes_for_calc)
    total_comments_for_calc = sum(comments_for_calc)
    
    engagement_rate = round(((total_likes_for_calc + total_comments_for_calc) / followers) * 100, 2) if followers and likes_for_calc else 0

    # Skip AI predictions - keep existing data
    print(f"ℹ️ Skipping AI predictions - keeping existing secondary_niche and location data")

    # --- Final structured data (only updating metrics, not niche/location) ---
    influencer_data = {
        "handle": username,
        "display_name": profile_data.get("full_name", ""),
        "profile_url": f"https://instagram.com/{username}",
        "profile_image_url": profile_data.get("profile_pic_url_hd"),
        "bio": bio,
        "platform": "Instagram",
        # Note: primary_niche, secondary_niche, and location will be kept from existing data
        "followers_count": followers,
        "average_views": avg_views,
        "average_comments": avg_comments,
        "engagement_rate": engagement_rate,
        "average_likes": {"avg_value": avg_likes},
        "hashtags": list(set(all_hashtags)),
        "email": (extract_emails(bio) or [None])[0],
        "bio_links": get_bio_urls(profile_data),
        "brand_tags": list(set(all_tagged_users)),
        "past_ad_placements": list(set(past_ad_placements)),
    }
    
    for i, post in enumerate(recent_posts):
        influencer_data[f"recent_post_{i+1}"] = post

    # Check if creator has posted in the last 45 days
    print("\n📅 Checking creator activity...")
    if not is_creator_active(recent_posts, days_threshold=45):
        print(f"🚫 Skipping @{username}: No posts in the last 45 days")
        return {'skipped': True}
        
    return influencer_data






# ==================== Supabase Interaction ====================

def get_existing_creators():
    """Fetches all existing Instagram Trading, Crypto, and Finance creator data from Supabase."""
    print("\nFetching existing 'Instagram' & ('Trading', 'Crypto', 'Finance') creators from Supabase...")
    target_niches = ['Trading', 'Crypto', 'Finance']
    try:
        # Fetch all columns where primary_niche is one of the target niches
        response = supabase.table("creatordata").select(
            "*"
        ).in_('primary_niche', target_niches).eq('platform', 'Instagram').execute()
        
        print(f"Found {len(response.data)} existing creators in the target niches.")
        return response.data
    except Exception as e:
        print(f"❌ Error fetching existing creators: {e}")
        return []
    


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


def rescrape_and_update_creator(handle, existing_data):
    """Rescrapes a creator and updates their record in Supabase with change tracking."""
    print(f"\n{'='*20} RESCRAPING @{handle} {'='*20}")
    
    new_data = scrape_user_data(handle)
    
    if not new_data:
        print(f"ℹ️ No data returned for @{handle}, skipping update.")
        return
    
    # Check if creator was skipped due to inactivity (no posts in 45 days)
    if new_data.get('skipped'):
        print(f"🗑️ Deleting inactive creator @{handle} from database...")
        try:
            supabase.table("creatordata").delete().eq("handle", handle).execute()
            print(f"✅ Successfully deleted inactive creator @{handle}")
        except Exception as e:
            print(f"❌ Error deleting inactive creator @{handle}: {e}")
        return

    delete_all_creator_media(handle)

    # Calculate Buzz Score using new and historical data
    buzz_score = calculate_buzz_score(new_data, existing_data)

    # Safely get old average likes for change calculation
    old_likes_data = existing_data.get('average_likes')
    old_avg_likes = 0
    if isinstance(old_likes_data, dict):
        old_avg_likes = old_likes_data.get('avg_value')
    elif isinstance(old_likes_data, int):
        old_avg_likes = old_likes_data

    followers_change, followers_change_type = calculate_change(new_data.get('followers_count'), existing_data.get('followers_count'))
    er_change, er_change_type = calculate_change(new_data.get('engagement_rate'), existing_data.get('engagement_rate'))
    views_change, views_change_type = calculate_change(new_data.get('average_views'), existing_data.get('average_views'))
    likes_change, likes_change_type = calculate_change(new_data.get('average_likes', {}).get('avg_value'), old_avg_likes)
    comments_change, comments_change_type = calculate_change(new_data.get('average_comments'), existing_data.get('average_comments'))

    # Preserve existing niche and location data
    update_payload = {
        **new_data,
        "buzz_score": buzz_score, # Add the calculated buzz score
        "followers_change": followers_change, "followers_change_type": followers_change_type,
        "engagement_rate_change": er_change, "engagement_rate_change_type": er_change_type,
        "average_views_change": views_change, "average_views_change_type": views_change_type,
        "average_likes_change": likes_change, "average_likes_change_type": likes_change_type,
        "average_comments_change": comments_change, "average_comments_change_type": comments_change_type,
        # Preserve existing niche and location data
        "primary_niche": existing_data.get("primary_niche"),
        "secondary_niche": existing_data.get("secondary_niche"),
        "location": existing_data.get("location"),
    }
    
    media_updates = process_creator_media(existing_data['id'], handle, new_data)
    update_payload.update(media_updates)

    print(f"💾 Updating data for @{handle} in Supabase...")
    try:
        supabase.table("creatordata").update(update_payload).eq("handle", handle).execute()
        print(f"✅ Successfully updated @{handle}.")
    except Exception as e:
        print(f"❌ Supabase update error for @{handle}: {e}")







def process_new_creator(username):
    """Processes a new creator and upserts their data into Supabase."""
    print(f"\n{'='*20} PROCESSING NEW @{username} {'='*20}")
    
    influencer_data = scrape_user_data(username)
    
    if not influencer_data or influencer_data.get('skipped'):
        print(f"ℹ️ No data or skipped, aborting save for @{username}.")
        return False

    print(f"💾 Saving new creator @{username} to Supabase...")
    try:
        # Upsert is safer: inserts if new, updates if exists.
        response = supabase.table("creatordata").upsert(influencer_data, on_conflict='handle').execute()
        print(f"✅ Successfully saved @{username}.")
        
        # Process media for the newly created record
        if response.data:
            creator_id = response.data[0]['id']
            print("\n⬇️ Starting media download and upload process for new creator...")
            media_updates = process_creator_media(creator_id, username, influencer_data)
            if media_updates:
                print(f"🖼️ Updating media URLs for @{username}...")
                supabase.table("creatordata").update(media_updates).eq("id", creator_id).execute()
        
        return True
    except Exception as e:
        print(f"❌ Supabase save error for @{username}: {e}")
        return False


def cleanup_inactive_creators():
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
    
    for creator in existing_creators:
        handle = creator.get('handle')
        if not handle:
            continue
            
        # Rescrape the creator to get fresh data with created_at timestamps
        fresh_data = scrape_user_data(handle)
        
        if fresh_data and not fresh_data.get('skipped'):
            # Update the creator with fresh data including created_at timestamps
            try:
                # Preserve existing niche and location data
                update_data = {
                    **fresh_data,
                    "primary_niche": creator.get("primary_niche"),
                    "secondary_niche": creator.get("secondary_niche"),
                    "location": creator.get("location"),
                }
                # First update the database with fresh data
                supabase.table("creatordata").update(update_data).eq("handle", handle).execute()
                
                # Then download and upload media files
                print(f"⬇️ Downloading media for @{handle}...")
                media_updates = process_creator_media(creator.get('id'), handle, update_data)
                
                # Update with media URLs if any were processed
                if media_updates:
                    supabase.table("creatordata").update(media_updates).eq("handle", handle).execute()
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
                supabase.table("creatordata").delete().eq("handle", handle).execute()
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

# ==================== Main Execution ====================
if __name__ == "__main__":
    print("🚀 STARTING INSTAGRAM RE-SCRAPER SCRIPT 🚀")
    
    # --- PHASE 1: CLEANUP INACTIVE CREATORS (includes rescraping) ---
    cleanup_inactive_creators()
    
    # --- PHASE 2: ADD NEW CREATORS FROM EXCEL ---
    print("\n" + "="*50)
    print("PHASE 2: ADDING NEW CREATORS FROM EXCEL FILE")
    print("="*50)
    
    # Get updated list of existing creators after cleanup
    existing_creators = get_existing_creators()
    existing_handles = {creator['handle'] for creator in existing_creators}

    excel_file = "abc.xlsx"
    script_dir = os.path.dirname(os.path.abspath(__file__))
    excel_path = os.path.join(script_dir, excel_file)
    
    if not os.path.exists(excel_path):
        print(f"❌ File '{excel_file}' not found. Skipping Phase 2.")
    else:
        try:
            df = pd.read_excel(excel_path)
            if 'Usernames' not in df.columns:
                print("❌ Excel file must have a 'Usernames' column.")
            else:
                all_usernames_from_excel = df['Usernames'].dropna().str.strip().tolist()
                new_usernames = [user for user in all_usernames_from_excel if user not in existing_handles]
                
                print(f"🔍 Found {len(all_usernames_from_excel)} total usernames in Excel.")
                print(f"✨ Found {len(new_usernames)} new usernames to process.")
                
                if not new_usernames:
                    print("✅ No new usernames to add.")
                else:
                    success_count = 0
                    for username in new_usernames:
                        if process_new_creator(username):
                            success_count += 1
                    print(f"\nNew Creator Processing Complete: {success_count}/{len(new_usernames)} successful.")

        except Exception as e:
            print(f"❌ Error processing Excel file: {e}")

    print("\n\n🎉 SCRIPT FINISHED! 🎉")