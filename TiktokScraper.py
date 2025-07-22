# import re
# from collections import Counter
# import requests

# def extract_hashtags(text):
#     if not isinstance(text, str):
#         return []
#     return re.findall(r"#\w+", text)

# def is_paid_partnership(post):
#     return post.get("commerce_info", {}).get("bc_label_test_text", "") == "Paid partnership"

# # Step 1: Get username input
# username_input = input("Enter TikTok username: ").strip().lstrip("@")
# print(f"\n🔄 Fetching data for: {username_input}")

# # Step 2: Call the ScrapeCreators API
# api_url = f"https://api.scrapecreators.com/v3/tiktok/profile/videos?handle={username_input}"
# headers = {"x-api-key": "wjhGgI14NjNMUuXA92YWXjojozF2"}
# response = requests.get(api_url, headers=headers)

# if response.status_code != 200:
#     print(f"❌ Failed to fetch data: {response.status_code}")
#     exit()

# data = response.json()
# posts = data.get('aweme_list', [])
# if not posts:
#     print("No posts found.")
#     exit()

# # --- Profile Info ---
# user_info = posts[0].get('author')

# profile = {
#     'username': user_info.get('unique_id', ''),
#     'full_name': user_info.get('nickname', ''),
#     'profile_pic': user_info.get('avatar_thumb', {}).get('url_list', [''])[0],
#     'profile_url': f"https://www.tiktok.com/@{user_info.get('unique_id', '')}",
#     'followers': user_info.get('follower_count', 0),
#     'following': user_info.get('following_count', 0),
#     'total_likes': user_info.get('total_favorited', 0),
#     'total_videos': user_info.get('aweme_count', 0)
# }
# print("\n🧑 Profile Info:")
# for k, v in profile.items():
#     print(f"{k.capitalize().replace('_', ' ')}: {v}")

# # --- Posts Data ---
# likes_list, comments_list, views_list = [], [], []
# all_hashtags = []

# print(f"\n📮 Post Details (Total: {len(posts)}):")
# for idx, post in enumerate(posts, start=1):
#     caption = post.get('desc', '')
#     likes = post.get('statistics', {}).get('digg_count', 0)
#     comments = post.get('statistics', {}).get('comment_count', 0)
#     views = post.get('statistics', {}).get('play_count', 0)
#     hashtags = extract_hashtags(caption)
#     all_hashtags += hashtags
#     cover_url = post.get('video', {}).get('cover', {}).get('url_list', [''])[0]
#     share_url = post.get('share_url', '')

#     # ✅ Paid partnership check
#     paid_partnership = is_paid_partnership(post)

#     print(f"\nPost {idx}:")
#     print(f"Caption: {caption[:80]}{'...' if len(caption) > 80 else ''}")
#     print(f"Likes: {likes}")
#     print(f"Comments: {comments}")
#     print(f"Views: {views}")
#     print(f"Hashtags: {', '.join(hashtags) if hashtags else 'None'}")
#     print(f"Paid Partnership: {'✅ Yes' if paid_partnership else '❌ No'}")
#     print(f"Cover Image: {cover_url}")
#     print(f"Share URL: {share_url}")

#     likes_list.append(likes)
#     comments_list.append(comments)
#     views_list.append(views)

# # --- Aggregated Stats ---
# def safe_avg(lst):
#     return sum(lst) // len(lst) if lst else 0

# avg_likes = safe_avg(likes_list)
# avg_comments = safe_avg(comments_list)
# avg_views = safe_avg(views_list)
# total_likes = sum(likes_list)
# total_comments = sum(comments_list)
# followers = profile['followers']
# engagement_rate = round(((total_likes + total_comments) / followers) * 100, 2) if followers else 0
# hashtag_counts = Counter(all_hashtags)

# print("\n📊 Aggregated Stats:")
# print(f"Average Likes per Post: {avg_likes}")
# print(f"Average Comments per Post: {avg_comments}")
# print(f"Average Views per Post: {avg_views}")
# print(f"Engagement Rate: {engagement_rate}%")
# print(f"All Hashtags Used: {', '.join(hashtag_counts.keys()) if hashtag_counts else 'None'}")


# import re
# from collections import Counter
# import requests
# from supabase import create_client, Client
# import google.generativeai as genai


# # Initialize Supabase client
# SUPABASE_URL = "https://pymodsojugwbbzksgubb.supabase.co"
# SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InB5bW9kc29qdWd3YmJ6a3NndWJiIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MDQ1MzQzNSwiZXhwIjoyMDY2MDI5NDM1fQ.Vlu0o5H-2Ydum6jDkMQjLj1U5VKrpZCdlzoe7WmDpj4"
# supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# GEMINI_API_KEY = "AIzaSyANXyH76cT7oDeAc7kwPvLi5hzx9NJm6FQ"
# genai.configure(api_key=GEMINI_API_KEY)
# gemini_model = genai.GenerativeModel('gemini-2.0-flash')


# PRESET_CRYPTO_TRADING_NICHES = [
#     "Altcoins", "DeFi", "Blockchain Gaming",
#     "NFTs", "Crypto Airdrops", "Web3",
#     "Metaverse", "Crypto Trading Strategies", "Blockchain Security",
#     "Crypto App Reviews", "Crypto Exchange Reviews", "Crypto News",
#     "Market Analysis", "Educational Content", "Crypto Regulations"
# ]



# def predict_secondary_niche(all_hashtags, bio, all_tagged_users):
#     """Predict secondary niche using Gemini AI (from crypto/trading preset list)"""
#     unique_hashtags = list(set(all_hashtags))
#     unique_tagged_users = list(set(all_tagged_users))
    
#     prompt = f"""
#     Analyze this Tiktok account to classify into ONE crypto/trading niche.
#     Consider these factors:
#     1. Bio content
#     2. All hashtags used
#     3. All users/brands tagged in posts
    
#     Preset Options: {PRESET_CRYPTO_TRADING_NICHES}
    
#     BIO: "{bio}"
#     ALL HASHTAGS USED: {unique_hashtags}
#     ALL TAGGED USERS/BRANDS: {unique_tagged_users}
    
#     Instructions:
#     - Return ONLY one niche name from the preset list
#     - Focus on the most dominant pattern
#     - If crypto-related but no clear match, return "General Crypto"
#     - If not crypto-related at all, return "Non-Crypto"
#     """
#     try:
#         response = gemini_model.generate_content(prompt)
#         niche = response.text.strip()
#         return niche if niche in PRESET_CRYPTO_TRADING_NICHES else "General Crypto"
#     except Exception as e:
#         print(f"⚠️ Niche prediction failed: {str(e)}")
#         return "General Crypto"
    

# def predict_freeform_location(all_captions, bio, region):
#     """Predict location in free-form text using Gemini AI with all available data"""
    
#     # Combine all captions into one text
#     combined_captions = "\n".join([f"Post {i+1}: {caption}" for i, caption in enumerate(all_captions)])
    
#     prompt = f"""
#         You are an AI agent that specializes in user profiling. Based on the following Instagram bio, 
#         all post captions, and location tags from their posts, predict the most likely location 
#         (city and country) where the user is based. Be specific and follow the priority order below:

#         PRIORITY ORDER:
#         1. Highest priority: Location explicitly mentioned in the Profile Region.
#         2. If not found in profile region: Location explicitly mentioned in the BIO.
#         3. If not found in bio, use cues from POST CAPTIONS (e.g., place names, cultural references).
#         4. If no location is clearly identifiable, return "Global".

#         BIO:
#         {bio}

#         ALL POST CAPTIONS:
#         {combined_captions}

#         Profile Region:
#         {region}

#         Return ONLY the most relevant location in "City, Country" format. No explanations.
#         Example: "Paris, France" or "Global" if uncertain.
#     """

#     try:
#         response = gemini_model.generate_content(prompt)
#         return response.text.strip()
#     except Exception as e:
#         print(f"⚠️ Location prediction failed: {str(e)}")
#         return None

# def get_profile_region(posts):
#     """Extract region from first available post"""
#     if not posts:
#         return "Global"
    
#     # Get region from first post (all posts share same profile region)
#     region = posts[0].get('region')
    
#     # Standardize region format if needed
#     if region and isinstance(region, str):
#         return region.upper()  # Example: Convert "us" to "US"
#     return "Global"

# def extract_hashtags(text):
#     if not isinstance(text, str):
#         return []
#     return re.findall(r"#\w+", text)

# def extract_tagged_users(text):
#     if not isinstance(text, str):
#         return []
#     return re.findall(r"@(\w+)", text)

# def extract_emails(text):
#     if not isinstance(text, str):
#         return []
#     return re.findall(r"\b[\w\.-]+@[\w\.-]+\.\w{2,4}\b", text)

# def is_paid_partnership(post):
#     return post.get("commerce_info", {}).get("bc_label_test_text", "") == "Paid partnership"

# # Step 1: Get username input
# username_input = input("Enter TikTok username: ").strip().lstrip("@")
# print(f"\n🔄 Fetching data for: @{username_input}")

# # Step 2: Call the ScrapeCreators API
# api_url = f"https://api.scrapecreators.com/v3/tiktok/profile/videos?handle={username_input}"
# headers = {"x-api-key": "G8w3KwSadMVJDYCKYzC8xPwFJoA3"}
# response = requests.get(api_url, headers=headers)

# if response.status_code != 200:
#     print(f"❌ Failed to fetch data: {response.status_code}")
#     exit()

# data = response.json()
# posts = data.get('aweme_list', [])
# if not posts:
#     print("❌ No posts found for this account")
#     exit()

# # --- Profile Info ---
# user_info = posts[0].get('author', {})
# bio = user_info.get('signature', '')
# emails_in_bio = extract_emails(bio)
# tagged_users_in_bio = extract_tagged_users(bio)

# profile = {
#     'username': user_info.get('unique_id', ''),
#     'full_name': user_info.get('nickname', ''),
#     'bio': bio,
#     'emails_in_bio': emails_in_bio,
#     'profile_pic': user_info.get('avatar_thumb', {}).get('url_list', [''])[0],
#     'profile_url': f"https://www.tiktok.com/@{user_info.get('unique_id', '')}",
#     'followers': user_info.get('follower_count', 0),
#     'following': user_info.get('following_count', 0),
#     'total_likes': user_info.get('total_favorited', 0),
#     'total_videos': user_info.get('aweme_count', 0),
#     'verified': user_info.get('verified', False)
# }

# print("\n🧑 Profile Info:")
# print(f"Username       : @{profile['username']}")
# print(f"Full Name      : {profile['full_name']}")
# print(f"Bio            : {profile['bio']}")
# print(f"Followers      : {profile['followers']:,}")
# print(f"Following      : {profile['following']:,}")
# print(f"Total Likes    : {profile['total_likes']:,}")
# print(f"Total Videos   : {profile['total_videos']:,}")
# print(f"Verified       : {'Yes' if profile['verified'] else 'No'}")
# print(f"Profile Pic    : {profile['profile_pic']}")
# print(f"Profile URL    : {profile['profile_url']}")
# print(f"Emails in Bio  : {', '.join(profile['emails_in_bio']) if profile['emails_in_bio'] else 'None'}")

# # --- Posts Data ---
# likes_list, comments_list, views_list = [], [], []
# all_hashtags = []
# all_tagged_users = []
# recent_posts = []
# past_ad_placements = []

# print(f"\n📮 Post Details (Analyzing {min(12, len(posts))} recent posts):")
# for idx, post in enumerate(posts[:12], start=1):
#     caption = post.get('desc', '')
#     stats = post.get('statistics', {})
#     likes = stats.get('digg_count', 0)
#     comments = stats.get('comment_count', 0)
#     views = stats.get('play_count', 0)
#     hashtags = extract_hashtags(caption)
#     tagged_users = extract_tagged_users(caption)
#     video_url = post.get('video', {}).get('play_addr', {}).get('url_list', [''])[0]
#     share_url = post.get('share_url', '')
#     paid_partnership = is_paid_partnership(post)
    
#     if paid_partnership and tagged_users:
#         past_ad_placements.extend(tagged_users)
    
#     all_hashtags.extend(hashtags)
#     all_tagged_users.extend(tagged_users)
#     likes_list.append(likes)
#     comments_list.append(comments)
#     views_list.append(views)
    
#     post_data = {
#         "caption": caption,
#         "likes": likes,
#         "comments": comments,
#         "views": views,
#         "hashtags": hashtags,
#         "tagged_users": tagged_users,
#         "is_paid_partnership": paid_partnership,
#         "video_url": video_url,
#         "share_url": share_url
#     }
#     recent_posts.append(post_data)

#     print(f"\n📹 Post {idx}:")
#     print(f"Caption         : {caption[:100]}{'...' if len(caption) > 100 else ''}")
#     print(f"Likes           : {likes:,}")
#     print(f"Comments        : {comments:,}")
#     print(f"Views           : {views:,}")
#     print(f"Hashtags        : {', '.join(hashtags) if hashtags else 'None'}")
#     print(f"Tagged Users    : {', '.join(tagged_users) if tagged_users else 'None'}")
#     print(f"Paid Partnership: {'✅ Yes' if paid_partnership else '❌ No'}")
#     print(f"Video URL       : {video_url}")
#     print(f"Share URL       : {share_url}")

# # --- Aggregated Stats ---
# def safe_avg(lst):
#     return sum(lst) // len(lst) if lst else 0

# avg_likes = safe_avg(likes_list)
# avg_comments = safe_avg(comments_list)
# avg_views = safe_avg(views_list)
# total_likes = sum(likes_list)
# total_comments = sum(comments_list)
# engagement_rate = round(((total_likes + total_comments) / profile['followers']) * 100, 2) if profile['followers'] else 0
# hashtag_counts = Counter(all_hashtags)
# top_hashtags = [tag for tag, count in hashtag_counts.most_common(5)]
# unique_tagged_users = list(set(all_tagged_users))

# print("\n📊 Aggregated Stats:")
# print(f"Average Likes       : {avg_likes:,}")
# print(f"Average Comments    : {avg_comments:,}")
# print(f"Average Views       : {avg_views:,}")
# print(f"Engagement Rate     : {engagement_rate}%")
# print(f"Total Hashtags Used : {len(set(all_hashtags))}")
# print(f"Top 5 Hashtags      : {', '.join(top_hashtags)}")
# print(f"Unique Tagged Users : {len(unique_tagged_users)}")
# print(f"Past Ad Placements  : {', '.join(set(past_ad_placements)) if past_ad_placements else 'None'}")

# all_captions = [post.get('desc', '') for post in posts[:12]]

# profile_region = get_profile_region(posts)
# secondary_niche = predict_secondary_niche(all_hashtags, profile['bio'], all_tagged_users)
# location = predict_freeform_location(all_captions, profile['bio'], profile_region)

# # --- Prepare for Supabase ---
# influencer_data = {
    
#     "handle": profile['username'],
#     "display_name": profile['full_name'] or "",
#     "profile_url": profile['profile_url'] or "",
#     "profile_image_url": profile['profile_pic'] or "",
#     "bio": profile['bio'] or "",
#     "platform": "TikTok",
#     "primary_niche": "Crypto Trading",
#     "secondary_niche": secondary_niche, 
#     "brand_tags": list(set(all_tagged_users)) if all_tagged_users else [],
#     "location": location,             
#     "followers_count": profile['followers'] or 0,
#     "average_views": avg_views or 0,
#     "average_likes": avg_likes or 0,
#     "average_comments": avg_comments or 0,
#     "engagement_rate": float(engagement_rate) if engagement_rate else 0.0,
#     "hashtags": list(set(all_hashtags)) if all_hashtags else [],
#     "email": profile['emails_in_bio'][0] if profile['emails_in_bio'] else None,
#     "past_ad_placements": list(set(past_ad_placements)) if past_ad_placements else [],
# }

# # Add recent posts data
# for i in range(min(12, len(recent_posts))):
#     post = recent_posts[i]
#     influencer_data[f"recent_post_{i+1}"] = {
#         "caption": post.get("caption", ""),
#         "likes": post.get("likes", 0),
#         "comments": post.get("comments", 0),
#         "views": post.get("views", 0),
#         "hashtags": post.get("hashtags", []),
#         "tagged_users": post.get("tagged_users", []),
#         "is_paid_partnership": post.get("is_paid_partnership", False),
#         "video_url": post.get("video_url", ""),
#         "share_url": post.get("share_url", "")
#     }

# # Insert into Supabase
# try:
#     response = supabase.table("influencerdata").insert(influencer_data).execute()
#     if hasattr(response, 'error') and response.error:
#         print(f"\n❌ Error inserting into Supabase: {response.error}")
#     else:
#         print("\n✅ Successfully inserted TikTok influencer data into Supabase!")
# except Exception as e:
#     print(f"\n❌ Supabase insertion error: {str(e)}")





import re
import pandas as pd
from collections import Counter
import requests
from supabase import create_client, Client
import google.generativeai as genai
from tqdm import tqdm
import time
import traceback
import re
import pandas as pd
from datetime import datetime
import os
from urllib.parse import urlparse
from typing import Optional
import mimetypes
import asyncio
import pillow_heif
from PIL import Image
import io

# Initialize Supabase client
SUPABASE_URL = "https://unovwhgnwenxbyvpevcz.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InVub3Z3aGdud2VueGJ5dnBldmN6Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MjEzNjU0MCwiZXhwIjoyMDY3NzEyNTQwfQ.bqXWKTR64TRARH2VOrjiQdPnQ7W-8EGJp5RIi7yFmck"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

GEMINI_API_KEY = "AIzaSyAviNluYLDgL9qrgKQx3j7lfOoUQv8F3nw"
genai.configure(api_key=GEMINI_API_KEY)
gemini_model = genai.GenerativeModel('gemini-2.0-flash')

BUCKET_NAME = "profile-media"
MAX_RECENT_POSTS = 4

PRESET_CRYPTO_TRADING_NICHES = [
    "Altcoins", "DeFi",
    "NFTs", "Crypto Airdrops", "Web3", "Crypto Trading",
    "Crypto News","Market Analysis","Meme Coins"
]



def load_usernames_from_excel(file_path):
    """Load usernames from Excel file"""
    try:
        df = pd.read_excel(file_path)
        if 'Usernames' not in df.columns:
            raise ValueError("Excel file must contain 'Usernames' column")
        return df['Usernames'].dropna().tolist()
    except Exception as e:
        print(f"❌ Error loading Excel file: {str(e)}")
        return []



def is_crypto_influencer(bio: str, all_captions: list = [], all_hashtags: list = [], username: str = "") -> bool:

    print(bio)
    print(username)
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
        print(decision)
        return decision
    except Exception as e:
        print(f"⚠️ Error determining influencer type: {e}")
        return False



def predict_secondary_niche(all_hashtags, bio, all_tagged_users):
    """Predict secondary niche using Gemini AI (from crypto/trading preset list)"""
    print("🔮 Predicting secondary niche...")
    unique_hashtags = list(set(all_hashtags))
    unique_tagged_users = list(set(all_tagged_users))

    print(f"Unique Hashtags: {unique_hashtags}")
    print(f"Unique Tagged Users: {unique_tagged_users}")
    
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



def predict_freeform_location(all_captions, bio, region):
    """Predict location in free-form text using Gemini AI"""
    combined_captions = "\n".join([f"Post {i+1}: {caption}" for i, caption in enumerate(all_captions)])
    
    prompt = f"""
    Predict the most likely location (city and country) where the user is based.
    PRIORITY ORDER:
    1. Profile Region if specific
    2. Location mentioned in BIO
    3. Cues from POST CAPTIONS
    4. Return "Global" if uncertain
    
    BIO: "{bio}"
    ALL POST CAPTIONS: {combined_captions}
    Profile Region: {region}
    
    Return ONLY the location in "City, Country" format or "Global".
    """
    try:
        response = gemini_model.generate_content(prompt)
        return response.text.strip()
    except Exception as e:
        print(f"⚠️ Location prediction failed: {str(e)}")
        return "Global"


def get_profile_region(posts):
    """Extract region from first available post"""
    if not posts:
        return "Global"
    region = posts[0].get('region')
    return region.upper() if region and isinstance(region, str) else "Global"


def extract_hashtags(text):
    """Extract hashtags from text using regex"""
    if not isinstance(text, str):
        return []
    return [tag.lower() for tag in re.findall(r"#(\w+)", text)]


def extract_tagged_users(text):
    """Extract @mentions from text using regex"""
    if not isinstance(text, str):
        return []
    return re.findall(r"@(\w+)", text)


def extract_emails(text):
    """Extract emails from text using regex"""
    if not isinstance(text, str):
        return []
    return re.findall(r"\b[\w\.-]+@[\w\.-]+\.\w{2,4}\b", text)


def extract_bio_links(bio):
    """Extract all URLs from bio links in the same style as followers count extraction"""
    bio_urls = []
    
    if bio.get("bio_links"):
        for link in bio["bio_links"]:
            if link.get("url"):
                bio_urls.append(link["url"])
    
    if bio.get("external_url"):
        bio_urls.append(bio["external_url"])
    
    return bio_urls


def is_paid_partnership(post):
    """Check if post is marked as paid partnership"""
    return post.get("commerce_info", {}).get("bc_label_test_text", "") == "Paid partnership"

def get_file_extension_and_type(url: str) -> tuple:
    """Extract file extension and content type from URL."""
    parsed = urlparse(url)
    path = parsed.path
    
    # Get the extension
    ext = os.path.splitext(path)[1]
    if not ext:
        # If no extension, try to determine from content
        ext = '.mp4' if 'video' in path.lower() else '.jpg'
    
    # Guess the content type
    content_type, _ = mimetypes.guess_type(url)
    if not content_type:
        content_type = 'video/mp4' if ext == '.mp4' else 'image/jpeg'
    
    return ext, content_type


def clean_handle(handle: str) -> str:
    """Clean the handle to be used as a folder name."""
    return handle.replace('/', '_').replace('\\', '_').strip()


# def download_file(url: str) -> bytes:
#     """Download file from URL and return its content as bytes."""
#     try:
#         response = requests.get(url, timeout=10)
#         response.raise_for_status()
#         return response.content
#     except requests.RequestException as e:
#         print(f"Error downloading {url}: {e}")
#         return None



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
        # Upload the file
        res = supabase.storage.from_(bucket).upload(
            path=path,
            file=file_content,
            file_options={"content-type": content_type} if content_type else {}
        )
        
        # Get public URL
        url = supabase.storage.from_(bucket).get_public_url(path)
        return url
    except Exception as e:
        print(f"Error uploading to Supabase storage: {e}")
        return None



def process_creator_media(creator_id: str, handle: str, creator_data: dict):
    """Process media for a single creator."""
    # Clean the handle for use in storage paths
    clean_handle_name = clean_handle(handle)
    storage_folder = f"{clean_handle_name}/"
    
    updates = {}
    processed_media = 0
    
    # Process profile image (only if it doesn't exist in storage)
    if creator_data.get("profile_image_url"):
        profile_ext, profile_content_type = get_file_extension_and_type(creator_data["profile_image_url"])
        profile_storage_path = f"{storage_folder}profile{profile_ext}"
        
        # Check if profile image already exists in storage
        try:
            existing_files = supabase.storage.from_(BUCKET_NAME).list(storage_folder)
            profile_exists = any(f['name'] == f"profile{profile_ext}" for f in existing_files)
        except:
            profile_exists = False
        
        if not profile_exists:
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
    
    # Process recent posts (up to MAX_RECENT_POSTS)
    for i in range(1, MAX_RECENT_POSTS + 1):
        post_key = f"recent_post_{i}"
        if processed_media >= MAX_RECENT_POSTS:
            break
            
        if creator_data.get(post_key) and isinstance(creator_data[post_key], dict):
            post = creator_data[post_key].copy()  # Create a copy to modify
            
            # Normalize video_url to always be a list
            video_urls = []
            if post.get("video_url"):
                if isinstance(post["video_url"], str):
                    video_urls = [post["video_url"]]
                elif isinstance(post["video_url"], list):
                    video_urls = post["video_url"]
            
            # Process each media URL
            new_video_urls = []
            for media_url in video_urls:
                if processed_media >= MAX_RECENT_POSTS:
                    break

                # video_url = post["video_url"] if isinstance(post["video_url"], str) else post["video_url"][0]
                # thumbnail_url = get_thumbnail_from_video_url(video_url)
                

                
                if media_url:
                    try:
                        ext, content_type = get_file_extension_and_type(media_url)
                        media_storage_path = f"{storage_folder}media_{processed_media + 1}{ext}"
                        
                        file_content = download_file(media_url)
                        if file_content:
                            new_url = upload_to_supabase_storage(
                                BUCKET_NAME,
                                media_storage_path,
                                file_content,
                                content_type
                            )
                            if new_url:
                                post["video_url"] = new_url
                                updates[post_key] = post
                                processed_media += 1
                    except Exception as e:
                        print(f"Error processing media URL {media_url}: {str(e)}")
                        continue
            
            # Only update if we processed media URLs
            if new_video_urls:
                # Convert back to single URL if that was the original format
                if isinstance(post["video_url"], str) and len(new_video_urls) == 1:
                    post["video_url"] = new_video_urls[0]
                else:
                    post["video_url"] = new_video_urls
                
                updates[post_key] = post

            

    # Update database if there are changes
    if updates:
        supabase.table("creatordata").update(updates).eq("id", creator_id).execute()
        print(f"✅ Updated {processed_media} media URLs for creator {handle}")
    else:
        print(f"ℹ️ No new media to update for creator {handle}")



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

def process_tiktok_account(username, api_key):
    """Process a single TikTok account and return structured data"""
    print(f"\n🔄 Processing: @{username}")
    
    # API call to get profile data
    api_url = f"https://api.scrapecreators.com/v3/tiktok/profile/videos?handle={username}"
    headers = {"x-api-key": api_key}
    response = requests.get(api_url, headers=headers)
    
    if response.status_code != 200:
        print(f"❌ Failed to fetch data: {response.status_code}")
        return None
    
    data = response.json()
    posts = safe_get(data, ['aweme_list'], [])
    
    if not posts:
        print("❌ No posts found for this account")
        return None
    
    # Profile info extraction
    user_info = safe_get(posts[0], ['author'], {})
    bio = safe_get(user_info, ['signature'], '')
    followers = user_info.get('follower_count', 0)
    
    # Early filtering for crypto influencers with 10k-350k followers
    if not (10000 <= followers <= 350000):
        print(f"❌ Skipped: Follower count {followers} outside target range")
        return None
    
    if not is_crypto_influencer(bio, [], [], username):
        print("❌ Skipped: Not a crypto influencer")
        return None
    
    # Full data processing
    emails_in_bio = extract_emails(bio)
    tagged_users_in_bio = extract_tagged_users(bio)
    # bio_links = extract_bio_links(bio)
    # print(bio_links)

    

    profile = {
        'username': user_info.get('unique_id', ''),
        'full_name': user_info.get('nickname', ''),
        'bio': bio,
        'emails_in_bio': emails_in_bio,
        'bio_links': "",
        'profile_pic': user_info.get('avatar_thumb', {}).get('url_list', [''])[0],
        'profile_url': f"https://www.tiktok.com/@{user_info.get('unique_id', '')}",
        'followers': followers,
        'following': user_info.get('following_count', 0),
        'total_likes': user_info.get('total_favorited', 0),
        'total_videos': user_info.get('aweme_count', 0),
        # 'verified': user_info.get('is_verified', False),
        # 'category': user_info.get('category', ''),
    }
    print(profile)

    # Posts data processing
    likes_list, comments_list, views_list = [], [], []
    all_hashtags = []
    all_tagged_users = []
    recent_posts = []
    past_ad_placements = []
    all_captions = []
    
    for post in posts[:12]:
        caption = post.get('desc', '')
        stats = post.get('statistics', {})
        likes = stats.get('digg_count', 0)
        comments = stats.get('comment_count', 0)
        views = stats.get('play_count', 0)
        hashtags = extract_hashtags(caption)
        tagged_users = extract_tagged_users(caption)
        # video_url = post.get('video', {}).get('play_addr', {}).get('url_list', [''])[0]
        video_url = post.get('video', {}).get('ai_dynamic_cover', {}).get('url_list', [''])[0]
        share_url = post.get('share_url', '')
        paid_partnership = is_paid_partnership(post)
        
        if paid_partnership and tagged_users:
            past_ad_placements.extend(tagged_users)
        
        all_hashtags.extend(hashtags)
        all_tagged_users.extend(tagged_users)
        likes_list.append(likes)
        comments_list.append(comments)
        views_list.append(views)
        all_captions.append(caption)
        
        post_data = {
            "caption": caption,
            "likes": likes,
            "comments": comments,
            "views": views,
            "hashtags": hashtags,
            "tagged_users": tagged_users,
            "is_paid_partnership": paid_partnership,
            "video_url": video_url,
            "share_url": share_url,
            "is_video": True,  # TikTok is always video
            "is_carousel": False  # TikTok doesn't have carousels
        }
        recent_posts.append(post_data)
    
    # Calculate metrics
    avg_likes = sum(likes_list) // len(likes_list) if likes_list else 0
    avg_comments = sum(comments_list) // len(comments_list) if comments_list else 0
    avg_views = sum(views_list) // len(views_list) if views_list else 0
    total_likes = sum(likes_list)
    total_comments = sum(comments_list)
    engagement_rate = calculate_engagement_rate(total_likes, total_comments, profile['followers'])
    
    # Get additional predictions
    profile_region = get_profile_region(posts)
    secondary_niche = predict_secondary_niche(all_hashtags, profile['bio'], all_tagged_users)
    location = predict_freeform_location(all_captions, profile['bio'], profile_region)
    
    # Prepare data for Supabase
    influencer_data = {
        "handle": profile['username'],
        "display_name": profile['full_name'],
        "profile_url": profile['profile_url'],
        "profile_image_url": profile['profile_pic'],
        "bio": profile['bio'],
        "platform": "TikTok",
        "primary_niche": "Crypto",
        "secondary_niche": secondary_niche,
        "brand_tags": list(set(all_tagged_users)),
        "location": location,
        "followers_count": profile['followers'],
        "average_views": avg_views,
        "average_likes": avg_likes,
        "average_comments": avg_comments,
        "engagement_rate": float(engagement_rate),
        "hashtags": list(set(all_hashtags)),
        "email": emails_in_bio[0] if emails_in_bio else None,
        "past_ad_placements": list(set(past_ad_placements)),
        "bio_links": "",
    }
    
    # Add recent posts data
    for i in range(min(12, len(recent_posts))):
        post = recent_posts[i]
        influencer_data[f"recent_post_{i+1}"] = {
            "caption": post.get("caption", ""),
            "likes": post.get("likes", 0),
            "comments": post.get("comments", 0),
            "views": post.get("views", 0),
            "hashtags": post.get("hashtags", []),
            "tagged_users": post.get("tagged_users", []),
            "is_paid_partnership": post.get("is_paid_partnership", False),
            "video_url": post.get("video_url", ""),
            "share_url": post.get("share_url", ""),
            "is_video": post.get("is_video", True),
            "is_carousel": post.get("is_carousel", False)
        }
    
    return influencer_data


async def main():
    usernames = load_usernames_from_excel("TiktokList.xlsx")
    if not usernames:
        print("❌ No usernames found to process")
        return
    
    api_key = "wjhGgI14NjNMUuXA92YWXjojozF2"
    success_count = 0
    failure_count = 0
    
    print(f"\n🚀 Starting batch processing of {len(usernames)} accounts")
    
    for username in tqdm(usernames, desc="Processing accounts"):
        try:
            username = username.strip().lstrip("@")
            influencer_data = process_tiktok_account(username, api_key)
            
            if influencer_data:
                # Insert into Supabase
                try:
                    response = supabase.table("creatordata").insert(influencer_data).execute()
                    if hasattr(response, 'error') and response.error:
                        print(f"❌ Supabase error for @{username}: {response.error}")
                        failure_count += 1
                    else:
                        success_count += 1

                    if hasattr(response, 'data') and response.data:
                        inserted_data = response.data[0] if isinstance(response.data, list) else response.data
                        creator_id = inserted_data.get('id')

                        if creator_id:
                            print("\n⬇️ Starting media download and upload process...")
                            try:
                                # Ensure bucket exists
                                try:
                                    supabase.storage.get_bucket(BUCKET_NAME)
                                except:
                                    supabase.storage.create_bucket(BUCKET_NAME, public=True)

                                # Process media for this creator
                                await process_creator_media(creator_id, username, influencer_data)
                            except Exception as media_error:
                                print(f"⚠️ Media processing error: {str(media_error)}")
                except Exception as e:
                    print(f"❌ Error processing @{username}: {str(e)}")
                    traceback.print_exc()
                    failure_count += 1
            else:
                failure_count += 1
            
            # Rate limiting
            await asyncio.sleep(1)  # Changed to async sleep
            
        except Exception as e:
            print(f"\n❌ Unexpected error processing @{username}: {str(e)}")
            failure_count += 1
            continue
    
    print(f"\n🎉 Batch processing complete!")
    print(f"✅ Successfully processed: {success_count} accounts")
    print(f"❌ Failed to process: {failure_count} accounts")

if __name__ == "__main__":
    # Proper way to run the async main function
    asyncio.run(main())