import google.generativeai as genai
import requests
import re
import pandas as pd
from supabase import create_client, Client
from datetime import datetime
import os
from urllib.parse import urlparse
import mimetypes

# Initialize Gemini AI
GEMINI_API_KEY = "AIzaSyAviNluYLDgL9qrgKQx3j7lfOoUQv8F3nw"
genai.configure(api_key=GEMINI_API_KEY)
gemini_model = genai.GenerativeModel('gemini-2.0-flash')

# Initialize Supabase client
SUPABASE_URL = "https://unovwhgnwenxbyvpevcz.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InVub3Z3aGdud2VueGJ5dnBldmN6Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MjEzNjU0MCwiZXhwIjoyMDY3NzEyNTQwfQ.bqXWKTR64TRARH2VOrjiQdPnQ7W-8EGJp5RIi7yFmck"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BUCKET_NAME = "profile-media"
MAX_RECENT_POSTS = 4

# ==================== Media Downloading Functions ====================
def download_file(url: str) -> bytes:
    """Download file from URL and return its content as bytes."""
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.content
    except requests.RequestException as e:
        print(f"Error downloading {url}: {e}")
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

def process_creator_media(creator_id: str, handle: str, creator_data: dict):
    """Process media for a single creator."""
    clean_handle_name = clean_handle(handle)
    storage_folder = f"{clean_handle_name}/"
    
    updates = {}
    processed_media = 0
    
    # Process profile image
    if creator_data.get("profile_image_url"):
        profile_ext, profile_content_type = get_file_extension_and_type(creator_data["profile_image_url"])
        profile_storage_path = f"{storage_folder}profile{profile_ext}"
        
        try:
            existing_files = supabase.storage.from_(BUCKET_NAME).list(storage_folder)
            profile_exists = any(f['name'] == f"profile{profile_ext}" for f in existing_files)
        except Exception as e:
            print(f"⚠️ Error checking existing profile image: {e}")
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

        post = creator_data.get(post_key)
        if not post or not isinstance(post, dict):
            continue

        post = post.copy()
        media_urls = post.get("media_urls")

        if not media_urls:
            continue

        # Normalize to list
        if isinstance(media_urls, str):
            media_urls = [media_urls]
        elif not isinstance(media_urls, list):
            print(f"⚠️ Skipping post with invalid media_urls format: {media_urls}")
            continue

        new_media_urls = []
        for media_url in media_urls:
            if processed_media >= MAX_RECENT_POSTS:
                break

            if not media_url or not isinstance(media_url, str) or not media_url.startswith("http"):
                print(f"❌ Skipping invalid media URL: {media_url}")
                continue

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
                        new_media_urls.append(new_url)
                        processed_media += 1

            except Exception as e:
                print(f"❌ Error downloading or uploading {media_url}: {e}")

        if new_media_urls:
            post["media_urls"] = new_media_urls
            updates[post_key] = post

    # Commit updates to database
    if updates:
        supabase.table("creatordata").update(updates).eq("id", creator_id).execute()
        print(f"✅ Updated {processed_media} media URLs for creator {handle}")
    else:
        print(f"ℹ️ No new media to update for creator {handle}")




# ==================== Original Functions ====================

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

def predict_secondary_niche(all_hashtags, bio, all_tagged_users):
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

def predict_freeform_location(all_captions, bio, all_locations):
    """Predict location in free-form text using Gemini AI with all available data"""
    print("🌍 Predicting location...")
    location_text = "\n".join([
        f"- {loc['name']} (Address: {loc.get('address', 'N/A')}, City: {loc.get('city', 'N/A')}"
        for loc in all_locations if loc is not None
    ]) if all_locations else "No location tags in posts"
    
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

    try:
        response = gemini_model.generate_content(prompt)
        location = response.text.strip()
        print(f"✅ Predicted location: {location}")
        return location
    except Exception as e:
        print(f"⚠️ Location prediction failed: {str(e)}")
        return None

def extract_hashtags(text):
    if not isinstance(text, str):
        return []
    return re.findall(r"#\w+", text)

def get_bio_urls(user_data):
    """Extract all URLs from bio links in the same style as followers count extraction"""
    bio_urls = []
    
    # Check if bio_links exists in the API response
    if user_data.get("bio_links"):
        for link in user_data["bio_links"]:
            if link.get("url"):  # Only add if URL exists
                bio_urls.append(link["url"])
    
    # Also check for standalone external_url (common in Instagram API)
    if user_data.get("external_url"):
        bio_urls.append(user_data["external_url"])
    
    return bio_urls

def extract_emails(text):
    if not isinstance(text, str):
        return []
    return re.findall(r"\b[\w\.-]+@[\w\.-]+\.\w{2,4}\b", text)

def process_instagram_user(username_input):
    """Process a single Instagram username"""
    username_input = username_input.strip().lstrip("@")
    print(f"\n{'='*50}")
    print(f"🔄 Processing: @{username_input}")
    print(f"{'='*50}")
    
    # --- Profile Data from ScrapeCreators (v1) ---
    print("\n📡 Fetching profile data from ScrapeCreators API...")
    profile_url = f"https://api.scrapecreators.com/v1/instagram/profile?handle={username_input}"
    headers = {"x-api-key": "wjhGgI14NjNMUuXA92YWXjojozF2"}
    response = requests.get(profile_url, headers=headers)

    if response.status_code != 200:
        print(f"❌ Failed to fetch profile data: {response.status_code}")
        return None

    try:
        data_response = response.json()
        data = data_response["data"]["user"]

        full_name = data.get("full_name")
        bio = data.get("biography")
        avatar_url = data.get("profile_pic_url_hd")
        profile_link = f"https://instagram.com/{username_input}"
        category = data.get("category_name")
        followers = data.get("edge_followed_by", {}).get("count", 0)
        following = data.get("edge_follow", {}).get("count", 0)
        emails_in_bio = extract_emails(bio)
        bio_urls = get_bio_urls(data)

        print("\n🧑 Profile Info:")
        print(f"Username      : {username_input}")
        print(f"Full Name     : {full_name}")
        print(f"Bio           : {bio}")
        print(f"Emails in Bio : {', '.join(emails_in_bio) if emails_in_bio else 'None'}")
        print(f"Followers     : {followers}")
        print(f"Bio URLs  : {', '.join(bio_urls) if bio_urls else 'None'}")
        print(f"Following     : {following}")
        print(f"Profile Pic   : {avatar_url}")
        print(f"Profile URL   : {profile_link}")
        print(f"Category      : {category}")

    except Exception as e:
        print("❌ Error parsing ScrapeCreators profile data:", str(e))
        return None
    
    print(f"Category      : {category}")

    # Check follower count threshold
    if followers < 10_000 or followers > 350_000:
        print(f"🚫 Skipping: Follower count {followers} not in 10k–350k range.")
        return None
    # Check if user is crypto influencer using bio and username only
    precheck_crypto = is_crypto_influencer(bio=bio or "", username=username_input)
    if not precheck_crypto:
        print("🚫 Skipping: User not identified as crypto influencer based on username/bio.")
        return None

    # --- Posts Data from ScrapeCreators ---
    print("\n📡 Fetching post data from ScrapeCreators API...")
    scrapecreators_url = f"https://api.scrapecreators.com/v2/instagram/user/posts?handle={username_input}"
    scrapecreators_headers = {"x-api-key": "wjhGgI14NjNMUuXA92YWXjojozF2"}
    posts_response = requests.get(scrapecreators_url, headers=scrapecreators_headers)

    if posts_response.status_code != 200:
        print(f"❌ Failed to fetch post data from ScrapeCreators: {posts_response.status_code}")
        return None

    try:
        posts_data = posts_response.json().get("items", [])

        likes_list, comments_list, views_list = [], [], []
        all_hashtags = []
        tagged_users = []
        hidden_likes_count = 0
        recent_posts = []
        past_ad_placements = []
        all_captions = []
        all_locations = []
        all_tagged_users_in_posts = []

        print(f"\n📮 Found {len(posts_data)} posts. Analyzing first 12 posts...")

        for idx, post in enumerate(posts_data[:12], start=1):
            caption_obj = post.get("caption", {})
            caption = caption_obj.get("text", "") if isinstance(caption_obj, dict) else str(caption_obj)
            all_captions.append(caption)
            usertags = post.get("usertags", {}).get("in", [])
            brand_usernames = [tag.get("user", {}).get("username") for tag in usertags if tag.get("user", {}).get("username")]
            all_tagged_users_in_posts.extend(brand_usernames)
        
            is_paid_partnership = post.get("is_paid_partnership", False)
            
            if is_paid_partnership and brand_usernames:
                past_ad_placements.extend(brand_usernames)
            
            like_hidden = post.get("like_and_view_counts_disabled", True)
            likes = post.get("like_count") if not like_hidden else None
            comments = post.get("comment_count")
            play_count = post.get("play_count")

            is_video = post.get("media_type") in [2, 8]
            is_carousel = post.get("carousel_media_count", 0) > 0

            # Extract location data
            location = post.get("location", {})
            location_name = location.get("name") if location else None
            location_id = location.get("id") if location else None
            location_slug = location.get("slug") if location else None
            location_address = location.get("address") if location else None
            location_city = location.get("city") if location else None
            
            location_info = None
            if location_name:
                location_info = {
                    "name": location_name,
                    "id": location_id,
                    "slug": location_slug,
                    "address": location_address,
                    "city": location_city
                }
                all_locations.append(location_info)

            media_urls = post.get("image_versions2", {}) \
                        .get("additional_candidates", {}) \
                        .get("igtv_first_frame", {}) \
                        .get("url")

            if not like_hidden:
                likes_list.append(likes or 0)
            else:
                hidden_likes_count += 1

            comments_list.append(comments or 0)
            if play_count is not None:
                views_list.append(play_count)

            hashtags = extract_hashtags(caption)
            all_hashtags += hashtags

            tagged_users += brand_usernames

            post_data = {
                "caption": caption,
                "likes": likes,
                "comments": comments,
                "views": play_count,
                "is_video": is_video,
                "is_carousel": is_carousel,
                "media_urls": media_urls,
                "hashtags": hashtags,
                "brand_tags": brand_usernames,
                "is_paid_partnership": is_paid_partnership,
                "like_hidden": like_hidden,
                "location": location_info
            }
            recent_posts.append(post_data)

            print(f"\n📸 Post {idx}:")
            print(f"Type           : {'Video' if is_video else 'Image'}{' (Carousel)' if is_carousel else ''}")
            print(f"Likes          : {likes if not like_hidden else 'Hidden'}")
            print(f"Comments       : {comments}")
            print(f"Views          : {play_count if is_video else 'N/A'}")
            print(f"Brand Tags     : {', '.join(brand_usernames) if brand_usernames else 'None'}")
            print(f"Paid Partner   : {'Yes' if is_paid_partnership else 'No'}")
            print(f"Hashtags       : {', '.join(hashtags)}")
            print(f"Location       : {location_name if location_name else 'None'}")
            print(f"Caption Preview: {caption[:100]}{'...' if len(caption) > 100 else ''}")

        avg_likes = sum(likes_list) // len(likes_list) if likes_list else 0
        avg_comments = sum(comments_list) // len(comments_list) if comments_list else 0
        avg_views = sum(views_list) // len(views_list) if views_list else 0
        total_likes = sum(likes_list)
        total_comments = sum(comments_list)
        engagement_rate = round(((total_likes + total_comments) / followers) * 100, 2) if followers else 0

        likes_data = {
            "avg_value": avg_likes,
            "hidden_likes_count": hidden_likes_count,
        }

        print("\n📊 Aggregated Stats:")
        print(f"Average Likes    : {avg_likes}", end="")
        if hidden_likes_count > 0:
            if hidden_likes_count > 6:
                print(f" (Most post likes are hidden; {hidden_likes_count} posts with hidden likes)")
            else:
                print(f" (A few post likes are hidden; {hidden_likes_count} posts with hidden likes)")
        else:
            print()
        print(f"Average Comments : {avg_comments}")
        print(f"Average Views    : {avg_views}")
        print(f"Engagement Rate  : {engagement_rate}%")
        print(f"Total Hashtags   : {len(set(all_hashtags))} unique")
        print(f"Brand Tags       : {len(set(tagged_users))} unique")
        print(f"Past Ad Partners : {len(set(past_ad_placements))}")

        secondary_niche = predict_secondary_niche(all_hashtags, bio, all_tagged_users_in_posts)
        location = predict_freeform_location(all_captions, bio, all_locations)


        influencer_data = {
            "handle": username_input,
            "display_name": full_name or "",
            "profile_url": profile_link or "",
            "profile_image_url": avatar_url or "",
            "bio": bio or "",
            "platform": "Instagram",
            "primary_niche": "Crypto",
            "secondary_niche": secondary_niche,
            "brand_tags": list(set(tagged_users)) if tagged_users else [],
            "location": location,
            "followers_count": followers or 0,
            "bio_links": bio_urls,
            "average_views": avg_views or 0,
            "average_likes": likes_data or 0,
            "average_comments": avg_comments or 0,
            "engagement_rate": float(engagement_rate) if engagement_rate else 0.0,
            "hashtags": list(set(all_hashtags)) if all_hashtags else [],
            "email": emails_in_bio[0] if emails_in_bio else None,
            "past_ad_placements": list(set(past_ad_placements)) if past_ad_placements else []
        }

        for i in range(min(12, len(recent_posts))):
            post = recent_posts[i]
            influencer_data[f"recent_post_{i+1}"] = {
                "caption": post.get("caption", "") or "",
                "likes": post.get("likes", 0) or 0,
                "comments": post.get("comments", 0) or 0,
                "views": post.get("views", 0) or 0,
                "is_video": bool(post.get("is_video", False)),
                "is_carousel": bool(post.get("is_carousel", False)),
                "media_urls": post.get("media_urls", []) or [],
                "hashtags": post.get("hashtags", []) or [],
                "brand_tags": post.get("brand_tags", []) or [],
                "is_paid_partnership": bool(post.get("is_paid_partnership", False)),
                "like_hidden": bool(post.get("like_hidden", False)),
                "location": post.get("location")
            }

        print("\n💾 Preparing to save data to Supabase...")
        try:
            # First insert the basic data to get the ID
            insert_response = supabase.table("creatordata").insert(influencer_data).execute()
            
            if hasattr(insert_response, 'error') and insert_response.error:
                print(f"❌ Error inserting into Supabase: {insert_response.error}")
                return None
            else:
                print("\n✅ Successfully saved influencer data to Supabase!")
                print(f"Past Ad Placements: {', '.join(past_ad_placements) if past_ad_placements else 'None found'}")
                
                # Get the inserted ID from the response
                if hasattr(insert_response, 'data') and insert_response.data:
                    inserted_data = insert_response.data[0] if isinstance(insert_response.data, list) else insert_response.data
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
                            process_creator_media(creator_id, username_input, influencer_data)
                        except Exception as media_error:
                            print(f"⚠️ Media processing error: {str(media_error)}")
                    
                return influencer_data
        except Exception as e:
            print(f"❌ Supabase insertion error: {str(e)}")
            return None

    except Exception as e:
        print("❌ Error parsing ScrapeCreators post data:", str(e))
        return None

# Main execution
if __name__ == "__main__":
    
    excel_file = "FollowingList.xlsx"
    script_dir = os.path.dirname(os.path.abspath(__file__))
    excel_path = os.path.join(script_dir, excel_file)
    
    try:
        print(f"\n📂 Looking for Excel file: {excel_path}")
        if not os.path.exists(excel_path):
            print(f"❌ Error: File '{excel_file}' not found in the script directory")
            exit()
            
        df = pd.read_excel(excel_path)
        if 'Usernames' not in df.columns:
            print("❌ Error: The Excel file must have a column named 'usernames'")
            exit()
            
        usernames = df['Usernames'].dropna().tolist()
        print(f"🔍 Found {len(usernames)} usernames to process")
        
        success_count = 0
        for idx, username in enumerate(usernames, start=1):
            print(f"\n{'='*50}")
            print(f"🔢 Processing {idx}/{len(usernames)}: {username}")
            result = process_instagram_user(username)
            if result:
                success_count += 1
            print(f"{'='*50}")
            
        print("\n✅ Processing complete!")
        print(f"Total processed: {len(usernames)}")
        print(f"Successful: {success_count}")
        print(f"Failed: {len(usernames) - success_count}")
    except Exception as e:
        print(f"❌ Error reading Excel file: {str(e)}")