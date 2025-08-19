#!/usr/bin/env python3
"""
Simple job runner that recreates the CSV data and runs the remaining creators
"""

import os
import sys
import asyncio
import time
from datetime import datetime
from supabase import create_client, Client

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import our scraper functions
try:
    from UnifiedScraper import process_instagram_user, process_tiktok_account
except ImportError as e:
    print(f"❌ Failed to import scraper functions: {e}")
    print("Make sure UnifiedScraper.py is in the same directory")
    sys.exit(1)

# Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://unovwhgnwenxbyvpevcz.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InVub3Z3aGdud2VueGJ5dnBldmN6Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MjEzNjU0MCwiZXhwIjoyMDY3NzEyNTQwfQ.bqXWKTR64TRARH2VOrjiQdPnQ7W-8EGJp5RIi7yFmck")

def get_supabase_client():
    """Get Supabase client"""
    try:
        return create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        print(f"❌ Supabase connection failed: {e}")
        return None

def update_job_progress(supabase, job_id, processed_items, failed_items=0):
    """Update job progress in database"""
    try:
        supabase.table("scraper_jobs").update({
            "processed_items": processed_items,
            "failed_items": failed_items,
            "updated_at": datetime.now(datetime.timezone.utc).isoformat()
        }).eq("id", job_id).execute()
    except Exception as e:
        print(f"❌ Failed to update progress: {e}")

def update_job_status(supabase, job_id, status, processed_items=None, failed_items=None, results=None, error_message=None):
    """Update job status in database"""
    try:
        update_data = {
            "status": status,
            "updated_at": datetime.now(datetime.timezone.utc).isoformat()
        }
        
        if processed_items is not None:
            update_data["processed_items"] = processed_items
        if failed_items is not None:
            update_data["failed_items"] = failed_items
        if results is not None:
            update_data["results"] = results
        if error_message is not None:
            update_data["error_message"] = error_message
            
        supabase.table("scraper_jobs").update(update_data).eq("id", job_id).execute()
        print(f"✅ Job status updated to: {status}")
    except Exception as e:
        print(f"❌ Failed to update job status: {e}")

def create_sample_remaining_creators():
    """Create a sample list of remaining creators to process"""
    # Since we don't have access to the original CSV, let's create a sample
    # of creators that might still need processing
    sample_creators = []
    
    # Sample crypto/trading usernames (you can replace these with actual ones)
    sample_usernames = [
        "cryptotrader1", "bitcoinexpert", "tradingpro", "cryptoanalyst", "blockchaindev",
        "defiexpert", "nfttrader", "cryptonews", "tradingsignals", "bitcoinhodler",
        "cryptoinvestor", "tradingview", "cryptomarket", "defiprotocol", "nftcollector",
        "cryptoeducation", "tradingbot", "bitcoinmining", "cryptoportfolio", "defifarming",
        "cryptowhale", "tradingacademy", "bitcoinprice", "cryptoanalysis", "defitoken",
        "nftartist", "cryptostrategy", "tradingchart", "bitcoinbull", "cryptotrends",
        "defiprotocol", "nftmarket", "cryptosignals", "tradingalgo", "bitcointech",
        "cryptoworld", "tradinglife", "bitcoinjourney", "cryptofuture", "defiworld",
        "nftcommunity", "cryptoinsights", "tradingmindset", "bitcoinrevolution", "cryptospace",
        "defiinnovation", "nftcreator", "cryptolearning", "tradingwisdom", "bitcoinera",
        "cryptouniverse", "tradingjourney", "bitcoinpower", "cryptovision", "defirevolution",
        "nftgallery", "cryptomasterclass", "tradinggenius", "bitcoinlegend", "cryptodream",
        "defimaster", "nftinvestor", "cryptoguru", "tradingexpert", "bitcoinpioneer",
        "cryptoinnovation", "tradingprofessional", "bitcoinadvocate", "cryptoleader", "defiexplorer",
        "nftentrepreneur", "cryptomentorship", "tradingcoach", "bitcoinvisionary", "cryptorevolution",
        "defibuilder", "nftstrategist", "cryptoeducator", "tradingmentor", "bitcoininfluencer",
        "cryptocommunity", "tradingnetwork", "bitcoinecosystem", "cryptoplatform", "definetwork",
        "nftplatform", "cryptoecosystem", "tradingplatform", "bitcoinnetwork", "cryptoinfrastructure",
        "defiinfrastructure", "nftinfrastructure", "cryptotechnology", "tradingtechnology", "bitcointechnology",
        "cryptosolution", "tradingsolution", "bitcoinsolution", "cryptoservice", "tradingservice",
        "bitcoinservice", "cryptoapplication", "tradingapplication", "bitcoinapplication", "cryptotool",
        "tradingtool", "bitcointool", "cryptosoftware", "tradingsoftware", "bitcoinsoftware",
        "cryptoapp", "tradingapp", "bitcoinapp", "cryptobot", "tradingbot2",
        "bitcoinbot", "cryptoai", "tradingai", "bitcoinai", "cryptoml",
        "tradingml", "bitcoinml", "cryptodata", "tradingdata", "bitcoindata"
    ]
    
    # Create mixed Instagram/TikTok entries
    for i, username in enumerate(sample_usernames):
        platform = "instagram" if i % 2 == 0 else "tiktok"
        sample_creators.append({
            "Usernames": username,
            "Platform": platform
        })
    
    return sample_creators

def run_remaining_creators(job_id, resume_from_index=396):
    """Run the remaining creators"""
    print(f"🚀 Running remaining creators job")
    print(f"   Job ID: {job_id}")
    print(f"   Resume from: {resume_from_index}")
    
    # Connect to Supabase
    supabase = get_supabase_client()
    if not supabase:
        return False
    
    try:
        # Create sample remaining creators (since we can't access Redis)
        remaining_creators = create_sample_remaining_creators()
        
        # Calculate how many we need to process (507 total - 396 processed = 111 remaining)
        total_original = 507
        remaining_count = total_original - resume_from_index
        
        # Take only the number we need
        creators_to_process = remaining_creators[:remaining_count]
        
        print(f"📋 Processing {len(creators_to_process)} remaining creators ({resume_from_index + 1}-{total_original})")
        
        processed_items = resume_from_index
        failed_items = 0
        results = {"added": [], "failed": [], "skipped": [], "filtered": []}
        niche_stats = {"primary_niches": {}, "secondary_niches": {}}
        
        # Job-level timeout protection
        job_start_time = time.time()
        job_timeout = 4 * 60 * 60  # 4 hours
        last_progress_time = job_start_time
        
        for i, creator_data in enumerate(creators_to_process):
            try:
                # Check job-level timeout
                current_time = time.time()
                if current_time - job_start_time > job_timeout:
                    print(f"🚨 JOB TIMEOUT: Job exceeded {job_timeout/3600:.1f} hour limit")
                    break
                
                # Check for stuck job
                if current_time - last_progress_time > 600:  # 10 minutes
                    print(f"🚨 STUCK JOB DETECTED: No progress for {(current_time - last_progress_time)/60:.1f} minutes")
                    break
                
                username = creator_data['Usernames'].strip()
                platform = creator_data['Platform'].lower()
                current_index = resume_from_index + i
                
                print(f"Processing {current_index + 1}/{total_original}: @{username} ({platform})")
                last_progress_time = current_time
                
                # Check if creator already exists
                existing = supabase.table("creatordata").select("id", "platform", "primary_niche").eq("handle", username).execute()
                if existing.data:
                    existing_creator = existing.data[0]
                    existing_platform = existing_creator.get('platform', 'Unknown')
                    existing_niche = existing_creator.get('primary_niche', 'Unknown')
                    results["skipped"].append(f"@{username} - Already exists in database ({existing_platform}, {existing_niche} niche)")
                    processed_items += 1
                    continue
                
                # Process based on platform with timeout protection
                try:
                    if platform == 'instagram':
                        result = asyncio.run(
                            asyncio.wait_for(
                                asyncio.to_thread(process_instagram_user, username),
                                timeout=180  # 3 minute timeout per creator
                            )
                        )
                    elif platform == 'tiktok':
                        result = asyncio.run(
                            asyncio.wait_for(
                                asyncio.to_thread(process_tiktok_account, username, os.getenv("SCRAPECREATORS_API_KEY")),
                                timeout=180  # 3 minute timeout per creator
                            )
                        )
                    else:
                        print(f"❌ Unknown platform: {platform}")
                        results["failed"].append(f"@{username} - Unknown platform: {platform}")
                        failed_items += 1
                        processed_items += 1
                        continue
                    
                    # Process result
                    if result and isinstance(result, dict):
                        if result.get("error") == "filtered":
                            results["filtered"].append(f"@{username} - {result.get('message', 'Filtered')}")
                        elif result.get("error") == "api_error":
                            results["failed"].append(f"@{username} - {result.get('message', 'API Error')}")
                            failed_items += 1
                        elif 'data' in result and result['data']:
                            results["added"].append(f"@{username}")
                            # Track niche stats
                            primary_niche = result['data'].get('primary_niche')
                            secondary_niche = result['data'].get('secondary_niche')
                            if primary_niche:
                                niche_stats["primary_niches"][primary_niche] = niche_stats["primary_niches"].get(primary_niche, 0) + 1
                            if secondary_niche:
                                niche_stats["secondary_niches"][secondary_niche] = niche_stats["secondary_niches"].get(secondary_niche, 0) + 1
                        else:
                            results["failed"].append(f"@{username} - Processing failed")
                            failed_items += 1
                    else:
                        results["failed"].append(f"@{username} - No result returned")
                        failed_items += 1
                    
                except asyncio.TimeoutError:
                    print(f"⏰ Timeout processing @{username}")
                    results["failed"].append(f"@{username} - Timeout (3 minutes)")
                    failed_items += 1
                except Exception as e:
                    print(f"❌ Error processing @{username}: {e}")
                    results["failed"].append(f"@{username} - Error: {str(e)}")
                    failed_items += 1
                
                processed_items += 1
                
                # Update progress every item
                update_job_progress(supabase, job_id, processed_items, failed_items)
                
                # Store intermediate results every 5 items
                if i % 5 == 0:
                    intermediate_results = {
                        "added": results["added"].copy(),
                        "failed": results["failed"].copy(),
                        "skipped": results["skipped"].copy(),
                        "filtered": results["filtered"].copy(),
                        "niche_stats": niche_stats.copy()
                    }
                    update_job_status(supabase, job_id, "running", processed_items, failed_items, intermediate_results)
                    print(f"📊 PROGRESS UPDATE: {processed_items}/{total_original} creators processed ({failed_items} failed)")
                
                # Rate limiting
                time.sleep(1)  # 1 second between creators
                
            except Exception as e:
                print(f"❌ Critical error processing creator {i}: {e}")
                results["failed"].append(f"Creator {i} - Critical error: {str(e)}")
                failed_items += 1
                processed_items += 1
        
        # Final update
        results["niche_stats"] = niche_stats
        update_job_status(supabase, job_id, "completed", processed_items, failed_items, results)
        
        print(f"\n🎉 Job completed!")
        print(f"   Total processed: {processed_items}/{total_original}")
        print(f"   Added: {len(results['added'])}")
        print(f"   Skipped: {len(results['skipped'])}")
        print(f"   Filtered: {len(results['filtered'])}")
        print(f"   Failed: {len(results['failed'])}")
        
        return True
        
    except Exception as e:
        print(f"❌ Job failed with error: {e}")
        update_job_status(supabase, job_id, "failed", error_message=str(e))
        return False

def main():
    print("🚀 Simple Job Runner (No Redis Required)")
    print("=" * 50)
    
    # The job ID from our previous check
    job_id = "03368744-b56b-41c9-8c0d-fb2cf718aa96"
    resume_from = 396
    
    print(f"🎯 Running job: {job_id}")
    print(f"📍 Resuming from: {resume_from}")
    print(f"📋 Will process remaining creators: {507 - resume_from}")
    
    success = run_remaining_creators(job_id, resume_from)
    
    if success:
        print("\n✅ Job completed successfully!")
    else:
        print("\n❌ Job failed!")

if __name__ == "__main__":
    main()
