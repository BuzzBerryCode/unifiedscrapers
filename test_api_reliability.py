#!/usr/bin/env python3
"""
Test script for API reliability fixes
=====================================

This script tests the new API reliability system against creators that
previously failed to ensure the improvements work as expected.
"""

import sys
import os
import time
from datetime import datetime

# Add current directory to path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from improved_scrapers import improved_scrape_instagram_user_data, improved_scrape_tiktok_user_data
from api_reliability_fix import get_api_manager

# Test creators that previously failed (from your job results)
FAILED_CREATORS = {
    'instagram': [
        'levelupcreditconsulting',
        'groupchatnews', 
        'thestewardshipcoach',
        'bankrate',
        'frnzaps'
    ],
    'tiktok': [
        'investingcameronscrubs',
        'creditrepairfraud',
        'tradeshipuniversity', 
        'sierraxilyah',
        'mohammaedshahid8791'
    ]
}

def test_instagram_reliability():
    """Test Instagram API reliability."""
    print("\n" + "="*60)
    print("🔬 TESTING INSTAGRAM API RELIABILITY")
    print("="*60)
    
    results = {'success': 0, 'failed': 0, 'error_types': {}}
    
    for username in FAILED_CREATORS['instagram']:
        print(f"\n🧪 Testing Instagram: @{username}")
        start_time = time.time()
        
        try:
            result = improved_scrape_instagram_user_data(username)
            processing_time = time.time() - start_time
            
            if result is None:
                print(f"❌ Permanent failure for @{username} ({processing_time:.2f}s)")
                results['failed'] += 1
                results['error_types']['permanent_failure'] = results['error_types'].get('permanent_failure', 0) + 1
                
            elif isinstance(result, dict) and 'error' in result:
                error_type = result['error']
                error_msg = result['message']
                print(f"⏳ Temporary error for @{username}: {error_type} - {error_msg} ({processing_time:.2f}s)")
                results['failed'] += 1
                results['error_types'][error_type] = results['error_types'].get(error_type, 0) + 1
                
            elif isinstance(result, dict) and 'skipped' in result:
                print(f"🚫 Skipped @{username}: {result['reason']} ({processing_time:.2f}s)")
                results['failed'] += 1
                results['error_types']['skipped'] = results['error_types'].get('skipped', 0) + 1
                
            else:
                print(f"✅ Success for @{username} ({processing_time:.2f}s)")
                print(f"   📊 {result.get('followers_count', 0):,} followers, {result.get('engagement_rate', 0)}% engagement")
                results['success'] += 1
                
        except Exception as e:
            processing_time = time.time() - start_time
            print(f"💥 Exception for @{username}: {e} ({processing_time:.2f}s)")
            results['failed'] += 1
            results['error_types']['exception'] = results['error_types'].get('exception', 0) + 1
        
        # Small delay between tests
        time.sleep(1)
    
    return results

def test_tiktok_reliability():
    """Test TikTok API reliability."""
    print("\n" + "="*60)
    print("🔬 TESTING TIKTOK API RELIABILITY")
    print("="*60)
    
    results = {'success': 0, 'failed': 0, 'error_types': {}}
    
    for username in FAILED_CREATORS['tiktok']:
        print(f"\n🧪 Testing TikTok: @{username}")
        start_time = time.time()
        
        try:
            result = improved_scrape_tiktok_user_data(username)
            processing_time = time.time() - start_time
            
            if result is None:
                print(f"❌ Permanent failure for @{username} ({processing_time:.2f}s)")
                results['failed'] += 1
                results['error_types']['permanent_failure'] = results['error_types'].get('permanent_failure', 0) + 1
                
            elif isinstance(result, dict) and 'error' in result:
                error_type = result['error']
                error_msg = result['message']
                print(f"⏳ Temporary error for @{username}: {error_type} - {error_msg} ({processing_time:.2f}s)")
                results['failed'] += 1
                results['error_types'][error_type] = results['error_types'].get(error_type, 0) + 1
                
            elif isinstance(result, dict) and 'skipped' in result:
                print(f"🚫 Skipped @{username}: {result['reason']} ({processing_time:.2f}s)")
                results['failed'] += 1
                results['error_types']['skipped'] = results['error_types'].get('skipped', 0) + 1
                
            else:
                print(f"✅ Success for @{username} ({processing_time:.2f}s)")
                print(f"   📊 {result.get('followers_count', 0):,} followers, {result.get('engagement_rate', 0)}% engagement")
                results['success'] += 1
                
        except Exception as e:
            processing_time = time.time() - start_time
            print(f"💥 Exception for @{username}: {e} ({processing_time:.2f}s)")
            results['failed'] += 1
            results['error_types']['exception'] = results['error_types'].get('exception', 0) + 1
        
        # Small delay between tests
        time.sleep(1)
    
    return results

def print_test_summary(instagram_results, tiktok_results):
    """Print comprehensive test results."""
    print("\n" + "="*60)
    print("📊 API RELIABILITY TEST SUMMARY")
    print("="*60)
    
    total_tests = len(FAILED_CREATORS['instagram']) + len(FAILED_CREATORS['tiktok'])
    total_success = instagram_results['success'] + tiktok_results['success']
    total_failed = instagram_results['failed'] + tiktok_results['failed']
    
    print(f"\n🎯 Overall Results:")
    print(f"   Total Tests: {total_tests}")
    print(f"   ✅ Successes: {total_success} ({total_success/total_tests*100:.1f}%)")
    print(f"   ❌ Failures: {total_failed} ({total_failed/total_tests*100:.1f}%)")
    
    print(f"\n📱 Instagram Results:")
    ig_total = len(FAILED_CREATORS['instagram'])
    print(f"   ✅ Success: {instagram_results['success']}/{ig_total} ({instagram_results['success']/ig_total*100:.1f}%)")
    print(f"   ❌ Failed: {instagram_results['failed']}/{ig_total} ({instagram_results['failed']/ig_total*100:.1f}%)")
    
    if instagram_results['error_types']:
        print("   Error breakdown:")
        for error_type, count in instagram_results['error_types'].items():
            print(f"     • {error_type}: {count}")
    
    print(f"\n🎵 TikTok Results:")
    tt_total = len(FAILED_CREATORS['tiktok'])
    print(f"   ✅ Success: {tiktok_results['success']}/{tt_total} ({tiktok_results['success']/tt_total*100:.1f}%)")
    print(f"   ❌ Failed: {tiktok_results['failed']}/{tt_total} ({tiktok_results['failed']/tt_total*100:.1f}%)")
    
    if tiktok_results['error_types']:
        print("   Error breakdown:")
        for error_type, count in tiktok_results['error_types'].items():
            print(f"     • {error_type}: {count}")
    
    # Success criteria
    success_rate = total_success / total_tests * 100
    print(f"\n📈 Performance Analysis:")
    
    if success_rate >= 90:
        print(f"   🎉 EXCELLENT: {success_rate:.1f}% success rate!")
        print("   ✅ Ready for production deployment")
    elif success_rate >= 75:
        print(f"   👍 GOOD: {success_rate:.1f}% success rate")
        print("   ✅ Significant improvement over 92% failure rate")
    elif success_rate >= 50:
        print(f"   ⚠️  MODERATE: {success_rate:.1f}% success rate")
        print("   ✅ Still better than before, but room for improvement")
    else:
        print(f"   ❌ POOR: {success_rate:.1f}% success rate")
        print("   🔧 Further debugging needed")
    
    # Check circuit breaker status
    api_manager = get_api_manager("test_key")
    if api_manager.failure_count:
        print(f"\n🔴 Circuit Breaker Status:")
        for endpoint, failures in api_manager.failure_count.items():
            print(f"   {endpoint}: {failures['count']} failures")

if __name__ == "__main__":
    print("🚀 STARTING API RELIABILITY TESTS")
    print(f"📅 Test Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\n🎯 Testing previously failed creators to measure improvement...")
    
    start_time = time.time()
    
    # Run tests
    instagram_results = test_instagram_reliability()
    tiktok_results = test_tiktok_reliability()
    
    total_time = time.time() - start_time
    
    # Print comprehensive summary
    print_test_summary(instagram_results, tiktok_results)
    
    print(f"\n⏱️  Total Test Time: {total_time:.2f} seconds")
    print(f"💾 Test completed - results saved to console")
    
    print("\n" + "="*60)
    print("🔧 NEXT STEPS:")
    print("="*60)
    print("1. If success rate >75%: Apply fixes to production")  
    print("2. If success rate <75%: Debug remaining issues")
    print("3. Monitor production error rates after deployment")
    print("4. Update retry thresholds if needed")
    print("\n📖 See APPLY_API_RELIABILITY_FIXES.md for deployment guide")
