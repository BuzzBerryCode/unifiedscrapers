# 🚀 Unified Scrapers

A comprehensive, high-performance scraping system for Instagram and TikTok creators with automatic platform detection, concurrent processing, and intelligent API credit optimization.

## 📁 Repository Structure

```
unifiedscrapers/
├── UnifiedRescaper.py        # 🔄 MAIN RESCAPER (rescrapes existing creators)
├── UnifiedScraper.py         # ➕ NEW CREATOR SCRAPER (adds from Excel)
├── add_updated_at_column.sql # 🗄️ DATABASE schema update
├── backup_old_scrapers/      # 📦 Backup of old individual scrapers
└── README.md                 # 📖 This file
```

## ✨ Key Features

### 🎯 **UnifiedRescaper.py** - Main Rescaper
- **Platform Auto-Detection**: Automatically routes Instagram/TikTok creators to appropriate scrapers
- **Concurrent Processing**: 3-5x faster with configurable batch processing
- **Real-time Progress**: Live progress bars with timing estimates
- **Resume Functionality**: Continue from any specific creator
- **Activity Filtering**: Removes creators inactive for 45+ days
- **Media Processing**: Downloads profile images and post media to Supabase storage
- **Buzz Score Calculation**: Advanced algorithm for creator scoring
- **Percentage Changes**: Accurate growth/decline tracking
- **API Optimization**: Smart credit usage with early follower filtering
- **Test/Production Modes**: Safe testing with limited creators

### 📊 **UnifiedScraper.py** - New Creator Scraper  
- **Excel Integration**: Processes creators from `CreatorList.xlsx`
- **Niche Classification**: AI-powered categorization (Trading/Crypto/Finance)
- **Follower Filtering**: 10k-350k range validation
- **Duplicate Prevention**: Checks existing database before adding
- **Media Processing**: Full media download and storage

### 🗄️ **Database Integration**
- **Timestamp Tracking**: `created_at` and `updated_at` fields
- **Change Tracking**: Percentage changes for all metrics
- **Buzz Scoring**: Growth, engagement, and consistency metrics
- **Media URLs**: Supabase storage integration

## 🚀 Quick Start

### Prerequisites
```bash
pip install google-generativeai requests pandas supabase tqdm pillow pillow-heif aiohttp
```

### Configuration
Update API keys in the scripts:
- `SCRAPECREATORS_API_KEY` - Your ScrapeCreators API key
- `GEMINI_API_KEY` - Google AI API key
- `SUPABASE_URL` and `SUPABASE_KEY` - Your Supabase credentials

### Database Setup
```sql
-- Run this SQL in your Supabase database
ALTER TABLE public.creatordata 
ADD COLUMN updated_at timestamp without time zone;

COMMENT ON COLUMN public.creatordata.updated_at IS 'Timestamp when the record was last updated/rescraped';

CREATE INDEX idx_creatordata_updated_at ON public.creatordata(updated_at);
```

## 📖 Usage

### Rescraping Existing Creators

**Test Mode** (recommended first run):
```bash
python3 UnifiedRescaper.py --test
```

**Production Mode** (all creators):
```bash
python3 UnifiedRescaper.py --prod
```

**Resume from Specific Creator**:
```bash
python3 UnifiedRescaper.py philipasaya
```

### Adding New Creators

1. Create `CreatorList.xlsx` with columns: `Usernames`, `Platform`
2. Run the scraper:
```bash
python3 UnifiedScraper.py
```

## ⚡ Performance

### Concurrent Processing
- **Batch Size**: 2 creators simultaneously (configurable)
- **Rate Limiting**: 2-second delays between batches
- **Speedup**: 3-5x faster than sequential processing
- **Memory**: Moderate increase due to concurrent operations

### API Credit Optimization
- **Instagram**: 1-2 credits per creator (1 if out of range, 2 if in range)
- **TikTok**: 1 credit per creator (profile + posts in single call)
- **Smart Filtering**: Early follower range checking saves credits
- **No Redundant Calls**: Each API call serves a specific purpose

## 📊 Sample Output

```
🚀 STARTING UNIFIED RESCRAPER SCRIPT 🚀
============================================================
This script will:
  1. Cleanup Phase: Rescrape existing creators and remove inactive ones
  2. Show real-time progress with ETA and timing estimates
  3. Use concurrent processing for 3-5x speedup
  4. Handle both Instagram and TikTok creators automatically
============================================================

Fetching existing creators from Supabase...
Found 1000 existing creators in the target niches.
🧪 TEST MODE: Limiting to first 100 creators
🔍 Rescraping 100 creators to get created_at data and check activity...
📊 Platform breakdown:
   • TikTok: 35 creators
   • Instagram: 65 creators

📦 Processing 100 creators in 50 batches of 2
Rescraping and checking creators: 100%|██████████| 100/100 [15:30<00:00, 9.30s/it]

📊 Unified Rescaper Complete:
   • Creators updated: 95
   • Inactive creators deleted: 5
   • Errors encountered: 0
   • Total creators processed: 100
   • Total time elapsed: 0:15:30
   • Average time per creator: 9.3 seconds
```

## 🔧 Configuration Options

### Test Mode Settings
```python
TEST_MODE = False           # Enable/disable test mode
TEST_LIMIT = 100           # Number of creators in test mode
```

### Concurrent Processing
```python
CLEANUP_BATCH_SIZE = 2     # Creators processed simultaneously
BATCH_DELAY = 2.0          # Seconds between batches
```

### Media Processing
```python
MAX_RECENT_POSTS = 4       # Number of post media files to download
BUCKET_NAME = "profile-media"  # Supabase storage bucket
```

## 🛡️ Error Handling

- **API Failures**: Automatic retry with fallback to sequential processing
- **Rate Limiting**: Built-in delays and semaphore controls
- **Data Validation**: Comprehensive input validation and sanitization
- **Resume Support**: Continue from any point if interrupted
- **Graceful Degradation**: Continues processing even if individual creators fail

## 📈 Metrics Tracked

### Creator Metrics
- Follower count and growth percentage
- Engagement rate and changes
- Average views, likes, comments
- Buzz Score (growth + engagement + consistency)
- Activity status (45-day threshold)

### Performance Metrics
- Processing time per creator
- API credits used
- Success/failure rates
- Concurrent processing efficiency

## 🔄 Migration from Old Scrapers

The unified system replaces these individual scrapers:
- ~~FinalInstaRescraper.py~~ → UnifiedRescaper.py
- ~~FinalTiktokRescraper.py~~ → UnifiedRescaper.py  
- ~~InstagramScraper.py~~ → UnifiedScraper.py
- ~~TiktokRescraper.py~~ → UnifiedScraper.py

All functionality has been preserved and enhanced in the unified system.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 📄 License

This project is proprietary software owned by BuzzBerry Code.

## 🆘 Support

For issues or questions:
1. Check the error logs for specific error messages
2. Verify API keys and database connections
3. Test with `--test` mode first
4. Check the backup_old_scrapers/ folder for reference implementations

---

**Built with ❤️ by BuzzBerry Code**
