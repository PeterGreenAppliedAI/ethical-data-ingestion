# Ethical Data Ingestion System

A legal and ethical data collection system that discovers, catalogs, and provides smart search capabilities from official data sources. Build your own compliant search engine index without worrying about legal issues or robots.txt violations.

## 🎯 What This Does

Instead of scraping random websites (legally questionable), this system:
- ✅ **Only accesses officially permitted data sources**
- ✅ **Respects robots.txt and rate limits**
- ✅ **Checks compliance automatically**
- ✅ **Builds a searchable metadata catalog**
- ✅ **Provides direct links to official datasets**

Think of it as your own ethical alternative to web scraping for research purposes.

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Data Sources  │───▶│  Compliance      │───▶│    DuckDB       │
│                 │    │  Checking        │    │   Database      │
│ • NYC Open Data │    │                  │    │                 │
│ • UN Comtrade   │    │ • robots.txt     │    │ • Metadata      │
│ • SEC EDGAR     │    │ • Rate limits    │    │ • Search Index  │
│ • College Score │    │ • API keys       │    │ • Analytics     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## 📊 Data Sources

| Source | Description | API Key Required | Rate Limit |
|--------|-------------|------------------|------------|
| **NYC Open Data** | 1000+ NYC government datasets | Optional | 1 req/sec |
| **UN Comtrade** | International trade statistics | Required | 0.1 req/sec |
| **SEC EDGAR** | Company financial filings | No | 0.1 req/sec |
| **College Scorecard** | US university/college data | Required | 1 req/sec |

## 🚀 Quick Start

### 1. Installation

**Core Dependencies (Required):**
```bash
pip install aiohttp click beautifulsoup4 duckdb pandas rich
```

**Optional ML Dependencies (for advanced features):**
```bash
pip install sentence-transformers numpy scikit-learn tiktoken
```

### 2. Setup

```bash
# Basic setup (works without API keys for NYC and SEC)
python3 ethical_crawler.py setup

# Full setup with API keys
python3 ethical_crawler.py setup \
  --un-api-key YOUR_UN_KEY \
  --college-api-key YOUR_COLLEGE_KEY \
  --nyc-api-key YOUR_NYC_KEY
```

### 3. Collect Data

```bash
# Collect metadata from all compliant sources
python3 ethical_crawler.py collect
```

### 4. Search & Analyze

```bash
# Search for datasets
python3 ethical_crawler.py search --query "transportation"

# Advanced search with filters
python3 ethical_crawler.py search --query "economics" --source "UN Comtrade"

# View analytics
python3 ethical_crawler.py analytics

# Check system status
python3 ethical_crawler.py status
```

## 🔑 API Keys

### Required APIs:
- **UN Comtrade**: [Register here](https://comtradeapi.un.org/) (Free)
- **College Scorecard**: [Get key here](https://api.data.gov/signup/) (Free)

### Optional APIs:
- **NYC Open Data**: [Get token here](https://data.cityofnewyork.us/profile/app_tokens) (Increases rate limits)

### No API Key Needed:
- **SEC EDGAR**: Public domain data

## 📖 Commands Reference

### Setup Commands
```bash
# Initialize system and verify compliance
ethical_crawler.py setup [OPTIONS]

Options:
  --nyc-api-key TEXT      NYC Open Data API key (optional)
  --un-api-key TEXT       UN Comtrade API key (required)
  --college-api-key TEXT  College Scorecard API key (required)
  --db TEXT              Database file path (default: ethical_data.duckdb)
```

### Data Collection
```bash
# Collect from all compliant sources
ethical_crawler.py collect [OPTIONS]

# Same API key options as setup
```

### Search & Discovery
```bash
# Search collected datasets
ethical_crawler.py search --query QUERY [OPTIONS]

Options:
  --query TEXT           Search query (required)
  --source TEXT          Filter by data source
  --data-type TEXT       Filter by data type
  --limit INTEGER        Maximum results (default: 10)
```

### Analytics & Status
```bash
# Show comprehensive analytics
ethical_crawler.py analytics

# Show system status
ethical_crawler.py status

# Export data
ethical_crawler.py export [OPTIONS]

Export Options:
  --format [parquet|csv|json]  Export format (default: parquet)
  --output TEXT               Output file path
  --where TEXT                SQL WHERE clause for filtering
```

## 🛡️ Compliance Features

### Automatic Compliance Checking
- ✅ **Robots.txt verification**: Checks and respects robots.txt files
- ✅ **Rate limiting**: Enforces conservative request rates
- ✅ **API validation**: Verifies API keys and permissions
- ✅ **Terms of service**: Links to official ToS for each source

### Ethical Data Practices
- 📝 **Metadata only**: Stores descriptions, not full content
- 🔗 **Source attribution**: Maintains links to original data
- ⚖️ **License tracking**: Records data licenses and usage terms
- 📊 **Audit trail**: Complete compliance and collection history

## 💾 Database Schema

### Core Tables
```sql
-- Dataset metadata
data_records (
    id, source_name, record_id, title, description,
    data_type, url, metadata, tags, last_updated,
    ingested_at, file_format, size_bytes, license_info
)

-- Compliance tracking
compliance_checks (
    id, source_name, check_time, robots_txt_compliant,
    rate_limit_compliant, terms_compliant, api_key_valid,
    issues, recommendations
)

-- Source registry
data_sources (
    name, base_url, api_endpoint, requires_api_key,
    rate_limit_per_second, terms_of_service_url,
    robots_txt_url, data_license
)
```

## 🔍 Example Workflows

### Research Workflow
```bash
# 1. Setup system
python3 ethical_crawler.py setup --un-api-key KEY --college-api-key KEY

# 2. Collect data
python3 ethical_crawler.py collect

# 3. Research urban planning
python3 ethical_crawler.py search --query "transportation urban"

# 4. Get specific NYC data
python3 ethical_crawler.py search --query "traffic" --source "NYC Open Data"

# 5. Export results for analysis
python3 ethical_crawler.py export --format csv --output research_data
```

### Data Science Pipeline
```bash
# 1. Collect everything
python3 ethical_crawler.py collect

# 2. Analyze what's available
python3 ethical_crawler.py analytics

# 3. Export specific datasets
python3 ethical_crawler.py export \
  --format parquet \
  --where "data_type = 'dataset' AND tags @> '{economics}'" \
  --output economics_datasets

# 4. Use in Python
import pandas as pd
df = pd.read_parquet('economics_datasets.parquet')
```

## 🏃‍♂️ Performance

### Database Performance
- **DuckDB**: Columnar storage optimized for analytics
- **Indexing**: Optimized for source, type, and temporal queries
- **Full-text search**: Built-in search capabilities
- **Compression**: Efficient storage of metadata

### Rate Limiting
- **Conservative defaults**: Never exceeds API limits
- **Adaptive delays**: Automatic request spacing
- **Compliance first**: Prioritizes legal access over speed

## 🔧 Configuration

### Database Location
```bash
# Custom database location
python3 ethical_crawler.py setup --db /path/to/my_data.duckdb
```

### Advanced DuckDB Queries
```python
import duckdb

# Connect to your database
conn = duckdb.connect('ethical_data.duckdb')

# Custom analytics
result = conn.execute("""
    SELECT source_name, COUNT(*) as datasets,
           AVG(size_bytes) as avg_size
    FROM data_records 
    WHERE ingested_at >= '2025-01-01'
    GROUP BY source_name
    ORDER BY datasets DESC
""").df()
```

## 🚨 Troubleshooting

### Common Issues

**Import Errors:**
```bash
# Install missing dependencies
pip install aiohttp click beautifulsoup4 duckdb pandas rich
```

**API Key Issues:**
```bash
# Verify compliance status
python3 ethical_crawler.py status

# Check specific source
python3 ethical_crawler.py setup --un-api-key YOUR_KEY
```

**Database Issues:**
```bash
# Reset database
rm ethical_data.duckdb
python3 ethical_crawler.py setup
```

**Rate Limiting:**
- The system is intentionally conservative
- Check compliance status if sources are being skipped
- API keys often increase rate limits

## 📈 Roadmap

### Current Status: ✅ MVP Complete
- [x] Four official data sources
- [x] Compliance checking
- [x] DuckDB storage
- [x] Search and analytics
- [x] CLI interface

### Future Enhancements
- [ ] Additional data sources (World Bank, OECD, etc.)
- [ ] Web interface
- [ ] Scheduled data updates
- [ ] Advanced ML-powered search
- [ ] Data quality scoring
- [ ] Export to cloud platforms

## 📄 License & Ethics

### Project License
MIT License - Use freely for research and commercial purposes

### Data Source Licenses
- **NYC Open Data**: Public Domain
- **UN Comtrade**: Creative Commons Attribution 4.0
- **SEC EDGAR**: Public Domain  
- **College Scorecard**: Public Domain

### Ethical Commitment
This system is designed to be a **responsible alternative to web scraping**:
- Only accesses data that is explicitly made available
- Respects rate limits and technical constraints
- Maintains source attribution and licensing
- Provides audit trails for compliance verification

## 🤝 Contributing

### Adding New Data Sources
1. Create adapter in `adapters/` following `BaseAdapter` pattern
2. Add source configuration
3. Implement compliance checking
4. Add tests and documentation

### Development Setup
```bash
git clone <your-repo>
cd ethical-data-ingestion
pip install -e ".[dev]"
```

## 📞 Support

For issues, questions, or new data source suggestions:
- Check existing datasets: `python3 ethical_crawler.py status`
- Review compliance: `python3 ethical_crawler.py analytics`
- Verify setup: `python3 ethical_crawler.py setup --help`

---



---

*Built with Python, DuckDB, and a commitment to ethical data practices.*
