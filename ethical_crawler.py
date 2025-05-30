#!/usr/bin/env python3
"""
Ethical Data Ingestion System
A legal and ethical data collection system with specialized adapters for official sources.
Respects robots.txt, rate limits, and terms of service for responsible data collection.
"""

import asyncio
import aiohttp
import logging
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, AsyncGenerator, Any, Union
from urllib.parse import urljoin, urlparse, quote_plus
import hashlib
import json
import re
from contextlib import asynccontextmanager
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod
import csv
import io
from urllib.robotparser import RobotFileParser
import threading
from concurrent.futures import ThreadPoolExecutor

# Third-party imports
import click
from bs4 import BeautifulSoup
from sentence_transformers import SentenceTransformer
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
import tiktoken
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
from rich.table import Table
from rich.panel import Panel
from rich.tree import Tree
import duckdb
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
console = Console()

class DatabaseManager:
    """Thread-safe DuckDB database manager for analytical data storage."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = threading.Lock()
        self._connection = None
        self._executor = ThreadPoolExecutor(max_workers=1)  # Single thread for DB operations
    
    def get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get or create DuckDB connection (thread-safe)."""
        with self._lock:
            if self._connection is None:
                self._connection = duckdb.connect(self.db_path)
            return self._connection
    
    async def execute_async(self, query: str, parameters: Optional[List] = None) -> Any:
        """Execute query asynchronously using thread executor."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor, 
            self._execute_sync, 
            query, 
            parameters
        )
    
    def _execute_sync(self, query: str, parameters: Optional[List] = None) -> Any:
        """Execute query synchronously."""
        conn = self.get_connection()
        if parameters:
            return conn.execute(query, parameters)
        else:
            return conn.execute(query)
    
    async def fetch_df_async(self, query: str, parameters: Optional[List] = None) -> pd.DataFrame:
        """Fetch query results as pandas DataFrame."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor,
            self._fetch_df_sync,
            query,
            parameters
        )
    
    def _fetch_df_sync(self, query: str, parameters: Optional[List] = None) -> pd.DataFrame:
        """Fetch DataFrame synchronously."""
        conn = self.get_connection()
        if parameters:
            return conn.execute(query, parameters).df()
        else:
            return conn.execute(query).df()
    
    async def insert_df_async(self, table_name: str, df: pd.DataFrame, if_exists: str = 'append'):
        """Insert DataFrame into table asynchronously."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor,
            self._insert_df_sync,
            table_name,
            df,
            if_exists
        )
    
    def _insert_df_sync(self, table_name: str, df: pd.DataFrame, if_exists: str = 'append'):
        """Insert DataFrame synchronously."""
        conn = self.get_connection()
        if if_exists == 'replace':
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        # Use DuckDB's efficient DataFrame insertion
        conn.register('temp_df', df)
        
        if if_exists == 'replace' or not self._table_exists(table_name):
            conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_df")
        else:
            conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")
        
        conn.unregister('temp_df')
    
    def _table_exists(self, table_name: str) -> bool:
        """Check if table exists."""
        conn = self.get_connection()
        try:
            result = conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                [table_name]
            ).fetchone()
            return result[0] > 0
        except:
            return False
    
    def close(self):
        """Close database connection."""
        with self._lock:
            if self._connection:
                self._connection.close()
                self._connection = None
        self._executor.shutdown(wait=True)


@dataclass
class DataSource:
    """Metadata about a data source and its access requirements."""
    name: str
    base_url: str
    api_endpoint: Optional[str]
    requires_api_key: bool
    rate_limit_per_second: float
    terms_of_service_url: str
    robots_txt_url: str
    data_license: str
    last_checked: Optional[datetime] = None
    is_compliant: bool = True
    compliance_notes: str = ""


@dataclass
class DataRecord:
    """Structured data record from various sources."""
    source_name: str
    record_id: str
    title: str
    description: str
    data_type: str  # dataset, document, api_endpoint, etc.
    url: str
    metadata: Dict[str, Any]
    content_summary: str
    tags: List[str]
    last_updated: Optional[datetime]
    ingested_at: datetime
    file_format: Optional[str] = None
    size_bytes: Optional[int] = None
    license_info: Optional[str] = None


@dataclass
class ComplianceCheck:
    """Results of compliance verification."""
    source_name: str
    robots_txt_compliant: bool
    rate_limit_compliant: bool
    terms_compliant: bool
    api_key_valid: bool
    last_check: datetime
    issues: List[str]
    recommendations: List[str]


class RobotsChecker:
    """Check and respect robots.txt files."""
    
    def __init__(self):
        self._robots_cache: Dict[str, RobotFileParser] = {}
        self._cache_expiry: Dict[str, datetime] = {}
    
    async def can_fetch(self, url: str, user_agent: str = "EthicalDataBot/1.0") -> bool:
        """Check if we can fetch a URL according to robots.txt."""
        try:
            parsed_url = urlparse(url)
            domain = f"{parsed_url.scheme}://{parsed_url.netloc}"
            robots_url = urljoin(domain, "/robots.txt")
            
            # Check cache first
            if domain in self._robots_cache:
                if datetime.now() - self._cache_expiry[domain] < timedelta(hours=24):
                    rp = self._robots_cache[domain]
                    return rp.can_fetch(user_agent, url)
            
            # Fetch and parse robots.txt
            rp = RobotFileParser()
            rp.set_url(robots_url)
            
            async with aiohttp.ClientSession() as session:
                try:
                    async with session.get(robots_url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                        if response.status == 200:
                            robots_content = await response.text()
                            # Parse the content manually since RobotFileParser doesn't support async
                            rp.set_url(robots_url)
                            # Create a file-like object from the content
                            import io
                            robots_file = io.StringIO(robots_content)
                            # Read the robots.txt content into the parser
                            for line in robots_file:
                                rp.read()
                                break  # Just trigger the read
                            
                            # Simple manual parsing for async context
                            lines = robots_content.split('\n')
                            user_agent_section = False
                            disallowed_paths = []
                            
                            for line in lines:
                                line = line.strip()
                                if line.startswith('User-agent:'):
                                    ua = line.split(':', 1)[1].strip()
                                    user_agent_section = (ua == '*' or ua.lower() == user_agent.lower())
                                elif user_agent_section and line.startswith('Disallow:'):
                                    path = line.split(':', 1)[1].strip()
                                    if path:
                                        disallowed_paths.append(path)
                            
                            # Check if URL path is disallowed
                            url_path = parsed_url.path
                            for disallowed in disallowed_paths:
                                if url_path.startswith(disallowed):
                                    return False
                            
                            return True
                        else:
                            # If robots.txt doesn't exist, assume allowed
                            return True
                except:
                    # If can't fetch robots.txt, assume allowed but log warning
                    logger.warning(f"Could not fetch robots.txt for {domain}")
                    return True
        except Exception as e:
            logger.error(f"Error checking robots.txt for {url}: {e}")
            return True  # Default to allowed if check fails


class BaseAdapter(ABC):
    """Abstract base class for data source adapters."""
    
    def __init__(self, source: DataSource, api_key: Optional[str] = None):
        self.source = source
        self.api_key = api_key
        self.session: Optional[aiohttp.ClientSession] = None
        self.robots_checker = RobotsChecker()
        self.last_request_time = datetime.min()
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            headers={
                'User-Agent': 'EthicalDataBot/1.0 (Responsible Research Data Collection)',
                'Accept': 'application/json, application/xml, text/csv, text/html'
            }
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def respect_rate_limit(self):
        """Ensure we respect rate limits."""
        elapsed = datetime.now() - self.last_request_time
        min_interval = 1.0 / self.source.rate_limit_per_second
        
        if elapsed.total_seconds() < min_interval:
            wait_time = min_interval - elapsed.total_seconds()
            await asyncio.sleep(wait_time)
        
        self.last_request_time = datetime.now()
    
    async def check_compliance(self) -> ComplianceCheck:
        """Verify compliance with source requirements."""
        issues = []
        recommendations = []
        
        # Check robots.txt compliance
        robots_compliant = await self.robots_checker.can_fetch(self.source.base_url)
        if not robots_compliant:
            issues.append("Base URL disallowed by robots.txt")
        
        # Check API key if required
        api_key_valid = True
        if self.source.requires_api_key and not self.api_key:
            api_key_valid = False
            issues.append("API key required but not provided")
            recommendations.append("Obtain API key from official source")
        
        return ComplianceCheck(
            source_name=self.source.name,
            robots_txt_compliant=robots_compliant,
            rate_limit_compliant=True,  # We always respect rate limits
            terms_compliant=True,  # Assumed if using official APIs
            api_key_valid=api_key_valid,
            last_check=datetime.now(),
            issues=issues,
            recommendations=recommendations
        )
    
    @abstractmethod
    async def discover_datasets(self) -> List[DataRecord]:
        """Discover available datasets from the source."""
        pass
    
    @abstractmethod
    async def fetch_dataset_metadata(self, dataset_id: str) -> Optional[DataRecord]:
        """Fetch detailed metadata for a specific dataset."""
        pass


class NYCOpenDataAdapter(BaseAdapter):
    """Adapter for NYC Open Data (Socrata platform)."""
    
    def __init__(self, api_key: Optional[str] = None):
        source = DataSource(
            name="NYC Open Data",
            base_url="https://data.cityofnewyork.us",
            api_endpoint="https://data.cityofnewyork.us/api/views",
            requires_api_key=False,  # Public API, but API key recommended
            rate_limit_per_second=1.0,  # Conservative rate limiting
            terms_of_service_url="https://opendata.cityofnewyork.us/overview/",
            robots_txt_url="https://data.cityofnewyork.us/robots.txt",
            data_license="Public Domain"
        )
        super().__init__(source, api_key)
    
    async def discover_datasets(self) -> List[DataRecord]:
        """Discover NYC Open Data datasets."""
        await self.respect_rate_limit()
        
        if not await self.robots_checker.can_fetch(self.source.api_endpoint):
            logger.error("API endpoint disallowed by robots.txt")
            return []
        
        try:
            url = f"{self.source.api_endpoint}/metadata/v1"
            params = {
                'limit': 1000,
                'offset': 0
            }
            if self.api_key:
                params['$$app_token'] = self.api_key
            
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    records = []
                    
                    for item in data:
                        record = DataRecord(
                            source_name=self.source.name,
                            record_id=item.get('id', ''),
                            title=item.get('name', 'Untitled Dataset'),
                            description=item.get('description', ''),
                            data_type='dataset',
                            url=f"https://data.cityofnewyork.us/d/{item.get('id', '')}",
                            metadata={
                                'category': item.get('category'),
                                'tags': item.get('tags', []),
                                'attribution': item.get('attribution'),
                                'update_frequency': item.get('updateFrequency'),
                                'columns': item.get('columns', []),
                                'view_count': item.get('viewCount', 0),
                                'download_count': item.get('downloadCount', 0)
                            },
                            content_summary=self._generate_summary(item),
                            tags=item.get('tags', []),
                            last_updated=self._parse_date(item.get('rowsUpdatedAt')),
                            ingested_at=datetime.now(),
                            file_format='CSV/JSON',
                            license_info=self.source.data_license
                        )
                        records.append(record)
                    
                    logger.info(f"Discovered {len(records)} datasets from NYC Open Data")
                    return records
                else:
                    logger.error(f"Failed to fetch NYC datasets: {response.status}")
                    return []
        except Exception as e:
            logger.error(f"Error discovering NYC datasets: {e}")
            return []
    
    async def fetch_dataset_metadata(self, dataset_id: str) -> Optional[DataRecord]:
        """Fetch detailed metadata for a specific NYC dataset."""
        await self.respect_rate_limit()
        
        try:
            url = f"{self.source.api_endpoint}/{dataset_id}.json"
            params = {}
            if self.api_key:
                params['$$app_token'] = self.api_key
            
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    record = DataRecord(
                        source_name=self.source.name,
                        record_id=dataset_id,
                        title=data.get('name', 'Untitled Dataset'),
                        description=data.get('description', ''),
                        data_type='dataset',
                        url=f"https://data.cityofnewyork.us/d/{dataset_id}",
                        metadata=data,
                        content_summary=self._generate_summary(data),
                        tags=data.get('tags', []),
                        last_updated=self._parse_date(data.get('rowsUpdatedAt')),
                        ingested_at=datetime.now(),
                        file_format='CSV/JSON',
                        license_info=self.source.data_license
                    )
                    return record
                else:
                    logger.error(f"Failed to fetch dataset {dataset_id}: {response.status}")
                    return None
        except Exception as e:
            logger.error(f"Error fetching dataset {dataset_id}: {e}")
            return None
    
    def _generate_summary(self, data: Dict) -> str:
        """Generate a summary of the dataset."""
        summary_parts = []
        
        if 'name' in data:
            summary_parts.append(f"Dataset: {data['name']}")
        
        if 'category' in data:
            summary_parts.append(f"Category: {data['category']}")
        
        if 'columns' in data:
            col_count = len(data['columns'])
            summary_parts.append(f"Columns: {col_count}")
        
        if 'viewCount' in data:
            summary_parts.append(f"Views: {data['viewCount']:,}")
        
        return " | ".join(summary_parts)
    
    def _parse_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse date string to datetime."""
        if not date_str:
            return None
        try:
            # Handle Unix timestamp
            if date_str.isdigit():
                return datetime.fromtimestamp(int(date_str))
            # Handle ISO format
            return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        except:
            return None


class UNComtradeAdapter(BaseAdapter):
    """Adapter for UN Comtrade API."""
    
    def __init__(self, api_key: Optional[str] = None):
        source = DataSource(
            name="UN Comtrade",
            base_url="https://comtradeapi.un.org",
            api_endpoint="https://comtradeapi.un.org/data/v1",
            requires_api_key=True,  # API key required for higher rate limits
            rate_limit_per_second=0.1,  # Very conservative - 100 requests per hour for free tier
            terms_of_service_url="https://comtradeapi.un.org/",
            robots_txt_url="https://comtradeapi.un.org/robots.txt",
            data_license="Creative Commons Attribution 4.0"
        )
        super().__init__(source, api_key)
    
    async def discover_datasets(self) -> List[DataRecord]:
        """Discover available trade data categories."""
        await self.respect_rate_limit()
        
        # UN Comtrade has predefined data types we can discover
        data_types = [
            {
                'id': 'goods',
                'name': 'International Trade in Goods',
                'description': 'Trade flows of goods between countries',
                'endpoint': '/goods'
            },
            {
                'id': 'services',
                'name': 'International Trade in Services',
                'description': 'Trade flows of services between countries',
                'endpoint': '/services'
            }
        ]
        
        records = []
        for data_type in data_types:
            # Get available reference data
            try:
                url = f"{self.source.api_endpoint}/{data_type['endpoint']}/metadata"
                headers = {}
                if self.api_key:
                    headers['Ocp-Apim-Subscription-Key'] = self.api_key
                
                async with self.session.get(url, headers=headers) as response:
                    if response.status == 200:
                        metadata = await response.json()
                        
                        record = DataRecord(
                            source_name=self.source.name,
                            record_id=data_type['id'],
                            title=data_type['name'],
                            description=data_type['description'],
                            data_type='api_endpoint',
                            url=f"{self.source.api_endpoint}{data_type['endpoint']}",
                            metadata={
                                'endpoint': data_type['endpoint'],
                                'available_years': self._extract_years(metadata),
                                'countries': self._extract_countries(metadata),
                                'products': self._extract_products(metadata)
                            },
                            content_summary=f"Trade data endpoint: {data_type['name']}",
                            tags=['trade', 'international', 'economics', data_type['id']],
                            last_updated=datetime.now(),
                            ingested_at=datetime.now(),
                            file_format='JSON/CSV',
                            license_info=self.source.data_license
                        )
                        records.append(record)
                    else:
                        logger.warning(f"Could not fetch metadata for {data_type['id']}: {response.status}")
                        
            except Exception as e:
                logger.error(f"Error fetching UN Comtrade metadata for {data_type['id']}: {e}")
        
        logger.info(f"Discovered {len(records)} data endpoints from UN Comtrade")
        return records
    
    async def fetch_dataset_metadata(self, dataset_id: str) -> Optional[DataRecord]:
        """Fetch detailed metadata for UN Comtrade data."""
        # Implementation would fetch specific trade data parameters
        # This is a simplified version
        return None
    
    def _extract_years(self, metadata: Dict) -> List[int]:
        """Extract available years from metadata."""
        # Implementation depends on UN Comtrade API response structure
        return list(range(2000, datetime.now().year + 1))
    
    def _extract_countries(self, metadata: Dict) -> List[str]:
        """Extract available countries from metadata."""
        # Implementation depends on UN Comtrade API response structure
        return []
    
    def _extract_products(self, metadata: Dict) -> List[str]:
        """Extract available products from metadata."""
        # Implementation depends on UN Comtrade API response structure
        return []


class SECAdapter(BaseAdapter):
    """Adapter for SEC.gov EDGAR database."""
    
    def __init__(self):
        source = DataSource(
            name="SEC EDGAR",
            base_url="https://www.sec.gov",
            api_endpoint="https://data.sec.gov/api/xbrl",
            requires_api_key=False,
            rate_limit_per_second=0.1,  # SEC requires 10 requests per second max
            terms_of_service_url="https://www.sec.gov/privacy",
            robots_txt_url="https://www.sec.gov/robots.txt",
            data_license="Public Domain"
        )
        super().__init__(source)
    
    async def discover_datasets(self) -> List[DataRecord]:
        """Discover SEC filing types and data feeds."""
        await self.respect_rate_limit()
        
        # SEC has various data feeds we can discover
        data_feeds = [
            {
                'id': 'company_facts',
                'name': 'Company Facts',
                'description': 'XBRL facts for all companies',
                'url': 'https://data.sec.gov/api/xbrl/companyfacts.zip'
            },
            {
                'id': 'submissions',
                'name': 'Company Submissions',
                'description': 'All company filings metadata',
                'url': 'https://data.sec.gov/submissions/'
            },
            {
                'id': 'mutual_fund_prospectus',
                'name': 'Mutual Fund Prospectus',
                'description': 'Mutual fund and ETF prospectus summaries',
                'url': 'https://data.sec.gov/api/xbrl/frames/'
            }
        ]
        
        records = []
        for feed in data_feeds:
            try:
                # Check if the endpoint is accessible
                test_url = feed['url']
                if feed['id'] == 'submissions':
                    test_url += 'CIK0000320193.json'  # Apple's CIK for testing
                
                headers = {
                    'User-Agent': 'Research Institution compliance@university.edu',
                    'Accept-Encoding': 'gzip, deflate',
                    'Host': 'data.sec.gov'
                }
                
                async with self.session.head(test_url, headers=headers) as response:
                    if response.status in [200, 404]:  # 404 is OK for test endpoint
                        record = DataRecord(
                            source_name=self.source.name,
                            record_id=feed['id'],
                            title=feed['name'],
                            description=feed['description'],
                            data_type='data_feed',
                            url=feed['url'],
                            metadata={
                                'feed_type': feed['id'],
                                'format': 'JSON/ZIP',
                                'update_frequency': 'Daily'
                            },
                            content_summary=f"SEC data feed: {feed['name']}",
                            tags=['sec', 'financial', 'filings', 'xbrl'],
                            last_updated=datetime.now(),
                            ingested_at=datetime.now(),
                            file_format='JSON/ZIP',
                            license_info=self.source.data_license
                        )
                        records.append(record)
                    else:
                        logger.warning(f"SEC feed {feed['id']} not accessible: {response.status}")
                        
            except Exception as e:
                logger.error(f"Error checking SEC feed {feed['id']}: {e}")
        
        logger.info(f"Discovered {len(records)} data feeds from SEC EDGAR")
        return records
    
    async def fetch_dataset_metadata(self, dataset_id: str) -> Optional[DataRecord]:
        """Fetch detailed metadata for SEC dataset."""
        # Implementation would fetch specific filing metadata
        return None


class CollegeScorecardAdapter(BaseAdapter):
    """Adapter for College Scorecard API."""
    
    def __init__(self, api_key: Optional[str] = None):
        source = DataSource(
            name="College Scorecard",
            base_url="https://api.data.gov",
            api_endpoint="https://api.data.gov/ed/collegescorecard/v1",
            requires_api_key=True,
            rate_limit_per_second=1.0,  # data.gov standard rate limit
            terms_of_service_url="https://api.data.gov/terms/",
            robots_txt_url="https://api.data.gov/robots.txt",
            data_license="Public Domain"
        )
        super().__init__(source, api_key)
    
    async def discover_datasets(self) -> List[DataRecord]:
        """Discover College Scorecard data endpoints."""
        await self.respect_rate_limit()
        
        if not self.api_key:
            logger.error("College Scorecard requires API key from api.data.gov")
            return []
        
        endpoints = [
            {
                'id': 'schools',
                'name': 'School Data',
                'description': 'College and university data including academics, admissions, aid, cost, completion, earnings, and student body',
                'endpoint': '/schools'
            }
        ]
        
        records = []
        for endpoint in endpoints:
            try:
                url = f"{self.source.api_endpoint}{endpoint['endpoint']}"
                params = {
                    'api_key': self.api_key,
                    '_per_page': 1,  # Just check if endpoint works
                    '_fields': 'id,school.name'
                }
                
                async with self.session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        record = DataRecord(
                            source_name=self.source.name,
                            record_id=endpoint['id'],
                            title=endpoint['name'],
                            description=endpoint['description'],
                            data_type='api_endpoint',
                            url=url,
                            metadata={
                                'endpoint': endpoint['endpoint'],
                                'total_schools': data.get('metadata', {}).get('total', 0),
                                'available_fields': self._get_available_fields()
                            },
                            content_summary=f"College data endpoint: {endpoint['name']}",
                            tags=['education', 'college', 'university', 'scorecard'],
                            last_updated=datetime.now(),
                            ingested_at=datetime.now(),
                            file_format='JSON',
                            license_info=self.source.data_license
                        )
                        records.append(record)
                    else:
                        logger.error(f"Failed to access College Scorecard endpoint: {response.status}")
                        
            except Exception as e:
                logger.error(f"Error checking College Scorecard endpoint: {e}")
        
        logger.info(f"Discovered {len(records)} endpoints from College Scorecard")
        return records
    
    async def fetch_dataset_metadata(self, dataset_id: str) -> Optional[DataRecord]:
        """Fetch detailed College Scorecard metadata."""
        # Implementation would fetch specific school data
        return None
    
    def _get_available_fields(self) -> List[str]:
        """Get available data fields for College Scorecard."""
        # This would typically come from the API documentation
        return [
            'school.name', 'school.city', 'school.state', 'school.zip',
            'school.school_url', 'school.price_calculator_url',
            'admissions.admission_rate.overall', 'student.size',
            'cost.tuition.in_state', 'cost.tuition.out_of_state',
            'aid.median_debt.completers.overall', 'completion.completion_rate_4yr_150nt',
            'earnings.10_yrs_after_entry.median'
        ]


class EthicalDataManager:
    """Main manager for ethical data collection across multiple sources using DuckDB."""
    
    def __init__(self, db_path: str = "ethical_data.duckdb"):
        self.db_path = db_path
        self.db_manager = DatabaseManager(db_path)
        self.adapters: Dict[str, BaseAdapter] = {}
        self.compliance_checks: Dict[str, ComplianceCheck] = {}

    def register_adapter(self, adapter: BaseAdapter):
        """Register a data source adapter."""
        self.adapters[adapter.source.name] = adapter

    async def verify_all_compliance(self) -> Dict[str, ComplianceCheck]:
        """Verify compliance for all registered adapters."""
        compliance_results = {}
    
        for name, adapter in self.adapters.items():
            try:
                async with adapter:
                    check = await adapter.check_compliance()
                    compliance_results[name] = check
                    self.compliance_checks[name] = check
                
                    # Store in database using our fixed method
                    await self._store_compliance_check(check)
                
                    if check.issues:
                        console.print(f"[red]⚠️  Compliance issues for {name}:[/red]")
                        for issue in check.issues:
                            console.print(f"  • {issue}")
                        if check.recommendations:
                            console.print(f"[yellow]💡 Recommendations:[/yellow]")
                            for rec in check.recommendations:
                                console.print(f"  • {rec}")
                    else:
                        console.print(f"[green]✅ {name} is compliant[/green]")
                    
            except Exception as e:
                logger.error(f"Error checking compliance for {name}: {e}")
    
        return compliance_results    

    async def initialize_database(self):
        """Initialize DuckDB database with optimized analytical schema."""
    
        # Data records table with proper auto-increment
        await self.db_manager.execute_async("""
            CREATE SEQUENCE IF NOT EXISTS data_records_id_seq;
        """)
    
        await self.db_manager.execute_async("""
            CREATE TABLE IF NOT EXISTS data_records (
                id INTEGER PRIMARY KEY DEFAULT nextval('data_records_id_seq'),
                source_name VARCHAR NOT NULL,
                record_id VARCHAR NOT NULL,
                title VARCHAR NOT NULL,
                description TEXT,
                data_type VARCHAR,
                url VARCHAR,
                metadata JSON,
                content_summary TEXT,
                tags VARCHAR[],
                last_updated TIMESTAMP,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                file_format VARCHAR,
                size_bytes BIGINT,
                license_info VARCHAR,
                embedding FLOAT[],
                UNIQUE(source_name, record_id)
            )
        """)
    
        # Compliance tracking table with proper auto-increment
        await self.db_manager.execute_async("""
            CREATE SEQUENCE IF NOT EXISTS compliance_checks_id_seq;
        """)
    
        await self.db_manager.execute_async("""
            CREATE TABLE IF NOT EXISTS compliance_checks (
                id INTEGER PRIMARY KEY DEFAULT nextval('compliance_checks_id_seq'),
                source_name VARCHAR NOT NULL,
                check_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                robots_txt_compliant BOOLEAN,
                rate_limit_compliant BOOLEAN,
                terms_compliant BOOLEAN,
                api_key_valid BOOLEAN,
                issues VARCHAR[],
                recommendations VARCHAR[]
            )
        """)
    
        # Data sources registry
        await self.db_manager.execute_async("""
            CREATE TABLE IF NOT EXISTS data_sources (
                name VARCHAR PRIMARY KEY,
                base_url VARCHAR,
                api_endpoint VARCHAR,
                requires_api_key BOOLEAN,
                rate_limit_per_second DOUBLE,
                terms_of_service_url VARCHAR,
                robots_txt_url VARCHAR,
                data_license VARCHAR,
                last_checked TIMESTAMP,
                is_compliant BOOLEAN DEFAULT true,
                compliance_notes TEXT
            )
        """)
    
        # Create indexes for performance
        await self.db_manager.execute_async("CREATE INDEX IF NOT EXISTS idx_source_name ON data_records(source_name)")
        await self.db_manager.execute_async("CREATE INDEX IF NOT EXISTS idx_data_type ON data_records(data_type)")
        await self.db_manager.execute_async("CREATE INDEX IF NOT EXISTS idx_ingested_at ON data_records(ingested_at)")
    
        # Skip FTS for now since it's causing issues
        logger.info("Database initialized successfully")

    async def _store_compliance_check(self, check: ComplianceCheck):
        """Store compliance check results using raw SQL."""
        await self.db_manager.execute_async("""
            INSERT INTO compliance_checks 
            (source_name, check_time, robots_txt_compliant, rate_limit_compliant, 
            terms_compliant, api_key_valid, issues, recommendations)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            check.source_name,
            check.last_check,
            check.robots_txt_compliant,
            check.rate_limit_compliant,
            check.terms_compliant,
            check.api_key_valid,
            check.issues,
            check.recommendations
        ])

    async def _store_data_records_batch(self, records: List[DataRecord]):
        """Store data records using raw SQL."""
        if not records:
            return
    
        for record in records:
            await self.db_manager.execute_async("""
                INSERT OR IGNORE INTO data_records 
                (source_name, record_id, title, description, data_type, url, metadata, 
                content_summary, tags, last_updated, ingested_at, file_format, 
                size_bytes, license_info)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                record.source_name,
                record.record_id,
                record.title,
                record.description,
                record.data_type,
                record.url,
                json.dumps(record.metadata) if record.metadata else None,
                record.content_summary,
                record.tags,
                record.last_updated,
                record.ingested_at,
                record.file_format,
                record.size_bytes,
                record.license_info
            ])
    
    async def collect_all_data(self) -> Dict[str, List[DataRecord]]:
        """Collect data from all compliant sources using batch operations."""
        all_data = {}
        
        for name, adapter in self.adapters.items():
            # Check if source is compliant
            if name in self.compliance_checks:
                check = self.compliance_checks[name]
                if check.issues:
                    console.print(f"[yellow]⚠️  Skipping {name} due to compliance issues[/yellow]")
                    continue
            
            try:
                console.print(f"[blue]📡 Collecting data from {name}...[/blue]")
                async with adapter:
                    records = await adapter.discover_datasets()
                    all_data[name] = records
                    
                    # Store records in batch
                    if records:
                        await self._store_data_records_batch(records)
                    
                    console.print(f"[green]✅ Collected {len(records)} records from {name}[/green]")
                    
            except Exception as e:
                logger.error(f"Error collecting data from {name}: {e}")
                all_data[name] = []
        
        return all_data

    async def search_records(self, 
                           query: str, 
                           source_filter: Optional[str] = None,
                           data_type_filter: Optional[str] = None,
                           limit: int = 50) -> List[DataRecord]:
        """Search collected data records using DuckDB's analytical capabilities."""
        
        where_conditions = []
        parameters = []
        
        if query:
            where_conditions.append("""
                (title ILIKE ? OR 
                 description ILIKE ? OR 
                 content_summary ILIKE ?)
            """)
            search_pattern = f"%{query}%"
            parameters.extend([search_pattern] * 3)
        
        if source_filter:
            where_conditions.append("source_name = ?")
            parameters.append(source_filter)
        
        if data_type_filter:
            where_conditions.append("data_type = ?")
            parameters.append(data_type_filter)
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        sql_query = f"""
            SELECT * FROM data_records
            {where_clause}
            ORDER BY ingested_at DESC
            LIMIT ?
        """
        
        all_parameters = parameters + [limit]
        df = await self.db_manager.fetch_df_async(sql_query, all_parameters)
        
        # Convert DataFrame to DataRecord objects
        records = []
        for _, row in df.iterrows():
            record = DataRecord(
                source_name=row['source_name'],
                record_id=row['record_id'],
                title=row['title'],
                description=row['description'],
                data_type=row['data_type'],
                url=row['url'],
                metadata=json.loads(row['metadata']) if row['metadata'] else {},
                content_summary=row['content_summary'],
                tags=list(row['tags']) if row['tags'] else [],
                last_updated=pd.to_datetime(row['last_updated']) if pd.notna(row['last_updated']) else None,
                ingested_at=pd.to_datetime(row['ingested_at']),
                file_format=row['file_format'],
                size_bytes=int(row['size_bytes']) if pd.notna(row['size_bytes']) else None,
                license_info=row['license_info']
            )
            records.append(record)
        
        return records

    async def get_analytics_summary(self) -> Dict[str, Any]:
        """Get comprehensive analytics using DuckDB's analytical functions."""
        
        # Records by source with statistics
        source_stats_df = await self.db_manager.fetch_df_async("""
            SELECT 
                source_name,
                COUNT(*) as record_count,
                COUNT(DISTINCT data_type) as unique_data_types,
                MIN(ingested_at) as first_ingested,
                MAX(ingested_at) as last_ingested
            FROM data_records
            GROUP BY source_name
            ORDER BY record_count DESC
        """)
        
        # Data types distribution
        data_types_df = await self.db_manager.fetch_df_async("""
            SELECT 
                data_type,
                COUNT(*) as count
            FROM data_records
            GROUP BY data_type
            ORDER BY count DESC
        """)
        
        return {
            'source_stats': source_stats_df.to_dict('records'),
            'data_types': data_types_df.to_dict('records'),
            'total_records': int(source_stats_df['record_count'].sum()) if not source_stats_df.empty else 0,
            'total_sources': len(source_stats_df)
        }

    async def export_to_formats(self, 
                              query: Optional[str] = None,
                              output_format: str = 'parquet',
                              output_path: str = 'exported_data') -> str:
        """Export data to various formats using DuckDB's built-in exporters."""
        
        base_query = "SELECT * FROM data_records"
        if query:
            base_query += f" WHERE {query}"
        
        if output_format.lower() == 'parquet':
            output_file = f"{output_path}.parquet"
            await self.db_manager.execute_async(
                f"COPY ({base_query}) TO '{output_file}' (FORMAT PARQUET)"
            )
        elif output_format.lower() == 'csv':
            output_file = f"{output_path}.csv"
            await self.db_manager.execute_async(
                f"COPY ({base_query}) TO '{output_file}' (FORMAT CSV, HEADER)"
            )
        elif output_format.lower() == 'json':
            output_file = f"{output_path}.json"
            await self.db_manager.execute_async(
                f"COPY ({base_query}) TO '{output_file}' (FORMAT JSON)"
            )
        else:
            raise ValueError(f"Unsupported format: {output_format}")
        
        return output_file

    

    def close(self):
        """Close database connections."""
        self.db_manager.close()


# CLI Interface
@click.group()
def cli():
    """Ethical Data Ingestion System - Legal and compliant data collection"""
    pass


@cli.command()
@click.option('--nyc-api-key', help='NYC Open Data API key (optional)')
@click.option('--un-api-key', help='UN Comtrade API key (required)')
@click.option('--college-api-key', help='College Scorecard API key (required)')
@click.option('--db', default='ethical_data.duckdb', help='DuckDB database file path')
def setup(nyc_api_key, un_api_key, college_api_key, db):
    """Set up data source adapters and verify compliance."""
    async def run_setup():
        manager = EthicalDataManager(db_path=db)
        await manager.initialize_database()
        
        console.print(Panel.fit(
            "🔍 Setting up Ethical Data Collection System with DuckDB\n"
            "Verifying compliance with all data sources...",
            title="Ethical Data Setup"
        ))
        
        # Register adapters
        manager.register_adapter(NYCOpenDataAdapter(nyc_api_key))
        manager.register_adapter(UNComtradeAdapter(un_api_key))
        manager.register_adapter(SECAdapter())
        manager.register_adapter(CollegeScorecardAdapter(college_api_key))
        
        # Verify compliance
        compliance_results = await manager.verify_all_compliance()
        
        # Summary
        compliant_sources = sum(1 for check in compliance_results.values() if not check.issues)
        total_sources = len(compliance_results)
        
        console.print(f"\n[green]✅ Setup complete: {compliant_sources}/{total_sources} sources are compliant[/green]")
        console.print(f"[blue]📊 Database: {db} (DuckDB format)[/blue]")
        
        if compliant_sources < total_sources:
            console.print("[yellow]⚠️  Some sources have compliance issues. Address them before collecting data.[/yellow]")
        
        manager.close()
    
    asyncio.run(run_setup())


@cli.command()
@click.option('--nyc-api-key', help='NYC Open Data API key (optional)')
@click.option('--un-api-key', help='UN Comtrade API key (required)')
@click.option('--college-api-key', help='College Scorecard API key (required)')
@click.option('--db', default='ethical_data.duckdb', help='DuckDB database file path')
def collect(nyc_api_key, un_api_key, college_api_key, db):
    """Collect data from all compliant sources."""
    async def run_collection():
        manager = EthicalDataManager(db_path=db)
        await manager.initialize_database()
        
        # Register adapters
        manager.register_adapter(NYCOpenDataAdapter(nyc_api_key))
        manager.register_adapter(UNComtradeAdapter(un_api_key))
        manager.register_adapter(SECAdapter())
        manager.register_adapter(CollegeScorecardAdapter(college_api_key))
        
        console.print(Panel.fit(
            "📡 Starting Ethical Data Collection with DuckDB\n"
            "Collecting from all compliant sources...",
            title="Data Collection"
        ))
        
        # Verify compliance first
        await manager.verify_all_compliance()
        
        # Collect data
        all_data = await manager.collect_all_data()
        
        # Display results
        table = Table(title="Collection Results")
        table.add_column("Source", style="cyan")
        table.add_column("Records", style="magenta")
        table.add_column("Status", style="green")
        
        total_records = 0
        for source, records in all_data.items():
            table.add_row(source, f"{len(records):,}", "✅ Success")
            total_records += len(records)
        
        console.print(table)
        console.print(f"\n[green]📊 Total records collected: {total_records:,}[/green]")
        console.print(f"[blue]💾 Data stored in: {db} (DuckDB)[/blue]")
        
        manager.close()
    
    asyncio.run(run_collection())


@cli.command()
@click.option('--query', required=True, help='Search query')
@click.option('--source', help='Filter by data source')
@click.option('--data-type', help='Filter by data type')
@click.option('--limit', default=10, help='Maximum results')
@click.option('--db', default='ethical_data.duckdb', help='DuckDB database file path')
def search(query, source, data_type, limit, db):
    """Search collected data records using DuckDB analytics."""
    async def run_search():
        manager = EthicalDataManager(db_path=db)
        
        console.print(f"🔍 Searching DuckDB for: [bold cyan]{query}[/bold cyan]")
        if source:
            console.print(f"Source filter: [yellow]{source}[/yellow]")
        if data_type:
            console.print(f"Data type filter: [yellow]{data_type}[/yellow]")
        
        results = await manager.search_records(
            query=query, 
            source_filter=source,
            data_type_filter=data_type,
            limit=limit
        )
        
        if not results:
            console.print("[red]No results found.[/red]")
            manager.close()
            return
        
        console.print(f"\n[green]Found {len(results)} results:[/green]\n")
        
        for i, record in enumerate(results, 1):
            console.print(f"[bold blue]{i}. {record.title}[/bold blue]")
            console.print(f"[dim]{record.url}[/dim]")
            console.print(f"[yellow]Source:[/yellow] {record.source_name} | [yellow]Type:[/yellow] {record.data_type}")
            console.print(f"[yellow]License:[/yellow] {record.license_info}")
            
            if record.description:
                console.print(f"[italic]{record.description[:200]}{'...' if len(record.description) > 200 else ''}[/italic]")
            
            if record.tags:
                tags_display = ", ".join(record.tags[:5])
                console.print(f"[dim]Tags: {tags_display}[/dim]")
            
            console.print()
        sources_in_results = set(record.source_name for record in results)

        if sources_in_results:
            console.print("[bold]📋 Data Attribution:[/bold]")

            if "UN Comtrade" in sources_in_results:
                console.print("[blue]📢 UN Comtrade: Data licensed under CC BY 4.0 | https://comtradeapi.un.org/[/blue]")
            
            if "NYC Open Data" in sources_in_results:
                console.print("[blue]📢 NYC Open Data: Public Domain | https://opendata.cityofnewyork.us/[/blue]")
            
            if "SEC EDGAR" in sources_in_results:
                console.print("[blue]📢 SEC EDGAR: Public Domain | https://www.sec.gov/[/blue]")
            
            if "College Scorecard" in sources_in_results:
                console.print("[blue]📢 College Scorecard: Public Domain | https://collegescorecard.ed.gov/[/blue]")
        
        manager.close()
    
    asyncio.run(run_search())
       


@cli.command()
@click.option('--db', default='ethical_data.duckdb', help='DuckDB database file path')
def analytics(db):
    """Show comprehensive analytics using DuckDB's analytical capabilities."""
    async def show_analytics():
        manager = EthicalDataManager(db_path=db)
        
        console.print("[blue]📊 Generating analytics from DuckDB...[/blue]")
        
        stats = await manager.get_analytics_summary()
        
        # Source statistics
        console.print("\n[bold]📈 Data Sources Performance:[/bold]")
        source_table = Table()
        source_table.add_column("Source", style="cyan")
        source_table.add_column("Records", style="magenta")
        source_table.add_column("Data Types", style="yellow")
        source_table.add_column("Avg Size (bytes)", style="green")
        source_table.add_column("First Ingested", style="blue")
        
        for source in stats['source_stats']:
            avg_size = f"{int(source['avg_size_bytes']):,}" if source['avg_size_bytes'] else "N/A"
            first_date = pd.to_datetime(source['first_ingested']).strftime('%Y-%m-%d') if source['first_ingested'] else "N/A"
            
            source_table.add_row(
                source['source_name'],
                f"{source['record_count']:,}",
                str(source['unique_data_types']),
                avg_size,
                first_date
            )
        
        console.print(source_table)
        
        # Data types distribution
        if stats['data_types']:
            console.print("\n[bold]📋 Data Types Distribution:[/bold]")
            for dt in stats['data_types'][:5]:
                console.print(f"  {dt['data_type']}: {dt['count']:,} ({dt['percentage']}%)")
        
        # Popular tags
        if stats['popular_tags']:
            console.print("\n[bold]🏷️  Most Popular Tags:[/bold]")
            for tag in stats['popular_tags'][:10]:
                console.print(f"  {tag['tag']}: {tag['frequency']:,}")
        
        # License distribution
        if stats['license_distribution']:
            console.print("\n[bold]⚖️  License Distribution:[/bold]")
            for license_info in stats['license_distribution']:
                console.print(f"  {license_info['license']}: {license_info['count']:,}")
        
        # Summary
        console.print(f"\n[green]📊 Total: {stats['total_records']:,} records from {stats['total_sources']} sources[/green]")
        console.print(f"[blue]💾 Database: {db} (DuckDB format)[/blue]")
        
        if stats['source_stats']:
            console.print("\n[bold]📋 Data Source Attribution:[/bold]")
            
            source_names = [source['source_name'] for source in stats['source_stats']]
            
            if "UN Comtrade" in source_names:
                console.print("[blue]📢 UN Comtrade: Data licensed under CC BY 4.0 | https://comtradeapi.un.org/[/blue]")
            
            if "NYC Open Data" in source_names:
                console.print("[blue]📢 NYC Open Data: Public Domain | https://opendata.cityofnewyork.us/[/blue]")
            
            if "SEC EDGAR" in source_names:
                console.print("[blue]📢 SEC EDGAR: Public Domain | https://www.sec.gov/[/blue]")
            
            if "College Scorecard" in source_names:
                console.print("[blue]📢 College Scorecard: Public Domain | https://collegescorecard.ed.gov/[/blue]")
        
        manager.close()
    
    asyncio.run(show_analytics())


@cli.command()
@click.option('--format', default='parquet', type=click.Choice(['parquet', 'csv', 'json']), help='Export format')
@click.option('--output', default='exported_data', help='Output file path (without extension)')
@click.option('--where', help='SQL WHERE clause for filtering')
@click.option('--db', default='ethical_data.duckdb', help='DuckDB database file path')
def export(format, output, where, db):
    """Export data to various formats using DuckDB's native exporters."""
    async def run_export():
        manager = EthicalDataManager(db_path=db)
        
        console.print(f"[blue]📤 Exporting data to {format.upper()} format...[/blue]")
        if where:
            console.print(f"[yellow]Filter: {where}[/yellow]")
        
        try:
            output_file = await manager.export_to_formats(
                query=where,
                output_format=format,
                output_path=output
            )
            
            # Get file size
            file_path = Path(output_file)
            if file_path.exists():
                file_size = file_path.stat().st_size
                console.print(f"[green]✅ Export complete![/green]")
                console.print(f"[blue]📁 File: {output_file}[/blue]")
                console.print(f"[blue]📊 Size: {file_size:,} bytes[/blue]")
                
                # 🎯 ADD THIS ATTRIBUTION BLOCK HERE:
                # Check what sources are in the exported data
                sources_query = "SELECT DISTINCT source_name FROM data_records"
                if where:
                    sources_query += f" WHERE {where}"
                
                sources_df = await manager.db_manager.fetch_df_async(sources_query)
                source_names = sources_df['source_name'].tolist()
                
                if source_names:
                    console.print("\n[bold]📋 Exported Data Attribution:[/bold]")
                    
                    if "UN Comtrade" in source_names:
                        console.print("[blue]📢 UN Comtrade: Data licensed under CC BY 4.0 | https://comtradeapi.un.org/[/blue]")
                    
                    if "NYC Open Data" in source_names:
                        console.print("[blue]📢 NYC Open Data: Public Domain | https://opendata.cityofnewyork.us/[/blue]")
                    
                    if "SEC EDGAR" in source_names:
                        console.print("[blue]📢 SEC EDGAR: Public Domain | https://www.sec.gov/[/blue]")
                    
                    if "College Scorecard" in source_names:
                        console.print("[blue]📢 College Scorecard: Public Domain | https://collegescorecard.ed.gov/[/blue]")
                
            else:
                console.print(f"[red]❌ Export failed - file not found[/red]")
                
        except Exception as e:
            console.print(f"[red]❌ Export failed: {e}[/red]")
        
        manager.close()
    
    asyncio.run(run_export())


@cli.command()
@click.option('--db', default='ethical_data.duckdb', help='DuckDB database file path')
def status(db):
    """Show system status and compliance information."""
    async def show_status():
        manager = EthicalDataManager(db_path=db)
        
        # Get basic counts
        try:
            df = await manager.db_manager.fetch_df_async("""
                SELECT 
                    source_name, 
                    data_type,
                    COUNT(*) as count,
                    MAX(ingested_at) as last_ingested
                FROM data_records 
                GROUP BY source_name, data_type
                ORDER BY source_name, count DESC
            """)
            
            # Get compliance status
            compliance_df = await manager.db_manager.fetch_df_async("""
                SELECT 
                    source_name, 
                    robots_txt_compliant, 
                    rate_limit_compliant,
                    terms_compliant, 
                    api_key_valid, 
                    check_time
                FROM compliance_checks 
                WHERE check_time = (
                    SELECT MAX(check_time) 
                    FROM compliance_checks c2 
                    WHERE c2.source_name = compliance_checks.source_name
                )
            """)
            
        except Exception as e:
            console.print(f"[red]Database error: {e}[/red]")
            console.print("[yellow]Try running 'setup' command first[/yellow]")
            manager.close()
            return
        
        # Display source statistics
        table = Table(title="📊 DuckDB Data Collection Status")
        table.add_column("Source", style="cyan")
        table.add_column("Records", style="magenta")
        table.add_column("Data Types", style="yellow")
        table.add_column("Last Ingested", style="blue")
        table.add_column("Compliance", style="green")
        
        # Group by source
        source_stats = df.groupby('source_name').agg({
            'count': 'sum',
            'data_type': 'nunique',
            'last_ingested': 'max'
        }).reset_index()
        
        compliance_dict = {row['source_name']: row for _, row in compliance_df.iterrows()}
        
        for _, source in source_stats.iterrows():
            last_ingested = pd.to_datetime(source['last_ingested']).strftime('%Y-%m-%d %H:%M') if pd.notna(source['last_ingested']) else "Never"
            
            if source['source_name'] in compliance_dict:
                compliance = compliance_dict[source['source_name']]
                if all([compliance['robots_txt_compliant'], compliance['rate_limit_compliant'], 
                       compliance['terms_compliant'], compliance['api_key_valid']]):
                    compliance_status = "✅ Compliant"
                else:
                    compliance_status = "⚠️ Issues"
            else:
                compliance_status = "❓ Unknown"
            
            table.add_row(
                source['source_name'],
                f"{int(source['count']):,}",
                str(int(source['data_type'])),
                last_ingested,
                compliance_status
            )
        
        console.print(table)
        
        # Total records
        total_records = int(source_stats['count'].sum())
        console.print(f"\n[green]📈 Total records: {total_records:,}[/green]")
        console.print(f"[blue]💾 Database: {db} (DuckDB format)[/blue]")
        
        # Database file size
        db_path = Path(db)
        if db_path.exists():
            db_size = db_path.stat().st_size
            console.print(f"[blue]📊 Database size: {db_size:,} bytes[/blue]")
        
        if not source_stats.empty:
            console.print("\n[bold]📋 Data Source Licenses & Attribution:[/bold]")
            
            source_names = source_stats['source_name'].tolist()
            
            if "UN Comtrade" in source_names:
                console.print("[blue]📢 UN Comtrade: Data licensed under CC BY 4.0 | https://comtradeapi.un.org/[/blue]")
            
            if "NYC Open Data" in source_names:
                console.print("[blue]📢 NYC Open Data: Public Domain | https://opendata.cityofnewyork.us/[/blue]")
            
            if "SEC EDGAR" in source_names:
                console.print("[blue]📢 SEC EDGAR: Public Domain | https://www.sec.gov/[/blue]")
            
            if "College Scorecard" in source_names:
                console.print("[blue]📢 College Scorecard: Public Domain | https://collegescorecard.ed.gov/[/blue]")
        
        manager.close()
    
    asyncio.run(show_status())


if __name__ == "__main__":
    cli()
