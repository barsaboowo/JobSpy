from __future__ import annotations

import random
import time
from datetime import datetime
from typing import Optional

import requests
from bs4 import BeautifulSoup

from jobspy.model import (
    Scraper,
    ScraperInput,
    Site,
    JobPost,
    JobResponse,
    Location,
    Country,
    Compensation,
    JobType,
)
from jobspy.util import create_logger, create_session, extract_emails_from_text

log = create_logger("Wanted")


class Wanted(Scraper):
    """Scraper for Wanted.kr - a popular Korean job board"""

    base_url = "https://www.wanted.co.kr"
    api_base_url = "https://www.wanted.co.kr/api/v4/jobs"
    delay = 2
    band_delay = 3

    def __init__(
        self, proxies: list[str] | str | None = None, ca_cert: str | None = None, user_agent: str | None = None
    ):
        super().__init__(Site.WANTED, proxies=proxies, ca_cert=ca_cert)
        self.scraper_input = None
        self.session = None
        self.country = "south korea"

    def scrape(self, scraper_input: ScraperInput) -> JobResponse:
        """
        Scrapes Wanted.kr for jobs with scraper_input criteria
        """
        self.scraper_input = scraper_input
        self.session = create_session(
            proxies=self.proxies, ca_cert=self.ca_cert, is_tls=False, has_retry=True
        )
        job_list: list[JobPost] = []
        offset = 0
        limit = 20
        results_wanted = (
            scraper_input.results_wanted if scraper_input.results_wanted else 15
        )

        while len(job_list) < results_wanted:
            log.info(f"Fetching Wanted jobs with offset {offset}")
            jobs_data = self._fetch_jobs(self.scraper_input.search_term, offset, limit)
            if not jobs_data:
                break

            for job_data in jobs_data:
                try:
                    job_post = self._extract_job_info(job_data)
                    if job_post:
                        job_list.append(job_post)
                        if len(job_list) >= results_wanted:
                            break
                except Exception as e:
                    log.error(f"Wanted: Error extracting job info: {str(e)}")
                    continue

            if len(jobs_data) < limit:
                # No more results
                break

            offset += limit
            time.sleep(random.uniform(self.delay, self.delay + self.band_delay))

        job_list = job_list[:results_wanted]
        return JobResponse(jobs=job_list)

    def _fetch_jobs(self, query: str, offset: int, limit: int) -> list | None:
        """
        Fetches job results from Wanted API
        """
        try:
            params = {
                "query": query or "",
                "offset": offset,
                "limit": limit,
                "sort": "job.popularity_order",
            }

            headers = {
                "Accept": "application/json",
                "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
                "Referer": f"{self.base_url}/search?query={query}",
            }

            response = self.session.get(
                self.api_base_url, params=params, headers=headers, timeout=30
            )
            response.raise_for_status()

            data = response.json()
            return data.get("data", {}).get("jobs", [])

        except requests.RequestException as e:
            log.error(f"Wanted: Error fetching jobs - {str(e)}")
            return None
        except ValueError as e:
            log.error(f"Wanted: Error parsing JSON - {str(e)}")
            return None

    def _extract_job_info(self, job_data: dict) -> Optional[JobPost]:
        """
        Extracts job information from a single job data dictionary
        """
        if not job_data:
            return None

        job_id = str(job_data.get("id"))
        if not job_id:
            return None

        title = job_data.get("title") or job_data.get("position", "")
        company_name = job_data.get("company", {}).get("name") if job_data.get("company") else None

        # Get job URL
        job_url = f"{self.base_url}/wd/{job_id}"

        # Get location info
        location_data = job_data.get("location", {})
        location_str = location_data.get("address") if location_data else None

        city = None
        if location_str:
            # Parse "서울 · 강남구" or similar format
            parts = location_str.split("·")
            if len(parts) >= 2:
                city = parts[0].strip()
            else:
                city = location_str.strip()

        location = Location(
            city=city,
            country=Country.from_string(self.country),
        )

        # Parse job type
        job_type = self._parse_job_type(job_data)

        # Get posted date
        date_posted = None
        posted_at = job_data.get("posted_at")
        if posted_at:
            try:
                date_posted = datetime.fromisoformat(posted_at.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                pass

        # Get compensation info
        compensation = None
        annual_salary = job_data.get("annual_salary")
        if annual_salary:
            min_amount = annual_salary.get("from")
            max_amount = annual_salary.get("to")
            if min_amount or max_amount:
                compensation = Compensation(
                    min_amount=min_amount,
                    max_amount=max_amount,
                    currency="KRW",
                )

        # Get job description
        description = job_data.get("detail", {}).get("intro")
        if not description:
            description = job_data.get("detail", {}).get("requirement")

        # Get company logo
        company_logo = None
        if job_data.get("company", {}).get("logo"):
            company_logo = job_data["company"]["logo"]

        # Check if remote
        is_remote = False
        address = location_data.get("address", "") if location_data else ""
        if address and ("재택" in address or "원격" in address or "remote" in address.lower()):
            is_remote = True

        return JobPost(
            id=f"wanted-{job_id}",
            title=title,
            company_name=company_name,
            company_url=f"{self.base_url}/company/{job_data.get('company', {}).get('id', '')}" if job_data.get("company") else None,
            company_logo=company_logo,
            location=location,
            is_remote=is_remote,
            job_type=job_type,
            date_posted=date_posted,
            job_url=job_url,
            job_url_direct=job_data.get("apply_url"),
            compensation=compensation,
            description=description,
            emails=extract_emails_from_text(description) if description else None,
        )

    def _parse_job_type(self, job_data: dict) -> list[JobType] | None:
        """
        Parse job type from job data
        """
        job_types = []

        # Check employment type
        employment_type = job_data.get("employment_type", "")
        if employment_type:
            type_str = employment_type.lower()
            if "정규직" in type_str or "full" in type_str:
                job_types.append(JobType.FULL_TIME)
            elif "계약직" in type_str or "contract" in type_str:
                job_types.append(JobType.CONTRACT)
            elif "인턴" in type_str or "intern" in type_str:
                job_types.append(JobType.INTERNSHIP)
            elif "파트타임" in type_str or "part" in type_str:
                job_types.append(JobType.PART_TIME)

        return job_types if job_types else None
