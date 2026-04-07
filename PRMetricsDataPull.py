import requests
import os
from datetime import datetime, timedelta, timezone
from collections import defaultdict
import csv
import logging
import time

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# GitHub API base URL
GITHUB_API_URL = "https://api.github.com"

# GitHub token (optional for public repos)
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
# how to setup env variable: in windows use: 
REPO_OWNER = ""  # Will be prompted if empty
REPO_NAME = ""   # Will be prompted if empty

# Headers: include Authorization only if token is present
HEADERS = {
    "Accept": "application/vnd.github.v3+json"
}
if GITHUB_TOKEN:
    print("API token detected, using authenticated requests for higher rate limits.")
    HEADERS["Authorization"] = f"token {GITHUB_TOKEN}"

class RateLimitError(Exception):
    pass


def get_prs(state="closed", per_page=100):
    """Fetch PRs from GitHub API."""
    logger.info("[API] Fetching PRs: state=%s, per_page=%s for repo %s/%s", state, per_page, REPO_OWNER, REPO_NAME)
    url = f"{GITHUB_API_URL}/repos/{REPO_OWNER}/{REPO_NAME}/pulls"
    params = {"state": state, "per_page": per_page, "sort": "created", "direction": "desc"}
    response = requests.get(url, headers=HEADERS, params=params)
    if response.status_code == 403 and "rate limit" in response.text.lower():
        logger.error("[ERR] Rate limit exceeded when fetching PR list")
        raise RateLimitError("Rate limit exceeded")
    response.raise_for_status()
    prs = response.json()
    logger.info("[API] Successfully fetched %d PRs", len(prs))
    return prs

def safe_get(url, params=None, attempts=2):
    for i in range(attempts):
        resp = requests.get(url, headers=HEADERS, params=params)
        if resp.status_code == 403 and "rate limit" in resp.text.lower():
            logger.error("[ERR] Rate limit exceeded for URL %s", url)
            raise RateLimitError("Rate limit exceeded")
        try:
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            if resp.status_code in (500,502,503,504) and i < attempts-1:
                logger.warning("[ERR] transient server error %d for URL %s, retrying", resp.status_code, url)
                time.sleep(2 ** i)
                continue
            raise
    raise RuntimeError("Failed after retries")

def get_pr_details(pr_number):
    """Fetch detailed PR info."""
    url = f"{GITHUB_API_URL}/repos/{REPO_OWNER}/{REPO_NAME}/pulls/{pr_number}"
    logger.info("[API] Fetching details for PR #%d", pr_number)
    return safe_get(url)

def get_pr_reviews(pr_number):
    """Fetch reviews for a PR."""
    url = f"{GITHUB_API_URL}/repos/{REPO_OWNER}/{REPO_NAME}/pulls/{pr_number}/reviews"
    logger.info("[API] Fetching reviews for PR #%d", pr_number)
    return safe_get(url)

def collect_data():
    """Collect all data into structures mimicking the tables."""
    logger.info("[DEBUG] Starting data collection for repo %s/%s", REPO_OWNER, REPO_NAME)
    users = {}  # user_id -> user data
    prs = []    # list of pr dicts
    reviews = []  # list of review dicts

    # Fetch closed PRs
    closed_pr_list = get_prs(state="closed", per_page=100)  # Limit for PoC
    # Fetch open PRs
    open_pr_list = get_prs(state="open", per_page=100)

    all_prs = closed_pr_list + open_pr_list
    logger.info("[DEBUG] Total PRs to process: %d", len(all_prs))

    for pr in all_prs:
        pr_number = pr["number"]
        logger.info("[DEBUG] Processing PR #%d (%s)", pr_number, pr["state"])
        try:
            pr_details = get_pr_details(pr_number)
            pr_reviews = get_pr_reviews(pr_number)
        except RateLimitError:
            logger.error("[ERR] Rate limit reached during PR traversal, stopping further processing.")
            return users, prs, reviews
        except Exception as ex:
            logger.error("[ERR] Failed to process PR #%d: %s", pr_number, ex)
            continue

        # Collect users
        author = pr["user"]
        users[author["id"]] = {
            "user_id": author["id"],
            "user_login": author["login"],
            "user_type": author["type"],
            "avatar_url": author.get("avatar_url")
        }

        if pr_details.get("merged_by"):
            merger = pr_details["merged_by"]
            users[merger["id"]] = {
                "user_id": merger["id"],
                "user_login": merger["login"],
                "user_type": merger["type"],
                "avatar_url": merger.get("avatar_url")
            }


        # PR data
        created_at = datetime.fromisoformat(pr["created_at"].replace("Z", "+00:00"))
        updated_at = datetime.fromisoformat(pr["updated_at"].replace("Z", "+00:00"))
        merged_at = None
        if pr_details.get("merged_at"):
            merged_at = datetime.fromisoformat(pr_details["merged_at"].replace("Z", "+00:00"))
        closed_at = None
        if pr_details.get("closed_at"):
            closed_at = datetime.fromisoformat(pr_details["closed_at"].replace("Z", "+00:00"))

        cycle_time_hours = None
        if merged_at:
            cycle_time_hours = (merged_at - created_at).total_seconds() / 3600
        elif closed_at:
            cycle_time_hours = (closed_at - created_at).total_seconds() / 3600
        else: 
            cycle_time_hours = None

        today = datetime.now(timezone.utc)
        days_since_created = (today - created_at).days
        days_since_updated = (today - updated_at).days

        merged_by = pr_details.get("merged_by") if isinstance(pr_details, dict) else None
        merged_by_id = merged_by.get("id") if isinstance(merged_by, dict) else None

        first_review_at = None
        last_review_at = None
        if len(pr_reviews) > 0:
            first_review_at = datetime.fromisoformat(
                pr_reviews[0]["submitted_at"].replace("Z", "+00:00"))
            last_review_at = datetime.fromisoformat(
                pr_reviews[-1]["submitted_at"].replace("Z", "+00:00"))
            
        review_turnaround_time_hours = None
        if first_review_at and last_review_at and first_review_at != last_review_at:
            review_turnaround_time_hours = (last_review_at - first_review_at).total_seconds() / 3600

        approvals_count = 0
        changes_requested_count = 0
        commented_count = 0

        # Reviews data
        for review in pr_reviews:
            reviewer = review.get("user")
            if not reviewer:
                logger.warning("[ERR] Review without user on PR #%d review id %s", pr_number, review.get("id"))
                continue
            
            users[reviewer["id"]] = {
                "user_id": reviewer["id"],
                "user_login": reviewer["login"],
                "user_type": reviewer["type"],
                "avatar_url": reviewer.get("avatar_url")
            }

            review_data = {
                "review_id": review["id"],
                "reviewer_login": reviewer["login"],
                "pr_id": pr_number,
                "reviewer_id": reviewer["id"],
                "state": review.get("state"),
                "submitted_at": datetime.fromisoformat(review["submitted_at"].replace("Z", "+00:00")) if review.get("submitted_at") else None,
                "body_length": len(review.get("body", ""))
            }

            # Count review states for metrics
            if review.get("state") == "APPROVED":
                approvals_count += 1
            elif review.get("state") == "REQUEST_CHANGES":
                changes_requested_count += 1
            elif review.get("state") == "COMMENT":
                commented_count += 1

            reviews.append(review_data)

        pr_data = {
            "pr_id": f"{REPO_NAME}#{pr_number}",
            "repo_name": REPO_NAME,
            "pr_number": pr_number,
            "pr_title": pr["title"],
            "author_id": author["id"],
            "state": pr["state"],
            "is_merged": pr_details.get("merged_at") is not None if isinstance(pr_details, dict) else False,
            "is_draft": pr.get("draft", False),
            "created_at": created_at,
            "updated_at": updated_at,
            "closed_at": closed_at,
            "merged_at": merged_at,
            "merged_by_id": merged_by_id,
            "additions": pr_details.get("additions", 0) if isinstance(pr_details, dict) else 0,
            "deletions": pr_details.get("deletions", 0) if isinstance(pr_details, dict) else 0,
            "pr_size": (pr_details.get("additions", 0) + pr_details.get("deletions", 0)) if isinstance(pr_details, dict) else 0,
            "changed_files": pr_details.get("changed_files", 0) if isinstance(pr_details, dict) else 0,
            "commits_count": pr_details.get("commits", 0) if isinstance(pr_details, dict) else 0,
            "comments_count": pr_details.get("comments", 0) if isinstance(pr_details, dict) else 0,
            "review_comments_count": pr_details.get("review_comments", 0) if isinstance(pr_details, dict) else 0,
            "total_comments": (pr_details.get("comments", 0) + pr_details.get("review_comments", 0)) if isinstance(pr_details, dict) else 0,
            "cycle_time_hours": cycle_time_hours,
            "days_since_created": days_since_created,
            "days_since_updated": days_since_updated,
            "first_review_at": first_review_at,
            "last_review_at": last_review_at,
            "time_to_first_review_hours": ((first_review_at - created_at).total_seconds() / 3600) if first_review_at else None,
            "review_turnaround_time_hours": review_turnaround_time_hours,
            "approvals_count": approvals_count,
            "changes_requested_count": changes_requested_count,
            "commented_count": commented_count,
            "has_approval": approvals_count > 0,
            }
        print("###Appending PR data of PR #{} to list".format(pr_number))
        prs.append(pr_data)

    logger.info("[DEBUG] Data collection complete: %d users, %d PRs, %d reviews", len(users), len(prs), len(reviews))
    return users, prs, reviews

def save_to_csv(users, prs, reviews):
    """Save data to CSV files."""
    logger.info("[DEBUG] Saving data to CSV files")
    # Users
    with open("users.csv", "w", newline="", encoding="utf-8") as f:
        if users:
            writer = csv.DictWriter(f, fieldnames=list(users[next(iter(users))].keys()))
            writer.writeheader()
            writer.writerows(users.values())

    # PRs
    with open("prs.csv", "w", newline="", encoding="utf-8") as f:
        if prs:
            # Convert datetime to string
            prs_serialized = []
            for p in prs:
                p_copy = p.copy()
                for key in ["created_at", "updated_at", "closed_at", "merged_at"]:
                    if p_copy[key]:
                        p_copy[key] = p_copy[key].isoformat()
                prs_serialized.append(p_copy)
            writer = csv.DictWriter(f, fieldnames=list(prs_serialized[0].keys()))
            writer.writeheader()
            writer.writerows(prs_serialized)

    # Reviews
    with open("reviews.csv", "w", newline="", encoding="utf-8") as f:
        if reviews:
            # Convert datetime to string
            reviews_serialized = []
            for r in reviews:
                r_copy = r.copy()
                r_copy["submitted_at"] = r_copy["submitted_at"].isoformat()
                reviews_serialized.append(r_copy)
            writer = csv.DictWriter(f, fieldnames=list(reviews_serialized[0].keys()))
            writer.writeheader()
            writer.writerows(reviews_serialized)

    logger.info("[DEBUG] CSV files saved successfully")

logger.info("[DEBUG] Script started")
if not REPO_OWNER:
    REPO_OWNER = input("Enter the repository owner (e.g., octocat): ").strip()
if not REPO_NAME:
    REPO_NAME = input("Enter the repository name (e.g., Hello-World): ").strip()

users, prs, reviews = {}, [], []
try:
    users, prs, reviews = collect_data()
except RateLimitError:
    logger.error("[ERR] Rate limit reached before/while traversing PRs (partial data may have been collected).")
except Exception as ex:
    logger.error("[ERR] Unexpected error during collect_data: %s", ex)
finally:
    logger.info("[DEBUG] Final counts: users=%d, prs=%d, reviews=%d", len(users), len(prs), len(reviews))

    def log_sample(name, data):
        if isinstance(data, dict):
            sample = list(data.values())[:5]
        else:
            sample = data[:5]
        logger.info("[DEBUG] Sample %s (len=%d): %s", name, len(data), sample)

    log_sample("users", users)
    log_sample("prs", prs)
    log_sample("reviews", reviews)

    save_to_csv(users, prs, reviews)
    print("Data exported to users.csv, prs.csv, reviews.csv")
    logger.info("[DEBUG] Script completed successfully")