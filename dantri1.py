import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import psycopg2
import os
import sys
import io
import schedule
import time

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

def crawl_dantri(start_url, category_label, news_source_label, num_pages=1):
    """
    Crawls a specific category URL on Dantri, assigning a label to each article.
    
    Args:
        start_url (str): The URL of the category page to start crawling.
        category_label (str): The label to assign to all articles found on this page.
        news_source_label (str): The label for the news source (e.g., "Dân trí").
        num_pages (int): The number of pages to crawl for the category.

    Returns:
        list: A list of dictionaries, where each dictionary represents an article.
              Returns None if the page request fails.
    """
    seen_urls = set()
    articles = []

    # Create folder to save images if it doesn't exist
    image_folder = 'dantri'
    os.makedirs(image_folder, exist_ok=True)
    
    # Split the original URL to easily insert page numbers
    base_url_parts = start_url.rsplit('.', 1)
    base_url = base_url_parts[0]
    suffix = "." + base_url_parts[1] if len(base_url_parts) > 1 else ""

    for page in range(1, num_pages + 1):
        if page == 1:
            page_url = start_url
        else:
            page_url = f"{base_url}/trang-{page}{suffix}"
            
        print(f"\nCrawling category '{category_label}' on page: {page_url}")
        try:
            response = requests.get(page_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")

            for article_tag in soup.find_all("article"):
                title_element = article_tag.find("h3")
                link_element = article_tag.find("a", href=True)
                summary_element = article_tag.find("div", class_="article-excerpt")
                image_element = article_tag.find("div", class_="article-thumb")
                img_tag = image_element.find("img") if image_element else None
                
                image_url = None
                if img_tag:
                    image_url = img_tag.get("data-src") or img_tag.get("src")

                if title_element and summary_element and link_element and link_element.get("href"):
                    title = title_element.text.strip()
                    summary = summary_element.text.strip()
                    url = urljoin(start_url, link_element["href"])

                    if url not in seen_urls and url.startswith("https://dantri.com.vn"):
                        seen_urls.add(url)
                        
                        article_data = {
                            "title": title,
                            "url": url,
                            "summary": summary,
                            "category": category_label,
                            "news_source": news_source_label,
                            "image_url": image_url
                        }
                        articles.append(article_data)

        except requests.exceptions.RequestException as e:
            print(f"Error requesting page: {e}")
            continue 
    
    print(f"Crawled {len(articles)} articles from category '{category_label}'.")
    return articles


def save_to_postgresql(articles, db_config):
    """
    Saves a list of articles to a PostgreSQL database.
    It will create the 'raw_data' table if it doesn't exist.
    
    Args:
        articles (list): The list of article dictionaries to save.
        db_config (dict): The database connection configuration.

    Returns:
        bool: True if saving was successful, False otherwise.
    """
    if not articles:
        print("No articles to save.")
        return False
        
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()

        # 1. Check and create the table if it doesn't exist
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw_data (
                id SERIAL PRIMARY KEY,
                title TEXT,
                summary TEXT,
                image TEXT,
                category TEXT,
                news_source TEXT,
                url TEXT UNIQUE
            );
        """)
        conn.commit()

        # 2. Get the largest ID to name images, avoiding conflicts
        cur.execute("SELECT MAX(id) FROM raw_data;")
        max_id_result = cur.fetchone()[0]
        image_counter = max_id_result + 1 if max_id_result else 1

        print("Starting to save data to PostgreSQL...")
        for article in articles:
            # CHECK FOR ARTICLE EXISTENCE BEFORE ANY OPERATION
            cur.execute("SELECT url FROM raw_data WHERE url = %s;", (article['url'],))
            existing_article_in_db = cur.fetchone()

            if existing_article_in_db:
                print(f"Article '{article['url']}' already exists in the database. Skipping.")
                continue

            image_path = None
            if article.get("image_url"):
                try:
                    image_response = requests.get(article["image_url"], stream=True, timeout=10)
                    image_response.raise_for_status()
                    
                    image_name = f'image{image_counter}.png'
                    image_path = os.path.join("dantri", image_name)

                    with open(image_path, 'wb') as f:
                       for chunk in image_response.iter_content(chunk_size=8192):
                           f.write(chunk)
                    
                    image_counter += 1

                except requests.exceptions.RequestException as e:
                    print(f"Error downloading image {article['image_url']}: {e}")
                    image_path = None

            try:
                cur.execute("""
                    INSERT INTO raw_data (title, summary, image, category, news_source, url)
                    VALUES (%s, %s, %s, %s, %s, %s);
                """, (article['title'], article['summary'], image_path, article['category'], article['news_source'], article['url']))
            
            except psycopg2.Error as e:
                print(f"Error inserting article {article['url']}: {e}")
                conn.rollback()

        conn.commit()
        return True

    except psycopg2.Error as e:
        print(f"PostgreSQL connection or operation error: {e}")
        return False
    finally:
        if conn:
            cur.close()
            conn.close()

# --- Main execution part ---

CATEGORIES_TO_CRAWL = [
    {"url": "https://dantri.com.vn/kinh-doanh.htm", "category": "Kinh doanh"},
    {"url": "https://dantri.com.vn/xa-hoi.htm", "category": "Xã hội"},
    {"url": "https://dantri.com.vn/the-gioi.htm", "category": "Thế giới"},
    {"url": "https://dantri.com.vn/giai-tri.htm", "category": "Giải trí"},
    {"url": "https://dantri.com.vn/bat-dong-san.htm", "category": "Bất động sản"},
    {"url": "https://dantri.com.vn/the-thao.htm", "category": "Thể thao"},
    {"url": "https://dantri.com.vn/suc-khoe.htm", "category": "Sức khoẻ"},
    {"url": "https://dantri.com.vn/noi-vu.htm", "category": "Nội vụ"},
    {"url": "https://dantri.com.vn/tam-long-nhan-ai.htm", "category": "Nhân ái"},
    {"url": "https://dantri.com.vn/cong-nghe.htm", "category": "Công nghệ"},
    {"url": "https://dantri.com.vn/giao-duc.htm", "category": "Giáo dục"},
    {"url": "https://dantri.com.vn/lao-dong-viec-lam.htm", "category": "Việc làm"},
    {"url": "https://dantri.com.vn/phap-luat.htm", "category": "Pháp luật"}
]

def run_crawling_job():
    """Function to execute the crawling process for all defined categories."""
    all_articles = []
    
    # Database connection configuration
    db_config = {
        "host": "localhost",
        "database": "HeThongTrinhSat", 
        "user": "postgres",
        "password": "13082004"
    }

    for category_info in CATEGORIES_TO_CRAWL:
        articles = crawl_dantri(
            start_url=category_info["url"],
            category_label=category_info["category"],
            news_source_label="Dân trí",
            num_pages=3
        )
        if articles:
            all_articles.extend(articles)
    
    if all_articles:
        if save_to_postgresql(all_articles, db_config):
            print("\nSuccessfully saved all data to PostgreSQL!")
        else:
            print("\nFailed to save data!")
    else:
        print("\nNo data was crawled to save.")

if __name__ == "__main__":
    run_crawling_job()
    
# test