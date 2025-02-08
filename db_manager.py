from typing import List, Optional

import mysql.connector

from schema import News


class DBManager:
    def __init__(self, db_name, user, password, host, port):
        self.conn = mysql.connector.connect(
            database=db_name, user=user, password=password, host=host, port=port
        )
        self.create_tables()  # Ensure tables exist on initialization

    def create_tables(self):
        """Create `short_news` and `long_news` tables if they do not exist."""
        queries = [
            """
            CREATE TABLE IF NOT EXISTS short_news (
                id SERIAL PRIMARY KEY,
                source VARCHAR(255),
                timestamp TIMESTAMP,
                body TEXT
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS long_news (
                id SERIAL PRIMARY KEY,
                source VARCHAR(255),
                timestamp TIMESTAMP,
                news_link TEXT,
                title TEXT,
                body TEXT
            );
            """
        ]
        with self.conn.cursor() as cursor:
            for query in queries:
                cursor.execute(query)
            self.conn.commit()

    def get_unprocessed_news(self):
        """Retrieve all unprocessed news articles."""
        with self.conn.cursor(dictionary=True) as cursor:
            cursor.execute("SELECT * FROM raw_news WHERE has_processed = FALSE;")
            data_list = cursor.fetchall()
            return [News(**item) for item in data_list]

    def mark_news_as_processed(self, news_ids: List[int]):
        """Mark multiple news articles as processed."""
        if not news_ids:
            return  # No IDs provided, nothing to update

        with self.conn.cursor() as cursor:
            format_strings = ','.join(['%s'] * len(news_ids))
            cursor.execute(f"""
                UPDATE raw_news SET has_processed = TRUE WHERE id IN ({format_strings});
            """, tuple(news_ids))
            self.conn.commit()

    def insert_short_news(self, source: str, timestamp: str, body: str):
        """Insert short news (without a link) into the database."""
        with self.conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO short_news (source, timestamp, body)
                VALUES (%s, %s, %s);
            """, (source, timestamp, body))
            self.conn.commit()

    def insert_long_news(self, source: str, timestamp: str, title: str, body: str, news_link: str):
        """Insert long news (with a link) into the database."""
        with self.conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO long_news (source, timestamp, news_link, title, body)
                VALUES (%s, %s, %s, %s, %s);
            """, (source, timestamp, news_link, title, body))
            self.conn.commit()

    def create_news(self, source: str, timestamp: str, body: str, news_link: Optional[str] = None,
                    title: Optional[str] = None):
        """
        Create news entry. If a link is provided, it's treated as long news; otherwise, it's short news.
        """
        if news_link:
            if not title:
                raise ValueError("Title is required for long news.")
            self.insert_long_news(source, timestamp, title, body, news_link)
        else:
            self.insert_short_news(source, timestamp, body)

    def close(self):
        """Close the database connection."""
        self.conn.close()
