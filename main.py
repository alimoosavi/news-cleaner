import asyncio
import logging
from collections import defaultdict

from config import config
from db_manager import DBManager
from news_crawler_client import NewsCrawlerClient
from schema import CleanedNews
from utils import preprocess_persian_document, extract_link

NEWS_SOURCES = ['IRNA', 'ISNA', 'FARS', 'JAHAN_FOURI']


async def process_news(db_manager: DBManager, news_crawler_client: NewsCrawlerClient, logger: logging.Logger):
    """Processes unprocessed news by extracting links, fetching new content, and storing results."""
    short_news_mapping = defaultdict(list)
    news_links_mapping = defaultdict(set)

    unprocessed_news = db_manager.get_unprocessed_news()
    if not unprocessed_news:
        logger.info("No unprocessed news found.")
        return

    for item in unprocessed_news:
        links = extract_link(item.content, item.source)
        if links:
            news_links_mapping[item.source].update(links)
        else:
            short_news_mapping[item.source].append(
                CleanedNews(
                    source=item.source,
                    content=preprocess_persian_document(item.content),
                    published_date=item.published_date,
                )
            )

    BATCH_SIZE = 30
    for source in NEWS_SOURCES:
        links = list(news_links_mapping[source])
        batch = links[:BATCH_SIZE]
        idx = 0
        while len(batch) != 0:
            try:
                news = await news_crawler_client.fetch_news(source, batch)
                logger.info(f"Fetched {len(news)} {source} news items.")

                for link, item in news.items():
                    print(' link ', link, ' title ', item['title'], ' body ', item['body'], '\n\n\n ***** \n\n\n')

            except Exception as e:
                logger.error(f"Failed to fetch {source} news: {e}")

            finally:
                idx += 1
                batch = links[idx * BATCH_SIZE:(idx + 1) * BATCH_SIZE]


async def main():
    """Main function to initialize components and start processing news."""
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("news_collector")

    # Initialize DBManager
    db_manager = DBManager(
        db_name=config.database.name,
        user=config.database.user,
        password=config.database.password,
        host=config.database.hostname,
        port=config.database.port,
    )

    # Initialize NewsCrawlerClient
    news_crawler_client = NewsCrawlerClient(base_url=config.news_crawler.base_url)

    # Process news asynchronously
    await process_news(db_manager=db_manager, news_crawler_client=news_crawler_client, logger=logger)

    # Close DB connection
    db_manager.close()


if __name__ == "__main__":
    asyncio.run(main())
