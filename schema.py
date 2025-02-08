from datetime import datetime

from pydantic import BaseModel
from typing import List


class News(BaseModel):
    source: str
    content: str
    published_date: datetime

    def dict(self, **kwargs):
        news_dict = super().dict(**kwargs)
        news_dict['published_date'] = self.published_date.isoformat()
        return news_dict

    class Config:
        json_schema_extra = {
            "example": {
                "source": "IRNA",
                "content": "The Iranian government has announced a series of economic reforms...",
                "published_date": "2023-10-01T12:34:56Z"
            }
        }


class CleanedNews(BaseModel):
    source: str
    content: str
    published_date: datetime
