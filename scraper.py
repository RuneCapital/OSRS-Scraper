import asyncio
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Optional

import asyncpg
import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setFormatter(
    logging.Formatter("[%(module)s] %(asctime)s %(levelname)-8s %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.addHandler(stdout_handler)

USER_AGENT_HEADER = {"User-Agent": f"RuneCapital - {os.environ['CONTACT_INFO']}"}


def utcnow():
    return datetime.now(timezone.utc)


class FiveMinData(BaseModel):
    timestamp: datetime
    id: int
    buyprice: Optional[int] = Field(None, alias="avgLowPrice")
    sellprice: Optional[int] = Field(None, alias="avgHighPrice")
    buyvolume: Optional[int] = Field(None, alias="lowPriceVolume")
    sellvolume: Optional[int] = Field(None, alias="highPriceVolume")


class RealtimeData(BaseModel):
    timestamp: datetime
    id: int
    price: Optional[int]
    pricetype: Optional[str]


async def push_5min_data():
    conn = await asyncpg.connect()
    client = httpx.AsyncClient()
    async with client:
        logger.info("Fetching five minute data")
        five_minute_response_raw = await client.get("https://prices.runescape.wiki/api/v1/osrs/5m",
                                                    headers=USER_AGENT_HEADER)
        five_minute_response_obj = five_minute_response_raw.json()
        current_timestamp: datetime = datetime.fromtimestamp(five_minute_response_obj["timestamp"],
                                                             timezone.utc).replace(second=0, microsecond=0)
        # Round down to nearest 5 minutes
        current_timestamp.replace(minute=current_timestamp.minute - (current_timestamp.minute % 5))

        five_min_data_all = [tuple(FiveMinData(**info, id=item_id, timestamp=current_timestamp).dict().values()) for
                             item_id, info in five_minute_response_obj["data"].items()]

        five_min_data_all.sort(key=lambda x: x[1])  # Sort by id

        logger.info("Pushing five minute data")
        await conn.executemany("""
                INSERT INTO osrs_five_min (timestamp, id, buyprice, sellprice, buyvolume, sellvolume) VALUES ($1, $2, 
                $3, $4, $5, $6)
                """, five_min_data_all)
    await conn.close()

    logger.info("Five minute data pushed")


async def push_realtime_data():
    conn = await asyncpg.connect()
    client = httpx.AsyncClient()
    async with client:
        logger.info("Fetching realtime data")
        realtime_response_raw = await client.get("https://prices.runescape.wiki/api/v1/osrs/latest",
                                                 headers=USER_AGENT_HEADER)
        realtime_response_obj = realtime_response_raw.json()
        realtime_data_all = []

        for item_id, info in realtime_response_obj["data"].items():
            if info.get("lowTime", None) and info.get("low", None):
                buy_timestamp = datetime.fromtimestamp(info["lowTime"], timezone.utc)
                buy = RealtimeData(timestamp=buy_timestamp, id=item_id, price=info["low"], pricetype="buy")
                realtime_data_all.append(tuple(buy.dict().values()))

            if info.get("highTime", None) and info.get("high", None):
                sell_timestamp = datetime.fromtimestamp(info["highTime"], timezone.utc)
                sell = RealtimeData(timestamp=sell_timestamp, id=item_id, price=info["high"], pricetype="sell")
                realtime_data_all.append(tuple(sell.dict().values()))

        realtime_data_all.sort(key=lambda x: x[1])  # Sort by id

        logger.info("Pushing realtime data")
        await conn.executemany("""
                        INSERT INTO osrs_realtime (timestamp, id, price, pricetype) VALUES ($1, $2, 
                        $3, $4) ON CONFLICT DO NOTHING;
                        """, realtime_data_all)
    await conn.close()

    logger.info("Realtime data pushed")


if __name__ == "__main__":
    scheduler = AsyncIOScheduler()
    scheduler.add_job(push_5min_data, "cron",
                      minute="1-59/5")  # Scrape 5 minute data every 5 minutes starting at minute 1
    scheduler.add_job(push_realtime_data, "cron", minute="*",
                      second="10, 40")  # Scrape realtime data every minute starting at second 10

    scheduler.start()
    asyncio.get_event_loop().run_forever()
