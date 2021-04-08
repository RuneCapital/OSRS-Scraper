import asyncio
import logging
import os
import sys
from typing import List

import asyncpg
import httpx
from pydantic import BaseModel
from pydantic.error_wrappers import ValidationError
from pydantic.fields import Field

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setFormatter(
    logging.Formatter("[%(module)s] %(asctime)s %(levelname)-8s %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.addHandler(stdout_handler)

USER_AGENT_HEADER = {"User-Agent": f"RuneCapital Data Visualization {os.environ['CONTACT_INFO']}"}


class Item(BaseModel):
    id: int
    name: str
    members: bool = False
    buylimit: int = Field(1, alias="limit")
    value: int = 1
    highalch: int = 1
    lowalch: int = 1
    examine: str = ""


async def main():
    conn = await asyncpg.connect()
    client = httpx.AsyncClient()
    async with client:
        r = await client.get("https://prices.runescape.wiki/api/v1/osrs/mapping", headers=USER_AGENT_HEADER)
        response_obj = r.json()
        try:
            transformed_obj: List[Item] = [Item(**item) for item in response_obj]

            data = [tuple(item.dict().values()) for item in transformed_obj]
            await conn.executemany("""
            INSERT INTO osrs_item_info (id, name, members, buylimit, value, highalch, lowalch, examine) VALUES ($1, 
            $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, members = 
            EXCLUDED.members, buylimit = EXCLUDED.buylimit, value = EXCLUDED.value, highalch = EXCLUDED.highalch, 
            lowalch = EXCLUDED.lowalch, examine = EXCLUDED.examine;
            """, data)
        except ValidationError as e:
            logger.exception(e)
            exit(1)

    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
