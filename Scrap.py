from fake_useragent import UserAgent
from aiohttp_socks import ChainProxyConnector
import aiohttp
import asyncio
from aiohttp_retry import RetryClient, ExponentialRetry
from bs4 import BeautifulSoup
from data_base import get_values, create_connection, create_database, execute_query, create_users_table
from log import proxy_verification1, proxy_verification2, BD_host_name, Bd_user_name, BD_password
import re
import random


class DataScraping:
    def __init__(self):
        self.main_page_links = []
        self.detailed_links = []

    def get_all_pages(self, link):
        self.main_page_links.append(link)
        for page_num in range(1, 41):
            self.main_page_links.append(f"{link}pn={page_num}")


    @staticmethod
    async def _get_data(session, link):
        async with session.get(link) as response:
            resp = await response.text()
            soup = BeautifulSoup(resp, "lxml")
            price = soup.select("p._194zg6t3")[0].text
            info = soup.select("li._1wz55u82")
            details = soup.select("svg.k6cr000")
            name = link.split("_")[1]
            match = re.search(r"£([\d,]+)", price)
            detail_str = ", ".join([str(d) for d in details])
            info_str = ", ".join([str(i) for i in info])
            if match:
                price_str = match.group(1)
                price_int = int(price_str.replace(",", ""))
            get_values(name, price_int, info_str, detail_str, link)


    async def detailed_page(self, session, link):
        retry_options = ExponentialRetry(attempts=5)
        retry_client = RetryClient(
            raise_for_status=False,
            retry_options=retry_options,
            client_session=session,
            start_timeout=0.5,
        )
        async with retry_client.get(link) as response:
            if response.ok:
                resp = await response.text()
                soup = BeautifulSoup(resp, "lxml")
                tasks = [
                    self._get_data(session, detail_url["href"])
                    for detail_url in soup.select("a._1lw0o5c2")
                ]
                await asyncio.gather(*tasks)

    async def main(self):
        ua = UserAgent()
        fake_ua = {"user-agent": ua.random}
        connector = ChainProxyConnector.from_urls(
            [
                f"socks5://{proxy_verification2}",
                f"socks5://{proxy_verification1}",
            ]
        )
        async with aiohttp.ClientSession(
            connector=connector, headers=fake_ua
        ) as session:
            tasks = []
            for link in self.main_page_links:
                await asyncio.sleep(random.uniform(1, 2))
                tasks.append(self.detailed_page(session, link))
            await asyncio.gather(*tasks)


    def __call__(self, link, *args, **kwargs):
        connect = create_connection(BD_host_name, Bd_user_name, BD_password, None)

        create_database_query = "CREATE DATABASE IF NOT EXISTS rent_price_uk"
        create_database(connect, create_database_query)

        connect = create_connection(BD_host_name, Bd_user_name, BD_password, "rent_price_uk")

        execute_query(connect, create_users_table)

        self.get_all_pages(link)
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(self.main())


url = "website"
parsing = DataScraping()
parsing(url)
