import asyncio
import json
import logging
import sys
from random import random
from time import time

import peewee
from httpx import HTTPError

from igram.client import AsyncClient
from igram.exceptions import ChallengeException, NeedReconnectException, AttemptsException, LimitException, \
    SpamException, ParsingLimitException
from parsing.models import Account

formatter = logging.Formatter('[%(asctime)s][%(levelname)s]: %(message)s')
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


class Parser:
    def __init__(self, parser_uuid, proxy=None):
        self._account = None
        self._client = None
        self._start_time = time()
        self._next_log = time() + 60
        self._parsed_cnt = 0
        self._parser_uuid = parser_uuid
        self._proxy = proxy
        self._ticker = time()

    async def check_activity(self):
        if self._ticker + 610 <= time():
            print('Parser update activity:')
            print(await self.update_web_timeline_feed())
            print(await self.update_timeline_feed())
            self._ticker = time()

    async def _check_account(self):
        try:
            await self._client.api.username_info('cristiano')  # TODO: Make normal check
        except (ChallengeException, NeedReconnectException):
            await self._broken_account_event()
            return True
        except:
            return False
        return False

    def _set_free_account(self):
        try:
            account = Account.get_free_parsing_account()
            if not account.try_lock(self._parser_uuid):
                self._set_free_account()
            else:
                self._account = account
                logger.debug(f'Set account {self._account.login} for parser. Left today followers limit: {25000 - self._account.parsed_today}')

                self._client = AsyncClient(self._account.login, self._account.password, 'http://havrylovmykola_gmail_com:C8CqA5xk@yo-pvt-9.airsocks.in:20930', settings=json.loads(self._account.settings))
        except Account.DoesNotExist:
            logger.warning(f'No more free parsing accounts.')
            raise RuntimeError('No more free parsing accounts.')

    async def _broken_account_event(self):
        logger.warning(f'Broken account: {self._account.login}.')
        await self._client.api.session.close()
        self._account.delete_instance()
        self._set_free_account()

    def free(self):
        if self._account:
            self._account.try_unlock(self._parser_uuid)
            self._account = None

    async def likers_list(self, post_shortcode, max_id):
        if not self._account:
            self._set_free_account()

        attempts = 6
        count = 30

        while True:
            try:
                await self.check_activity()

                response = await self._client.api.likers_list(post_shortcode, max_id)
                count = len(response['data']['shortcode_media']['edge_liked_by']['edges'])
                if not response:
                    if not await self._check_account():
                        await asyncio.sleep(10)
                    attempts -= 1
                    if not attempts:
                        raise HTTPError()
                    continue
                else:
                    attempts = 6
                break
            except (ChallengeException, NeedReconnectException) as e:
                await self._broken_account_event()
            except (AttemptsException, TypeError, ParsingLimitException):
                if not await self._check_account():
                    await asyncio.sleep(120)
                attempts -= 1
                if not attempts:
                    raise HTTPError()
                continue

        logger.debug(f'Parsed {count} likers using account {self._account.login}.')
        # self._account.parsed_today += count
        self._account.save()
        self._parsed_cnt += count

        # if self._account.parsed_today >= 25000:
        #     await self._client.api.session.close()
        #     self._account.try_unlock(self._parser_uuid)
        #     self._account.set_limited()
        #     self._set_free_account()

        if self._next_log < time():
            logger.info(f'Parsing speed {int(self._parsed_cnt * (86400 / (time() - self._start_time)))} followers/day.')
            self._next_log = time() + 180

        return response

    async def followers(self, user_id, max_id=None):
        if not self._account:
            self._set_free_account()

        attempts = 6
        count = 30

        while True:
            try:
                await self.check_activity()

                response = await self._client.api.followers(user_id, max_id)
                count = len(response['data']['user']['edge_followed_by']['edges'])
                if not response:
                    if not await self._check_account():
                        await asyncio.sleep(10)
                    attempts -= 1
                    if not attempts:
                        raise HTTPError()
                    continue
                else:
                    attempts = 6
                break
            except (ChallengeException, NeedReconnectException) as e:
                await self._broken_account_event()
            except (AttemptsException, TypeError, ParsingLimitException):
                if not await self._check_account():
                    await asyncio.sleep(120)
                attempts -= 1
                if not attempts:
                    raise HTTPError()
                continue

        logger.debug(f'Parsed {count} followers using account {self._account.login}.')
        self._account.parsed_today += count
        self._account.save()
        self._parsed_cnt += count

        if self._account.parsed_today >= 25000:
            await self._client.api.session.close()
            self._account.try_unlock(self._parser_uuid)
            self._account.set_limited()
            self._set_free_account()

        if self._next_log < time():
            logger.info(f'Parsing speed {int(self._parsed_cnt * (86400 / (time() - self._start_time)))} followers/day.')
            self._next_log = time() + 180

        return response

    async def user_by_username(self, username):
        if not self._account:
            self._set_free_account()

        attempts = 6

        while True:
            try:
                await self.check_activity()

                response = await self._client.api.user_by_username(username)
                if not response:
                    if not await self._check_account():
                        await asyncio.sleep(10)
                    attempts -= 1
                    if not attempts:
                        raise HTTPError()
                    continue

                return response
            except (ChallengeException, NeedReconnectException) as e:
                await self._broken_account_event()

    async def user_posts(self, username):
        if not self._account:
            self._set_free_account()

        attempts = 6

        while True:
            try:
                await self.check_activity()

                response = await self._client.api.graph_username_info(username)
                if not response:
                    if not await self._check_account():
                        await asyncio.sleep(10)
                    attempts -= 1
                    if not attempts:
                        raise HTTPError()
                    continue

                posts = response['user']['edge_owner_to_timeline_media']
                return posts
            except (ChallengeException, NeedReconnectException) as e:
                await self._broken_account_event()

    async def update_web_timeline_feed(self):
        if not self._account:
            self._set_free_account()

        attempts = 6

        while True:
            try:
                response = await self._client.api.web_feed_timeline()
                if not response:
                    if not await self._check_account():
                        await asyncio.sleep(10)
                    attempts -= 1
                    if not attempts:
                        raise HTTPError()
                    continue
            except (ChallengeException, NeedReconnectException) as e:
                await self._broken_account_event()
            except HTTPError:
                await self._check_account()

    async def update_timeline_feed(self):
        if not self._account:
            self._set_free_account()

        attempts = 6

        while True:
            try:
                response = await self._client.api.feed_timeline()
                if not response:
                    if not await self._check_account():
                        await asyncio.sleep(10)
                    attempts -= 1
                    if not attempts:
                        raise HTTPError()
                    continue
            except (ChallengeException, NeedReconnectException) as e:
                await self._broken_account_event()
            except HTTPError:
                await self._check_account()

    async def get_reel(self, user_id):
        if not self._account:
            self._set_free_account()

        attempts = 6
        limit_attempts = 6

        while True:
            try:
                await self.check_activity()

                response = await self._client.get_reel(user_id)
                if not response:
                    if not await self._check_account():
                        await asyncio.sleep(10)
                    attempts -= 1
                    if not attempts:
                        raise HTTPError()
                    continue

                return response
            except (LimitException, SpamException):
                limit_attempts -= 1
                if not limit_attempts:
                    await self._broken_account_event()
                    limit_attempts = 6
                else:
                    await asyncio.sleep(90)
            except (ChallengeException, NeedReconnectException) as e:
                await self._broken_account_event()

    async def get_reels_web(self, user_ids):
        if not self._account:
            self._set_free_account()

        attempts = 6
        limit_attempts = 6

        while True:
            try:
                await self.check_activity()

                response = await self._client.get_reels_web(user_ids)
                if not response:
                    if not await self._check_account():
                        await asyncio.sleep(10)
                    attempts -= 1
                    if not attempts:
                        raise HTTPError()
                    continue

                return response
            except (LimitException, SpamException, ParsingLimitException, AttemptsException, IndexError, KeyError, ValueError):
                limit_attempts -= 1
                if not limit_attempts:
                    await self._broken_account_event()
                    limit_attempts = 6
                else:
                    await asyncio.sleep(90)
            except (ChallengeException, NeedReconnectException) as e:
                await self._broken_account_event()
