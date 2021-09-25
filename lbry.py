import json
import os
from time import time

import requests
from hexlib.log import logger

from state import LbryState

BASE_URL = "https://api.lbry.tv/api"
LIGHTHOUSE_URL = "https://lighthouse.lbry.com"


def now():
    return round(time() * 1000)


class LbryApi:
    def __init__(self):
        self._s = requests.session()

        if os.environ.get("PROXY") is not None:
            self._s.proxies = {
                "http": os.environ.get("PROXY"),
                "https": os.environ.get("PROXY"),
            }

    def _post(self, url, **kwargs):
        r = self._s.post(url, **kwargs)
        logger.debug("GET %s <%d>" % (url, r.status_code))
        return r

    def _get(self, url, **kwargs):
        r = self._s.get(url, **kwargs)
        logger.debug("GET %s <%d>" % (url, r.status_code))
        return r

    def channel_videos(self, channel_id, size=30, page=1):
        j = self._post(
            f"{BASE_URL}/v1/proxy?m=claim_search",
            data=json.dumps({
                "id": now(),
                "jsonrpc": "2.0",
                "method": "claim_search",
                "params": {
                    "channel_ids": [channel_id],
                    "claim_type": ["channel", "repost", "stream"],
                    "fee_amount": ">=0",
                    "include_purchase_receipt": True,
                    "no_totals": True,
                    "not_channel_ids": [],
                    "not_tags": [],
                    "order_by": ["release_time"],
                    "page": page,
                    "page_size": size,
                }
            })
        ).json()

        if j["result"]["items"]:
            def next_page():
                return self.channel_videos(channel_id, size, page + 1)

            return j["result"]["items"], next_page
        return j["result"]["items"], None

    def comment_list(self, claim_id, page_size=99999, page=1):
        j = self._post(
            f"{BASE_URL}/v1/proxy?m=comment_list",
            data=json.dumps({
                "id": now(),
                "jsonrpc": "2.0",
                "method": "comment_list",
                "params": {
                    "claim_id": claim_id,
                    "include_replies": True,
                    "page": page,
                    "page_size": page_size,
                    "skip_validation": True
                }
            })
        ).json()

        return j["result"]["items"] if "items" in j["result"] else []

    def comment_react_list(self, comment_ids):
        j = self._post(
            f"{BASE_URL}/v1/proxy?m=comment_react_list",
            data=json.dumps({
                "id": now(),
                "jsonrpc": "2.0",
                "method": "comment_react_list",
                "params": {
                    "comment_ids": ",".join(comment_ids)
                }
            })
        ).json()

        if "error" in j["result"]:
            return {}

        return j["result"]["others_reactions"]

    def resolve(self, urls):
        j = self._post(
            f"{BASE_URL}/v1/proxy?m=resolve",
            data=json.dumps({
                "id": now(),
                "jsonrpc": "2.0",
                "method": "resolve",
                "params": {
                    "include_is_my_output": False,
                    "include_purchase_receipt": True,
                    "urls": urls
                }
            })
        ).json()

        return j["result"]

    def get_related_videos(self, s, related_to, size=1000, from_=0, nsfw=False):
        if len(s) < 3:
            s = "aaa"
        return self._post(
            f"{LIGHTHOUSE_URL}/search",
            params={
                "s": s,
                "size": size,
                "from": from_,
                "related_to": related_to,
                # Note: I don't think there's a way to get both nsfw & sfw in the same query
                "nsfw": "true" if nsfw else "false"
            }
        ).json()


class LbryWrapper:
    def __init__(self):
        self._api = LbryApi()
        self._state = LbryState()

    def _iter(self, func, *args, **kwargs):
        items, next_page = func(*args, **kwargs)
        for item in items:
            yield item
        while next_page is not None:
            items, next_page = next_page()
            for item in items:
                yield item

    def _get_videos(self, channel_id):
        return self._iter(self._api.channel_videos, channel_id=channel_id)

    def _get_comments(self, claim_id, fetch_reactions=False):
        comments = self._api.comment_list(claim_id)
        comment_ids = [com["comment_id"] for com in comments]
        if fetch_reactions:
            reactions = self._api.comment_react_list(comment_ids)
        else:
            reactions = {}

        for k, v in reactions.items():
            for com in comments:
                if com["comment_id"] == k:
                    com["reactions"] = v
                    break

        return comments

    def _get_related(self, claim):
        j = self._api.get_related_videos(claim["name"], claim["claim_id"])
        return [(f"lbry://{c['name']}#{c['claimId']}", c["claimId"]) for c in j]

    def all_items(self):

        seed_list = [
            # Varg
            "d1bb8684d445e6dd397fc13bfbb14bbe194c7129",
            # Quartering
            "113515e893b8186595595e594ecc410bae50c026",
            # Liberty hangout
            "5499c784a960d96497151f5e0e8434b84ea5da24",
            # Alex Jones
            "cde3b125543e3e930ac2647df957a836e3da3816",
            # ancaps
            "0135b83c29aa82120401f3f9053bf5b0520529ed",
            "b89ed227c49e726fcccf913bdc9dec4c8fec99c2",

            "6caae01aaa534cc4cb2cb1d8d0a8fd4a9553b155",
            "dbe7328c6698c8d8853183f87e50a97a87a33222",
            "8954add966e59c9cba98a143a3387f788a36d7be"
        ]

        for channel_id in seed_list:
            if not self._state.has_visited(channel_id):
                self._state.queue_channel(channel_id)

        while True:
            channel_id = self._state.pop_channel()
            if channel_id is None:
                break

            if self._state.has_visited(channel_id):
                continue

            # re-queue immediately in case of fault: it will be ignored if pop'ed again
            #  only if it got crawled completely
            self._state.queue_channel(channel_id)

            published_channel_data = False

            for claim in self._get_videos(channel_id):

                if "short_url" not in claim["signing_channel"]:
                    continue

                channel_url = claim["signing_channel"]["short_url"]

                if not published_channel_data:
                    channel_data = self._api.resolve([channel_url])[channel_url]
                    yield channel_data, "channel"
                    published_channel_data = True

                if not self._state.has_visited(claim["claim_id"]):
                    yield claim, "video"

                    for comment in self._get_comments(claim["claim_id"]):
                        yield comment, "comment"

                    related_to_resolve = []
                    for rel_url, rel_id in self._get_related(claim):
                        if not self._state.has_visited(rel_id):
                            related_to_resolve.append(rel_url)
                    for rel_url, rel_claim in self._api.resolve(related_to_resolve).items():
                        if "error" in rel_claim:
                            continue

                        rel_claim["_related_to"] = claim["claim_id"]
                        yield rel_claim, "video"

                        for rel_comment in self._get_comments(rel_claim["claim_id"]):
                            yield rel_comment, "comment"

                        if "signing_channel" in rel_claim and "channel_id" in rel_claim["signing_channel"]:
                            rel_channel_id = rel_claim["signing_channel"]["channel_id"]
                            if not self._state.has_visited(rel_channel_id):
                                self._state.queue_channel(rel_channel_id)

                        self._state.mark_visited(rel_claim["claim_id"])
                    self._state.mark_visited(claim["claim_id"])
            self._state.mark_visited(channel_id)

        logger.warning("No more channels to crawl!")
