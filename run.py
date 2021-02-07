import json
import redis
import os

from lbry import LbryWrapper

REDIS_HOST = os.environ.get("LF_REDIS_HOST", "localhost")


def publish(item, item_type):
    routing_key = f"arc.lbry.{item_type}.x"

    if item_type == "video":
        item["_id"] = item["claim_id"]
    elif item_type == "comment":
        item["_id"] = item["comment_id"]
    elif item_type == "channel":
        item["_id"] = item["claim_id"]

    message = json.dumps(item, separators=(',', ':'), ensure_ascii=False, sort_keys=True)
    rdb.lpush(routing_key, message)


if __name__ == '__main__':
    lbry = LbryWrapper()

    rdb = redis.Redis(host=REDIS_HOST)

    for item, item_type in lbry.all_items():
        publish(item, item_type)
