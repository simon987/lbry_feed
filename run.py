import json

from hexlib.env import get_redis

from lbry import LbryWrapper


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

    rdb = get_redis()

    for item, item_type in lbry.all_items():
        publish(item, item_type)
