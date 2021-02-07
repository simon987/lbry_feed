from hexlib.db import VolatileQueue, VolatileBooleanState
import os

REDIS_HOST = os.environ.get("LF_REDIS_HOST", "localhost")


class LbryState:

    def __init__(self):
        self._visited = VolatileBooleanState(prefix="lbry", host=REDIS_HOST)
        self._channel_queue = VolatileQueue("lbry_channel_queue", host=REDIS_HOST)

    def has_visited(self, item_id):
        return self._visited["byid"][item_id]

    def mark_visited(self, item_id):
        self._visited["byid"][item_id] = True

    def queue_channel(self, channel_id):
        self._channel_queue.put(channel_id)

    def pop_channel(self):
        return self._channel_queue.get()
