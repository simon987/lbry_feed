from hexlib.db import VolatileQueue, VolatileBooleanState


class LbryState:

    def __init__(self):
        self._visited = VolatileBooleanState(prefix="lbry", sep=".")
        self._channel_queue = VolatileQueue("lbry_channel_queue")

    def has_visited(self, item_id):
        return self._visited["byid"][item_id]

    def mark_visited(self, item_id):
        self._visited["byid"][item_id] = True

    def queue_channel(self, channel_id):
        self._channel_queue.put(channel_id)

    def pop_channel(self):
        return self._channel_queue.get()
