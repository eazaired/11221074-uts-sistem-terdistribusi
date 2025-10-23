import time

class Uptime:
    """Penghitung sederhana untuk lama service berjalan."""
    def __init__(self):
        self.start = time.time()

    @property
    def seconds(self) -> float:
        return time.time() - self.start
