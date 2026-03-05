"""
Thread-safe round-robin GitHub token pool with rate-limit awareness.
Automatically skips exhausted tokens and waits for the earliest reset.
"""

import threading
import time
import logging

logger = logging.getLogger(__name__)


class TokenPool:
    """
    Manages a pool of GitHub Personal Access Tokens.
    Rotates between tokens in round-robin fashion.
    Marks tokens as exhausted when rate-limited and waits/skips accordingly.
    """

    def __init__(self, tokens: list):
        if not tokens:
            raise ValueError("At least one GitHub token is required.")
        self.tokens = list(tokens)
        self._index = 0
        self._lock = threading.Lock()
        # Per-token state: remaining requests + unix reset time
        self._state = {
            tok: {"remaining": 5000, "reset": 0.0}
            for tok in self.tokens
        }
        logger.info(f"TokenPool ready with {len(self.tokens)} token(s).")

    def acquire(self) -> str:
        """
        Return the next non-exhausted token in round-robin order.
        Blocks (with logging) if all tokens are currently rate-limited.
        """
        while True:
            with self._lock:
                now = time.time()
                # Scan all tokens once looking for one with quota
                for _ in range(len(self.tokens)):
                    token = self.tokens[self._index % len(self.tokens)]
                    self._index += 1
                    st = self._state[token]
                    # Refresh if reset window has passed
                    if st["reset"] <= now:
                        st["remaining"] = 5000
                        st["reset"] = 0.0
                    if st["remaining"] > 10:
                        return token

                # All exhausted — find the soonest reset
                soonest = min(st["reset"] for st in self._state.values())
                wait = max(1.0, soonest - now)

            logger.warning(
                f"All {len(self.tokens)} token(s) rate-limited. "
                f"Sleeping {wait:.1f}s until earliest reset…"
            )
            time.sleep(min(wait, 60))

    def update(self, token: str, remaining: int, reset: float):
        """Called after every API response to track remaining quota."""
        with self._lock:
            self._state[token]["remaining"] = remaining
            self._state[token]["reset"] = reset

    def mark_exhausted(self, token: str, reset: float):
        """Immediately mark a token as rate-limited."""
        with self._lock:
            self._state[token]["remaining"] = 0
            self._state[token]["reset"] = reset
            logger.warning(
                f"Token …{token[-4:]} rate-limited. "
                f"Reset at {time.strftime('%H:%M:%S', time.localtime(reset))}."
            )
