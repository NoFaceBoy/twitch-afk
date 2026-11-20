# -*- coding: utf-8 -*-
"""
Persistent Watch Streak state, one JSON file per account: streaks/<username>.json

For every streamer it keeps the broadcast currently (or last) watched, whether the
WATCH_STREAK bonus was already earned for it and how many minutes were watched, plus the
streak length and the missed broadcasts reported by Twitch.

Without it a restart in the middle of a stream made the miner believe the streak bonus
was still missing, so it spent one of the two watch slots on that streamer again.
"""

import json
import logging
import os
import tempfile
import threading
import time
from pathlib import Path

logger = logging.getLogger(__name__)

STREAK_STORE_VERSION = 1
# Write minutes watched at most this often (they change every ~20 seconds)
MINUTES_SAVE_INTERVAL = 60  # seconds
# Forget streamers that haven't been seen for this long
STALE_TTL = 60 * 24 * 60 * 60  # seconds


class StreakStore(object):
    def __init__(self, path):
        self.path = os.fspath(path)
        self.streamers = {}
        self.lock = threading.RLock()
        self.last_minutes_save = 0
        self.load()

    @classmethod
    def for_account(cls, username):
        return cls(
            os.path.join(Path().absolute(), "streaks", f"{username.lower().strip()}.json")
        )

    # === Persistence === #
    def load(self):
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                payload = json.load(f)
        except FileNotFoundError:
            return
        except (OSError, ValueError) as e:
            logger.warning(f"Unable to read the watch streak state from {self.path}: {e}")
            return
        if not isinstance(payload, dict) or payload.get("version") != STREAK_STORE_VERSION:
            logger.warning(f"Unsupported watch streak state in {self.path}, starting from scratch")
            return
        streamers = payload.get("streamers")
        if isinstance(streamers, dict):
            self.streamers = {
                login: entry for login, entry in streamers.items() if isinstance(entry, dict)
            }

    def save(self):
        with self.lock:
            now = time.time()
            self.streamers = {
                login: entry
                for login, entry in self.streamers.items()
                if now - (entry.get("updated_at") or 0) < STALE_TTL
            }
            payload = {"version": STREAK_STORE_VERSION, "streamers": self.streamers}
            target = Path(self.path)
            temp_path = None
            try:
                target.parent.mkdir(parents=True, exist_ok=True)
                # Write to a temporary file first, so a crash can't leave a broken file
                with tempfile.NamedTemporaryFile(
                    "w",
                    encoding="utf-8",
                    dir=target.parent,
                    prefix=f".{target.name}.",
                    suffix=".tmp",
                    delete=False,
                ) as f:
                    temp_path = f.name
                    json.dump(payload, f, indent=2, sort_keys=True)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(temp_path, target)
            except OSError as e:
                logger.warning(f"Unable to save the watch streak state to {target}: {e}")
                if temp_path is not None:
                    try:
                        os.unlink(temp_path)
                    except OSError:
                        pass

    # === State === #
    def __entry(self, login):
        entry = self.streamers.setdefault(login, {})
        entry["updated_at"] = time.time()
        return entry

    def get(self, login):
        with self.lock:
            entry = self.streamers.get(login)
            return dict(entry) if entry is not None else None

    def stream_started(self, login, broadcast_id, started_at=None):
        """
        Called when a streamer goes online.
        :return: the stored state of this broadcast if it was already seen (miner restarted
                 during the stream), None for a new broadcast.
        """
        if not broadcast_id:
            return None
        broadcast_id = str(broadcast_id)
        with self.lock:
            entry = self.__entry(login)
            if entry.get("broadcast_id") == broadcast_id:
                entry["ended_at"] = None
                self.save()
                return dict(entry)
            entry.update(
                {
                    "broadcast_id": broadcast_id,
                    "started_at": started_at or time.time(),
                    "ended_at": None,
                    "claimed": False,
                    "claimed_at": None,
                    "minutes_watched": 0,
                }
            )
            self.save()
            return None

    def stream_ended(self, login, broadcast_id, ended_at=None):
        with self.lock:
            entry = self.streamers.get(login)
            if entry is None or not broadcast_id or entry.get("broadcast_id") != str(broadcast_id):
                return
            entry["ended_at"] = ended_at or time.time()
            entry["updated_at"] = time.time()
            self.save()

    def streak_claimed(self, login, broadcast_id):
        with self.lock:
            entry = self.__entry(login)
            if broadcast_id and entry.get("broadcast_id") != str(broadcast_id):
                entry.update(
                    {"broadcast_id": str(broadcast_id), "started_at": time.time(), "ended_at": None}
                )
            entry["claimed"] = True
            entry["claimed_at"] = time.time()
            self.save()

    def minutes_watched(self, login, broadcast_id, minutes):
        with self.lock:
            entry = self.streamers.get(login)
            if entry is None or not broadcast_id or entry.get("broadcast_id") != str(broadcast_id):
                return
            entry["minutes_watched"] = round(minutes, 2)
            entry["updated_at"] = time.time()
            if time.time() - self.last_minutes_save >= MINUTES_SAVE_INTERVAL:
                self.last_minutes_save = time.time()
                self.save()

    def streak_status(self, login, streak, missed_streams):
        """Streak length and missed broadcasts, as reported by Twitch (RewardList)."""
        with self.lock:
            entry = self.__entry(login)
            entry["streak"] = streak
            entry["missed_streams"] = sorted(missed_streams)
            self.save()
