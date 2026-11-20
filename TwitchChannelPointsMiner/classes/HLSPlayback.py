# -*- coding: utf-8 -*-
"""
Simulated HLS playback of the watched streams.

Since 2026-10-07 Twitch only credits time-based Drop progress while the stream's media
segments are being requested, the way a real player does (#846). A HEAD request for the
latest segment every 20 seconds, as the miner used to do, is no longer enough.

For every watched stream this polls the lowest quality media playlist every few seconds
on a background thread and sends a HEAD request for each segment it hasn't requested
yet, so no audio or video is downloaded. The signed playlist URL is resolved once per
broadcast (instead of a PlaybackAccessToken request every 20 seconds) and re-resolved
when it expires.
"""

import copy
import logging
import random
import threading
import time
from collections import OrderedDict
from urllib.parse import urljoin

import requests

from TwitchChannelPointsMiner.constants import GQLOperations

logger = logging.getLogger(__name__)

MASTER_PLAYLIST_URL = "https://usher.ttvnw.net/api/channel/hls/{login}.m3u8"

# Twitch segments are ~2s long and a live playlist keeps a few of them,
# poll often enough not to miss any.
POLL_INTERVAL = 6  # seconds
# A stream stays active this long after the watch loop last asked for it.
ACTIVE_TIMEOUT = 60  # seconds
# Forget the state of a stream that hasn't been watched for this long.
IDLE_TIMEOUT = 10 * 60  # seconds
# Re-resolve the signed playlist URL at least this often (the token expires).
PLAYLIST_MAX_AGE = 15 * 60  # seconds
# Don't try to re-resolve a failing playlist more often than this.
RESOLVE_RETRY = 30  # seconds
# Responses meaning the signed playlist or segment URL is no longer valid.
EXPIRED_STATUS_CODES = (401, 403, 404, 410)
# Report a stream as stalled after this many consecutive failed polls.
STALL_THRESHOLD = 5
SEEN_SEGMENTS_LIMIT = 256
MAX_SEGMENTS_PER_POLL = 30
PLAYLIST_TIMEOUT = (5, 15)
SEGMENT_TIMEOUT = (5, 10)


def parse_master_playlist(text, base_url):
    """Returns the URL of the lowest bandwidth variant of a master playlist."""
    variants = []
    lines = [line.strip() for line in text.splitlines()]
    for i, line in enumerate(lines):
        if line.startswith("#EXT-X-STREAM-INF:"):
            bandwidth = None
            for attribute in line.split(":", 1)[1].split(","):
                if attribute.startswith("BANDWIDTH="):
                    try:
                        bandwidth = int(attribute.split("=", 1)[1])
                    except ValueError:
                        pass
            uri = next(
                (u for u in lines[i + 1:] if u and not u.startswith("#")), None
            )
            if uri is not None:
                variants.append((bandwidth if bandwidth is not None else 0, urljoin(base_url, uri)))
    if variants:
        return min(variants, key=lambda v: v[0])[1]
    # Fallback: the last URL in the playlist used to be the lowest quality
    uris = [line for line in lines if line and not line.startswith("#")]
    return urljoin(base_url, uris[-1]) if uris else None


def parse_media_playlist(text, base_url):
    """Returns the URLs of the media segments of a media playlist, oldest first."""
    segments = []
    expect_uri = False
    for line in (line.strip() for line in text.splitlines()):
        if not line:
            continue
        if line.startswith("#EXTINF:"):
            expect_uri = True
        elif not line.startswith("#"):
            if expect_uri:
                segments.append(urljoin(base_url, line))
            expect_uri = False
    return segments


class StreamState(object):
    __slots__ = [
        "username",
        "broadcast_id",
        "media_playlist_url",
        "resolved_at",
        "next_resolve",
        "seen_segments",
        "last_requested",
        "last_success",
        "failures",
        "stalled",
        "segments_requested",
        "lock",
    ]

    def __init__(self, username, broadcast_id):
        self.username = username
        self.broadcast_id = broadcast_id
        self.media_playlist_url = None
        self.resolved_at = 0
        self.next_resolve = 0
        self.seen_segments = OrderedDict()
        self.last_requested = time.time()
        self.last_success = 0
        self.failures = 0
        self.stalled = False
        self.segments_requested = 0
        self.lock = threading.Lock()

    def reset_playlist(self):
        self.media_playlist_url = None


class HLSPlayback(object):
    def __init__(self, twitch):
        self.twitch = twitch
        self.states = {}
        self.lock = threading.Lock()
        self.thread = None
        self.session = requests.Session()

    # === Public API === #
    def watch(self, streamer):
        """
        Marks the stream as watched and polls it right away.
        Called by the minute-watched loop for every watched streamer.
        :return: True if the stream's segments are being requested successfully.
        """
        state = self.__state(streamer)
        state.last_requested = time.time()
        self.__ensure_thread()
        with state.lock:
            self.__poll(state)
        return state.failures == 0

    def stop(self):
        with self.lock:
            self.states.clear()
        self.session.close()

    # === Internals === #
    def __state(self, streamer):
        with self.lock:
            state = self.states.get(streamer.username)
            broadcast_id = streamer.stream.broadcast_id
            if state is None or (
                broadcast_id is not None
                and state.broadcast_id is not None
                and state.broadcast_id != broadcast_id
            ):
                # New broadcast: new playlist and segments
                state = StreamState(streamer.username, broadcast_id)
                self.states[streamer.username] = state
            elif state.broadcast_id is None:
                state.broadcast_id = broadcast_id
            return state

    def __ensure_thread(self):
        with self.lock:
            if self.thread is None or not self.thread.is_alive():
                self.thread = threading.Thread(target=self.__run, daemon=True)
                self.thread.name = "HLS playback"
                self.thread.start()

    def __run(self):
        while self.twitch.running:
            time.sleep(POLL_INTERVAL + random.uniform(-1, 1))
            now = time.time()
            with self.lock:
                for username, state in list(self.states.items()):
                    if now - state.last_requested > IDLE_TIMEOUT:
                        del self.states[username]
                active = [
                    state
                    for state in self.states.values()
                    if now - state.last_requested <= ACTIVE_TIMEOUT
                ]
            for state in active:
                if not self.twitch.running:
                    break
                # Skip if the watch loop is polling it right now
                if state.lock.acquire(blocking=False):
                    try:
                        self.__poll(state)
                    except Exception:
                        logger.debug(
                            f"Unexpected error while polling {state.username} segments",
                            exc_info=True,
                        )
                    finally:
                        state.lock.release()

    def __resolve(self, state):
        """Resolves the signed URL of the lowest quality media playlist."""
        state.next_resolve = time.time() + RESOLVE_RETRY
        json_data = copy.deepcopy(GQLOperations.PlaybackAccessToken)
        json_data["variables"] = {
            "login": state.username,
            "isLive": True,
            "isVod": False,
            "vodID": "",
            "playerType": "site",
            "platform": "web",
        }
        response = self.twitch.post_gql_request(json_data)
        token = ((response or {}).get("data") or {}).get("streamPlaybackAccessToken") or {}
        signature, value = token.get("signature"), token.get("value")
        if not signature or not value:
            logger.debug(f"Invalid PlaybackAccessToken response for {state.username}: {response}")
            return False

        master_url = MASTER_PLAYLIST_URL.format(login=state.username)
        response = self.twitch.request(
            "GET",
            master_url,
            params={"sig": signature, "token": value},
            headers={"User-Agent": self.twitch.user_agent},
            timeout=PLAYLIST_TIMEOUT,
        )
        if response.status_code != 200:
            logger.debug(
                f"Master playlist request for {state.username} failed: {response.status_code}"
            )
            return False

        media_playlist_url = parse_master_playlist(response.text, response.url)
        if media_playlist_url is None:
            logger.debug(f"No variant in the master playlist of {state.username}")
            return False

        state.media_playlist_url = media_playlist_url
        state.resolved_at = time.time()
        return True

    def __poll(self, state):
        try:
            ok = self.__poll_segments(state)
        except requests.exceptions.RequestException as e:
            logger.debug(f"Segment request for {state.username} failed: {e}")
            ok = False

        if ok:
            if state.stalled:
                logger.info(f"Stream playback of {state.username} recovered")
            state.failures = 0
            state.stalled = False
            state.last_success = time.time()
        else:
            state.failures += 1
            if state.failures >= STALL_THRESHOLD and not state.stalled:
                state.stalled = True
                logger.warning(
                    f"Unable to request stream segments of {state.username}, "
                    "Drops progress may not be counted"
                )

    def __poll_segments(self, state):
        now = time.time()
        if (
            state.media_playlist_url is None
            or now - state.resolved_at > PLAYLIST_MAX_AGE
        ) and now >= state.next_resolve:
            # If a refresh fails, keep using the current URL until it expires
            self.__resolve(state)
        if state.media_playlist_url is None:
            return False

        response = self.session.get(
            state.media_playlist_url,
            headers={"User-Agent": self.twitch.user_agent},
            timeout=PLAYLIST_TIMEOUT,
            verify=self.__verify(),
        )
        if response.status_code in EXPIRED_STATUS_CODES:
            # Signed URL expired or the broadcast ended, resolve again on the next poll
            state.reset_playlist()
            state.next_resolve = 0
            return False
        if response.status_code != 200:
            return False

        segments = parse_media_playlist(response.text, response.url)
        if not segments:
            return False

        new_segments = [s for s in segments if s not in state.seen_segments]
        # On the first poll of a stream only request the most recent segments
        for segment in new_segments[:-MAX_SEGMENTS_PER_POLL]:
            state.seen_segments[segment] = True
        for segment in new_segments[-MAX_SEGMENTS_PER_POLL:]:
            segment_response = self.session.head(
                segment,
                headers={"User-Agent": self.twitch.user_agent},
                timeout=SEGMENT_TIMEOUT,
                verify=self.__verify(),
            )
            if segment_response.status_code in EXPIRED_STATUS_CODES:
                state.reset_playlist()
                state.next_resolve = 0
                return False
            if segment_response.status_code != 200:
                logger.debug(
                    f"Segment request for {state.username} failed: {segment_response.status_code}"
                )
                return False
            state.seen_segments[segment] = True
            state.segments_requested += 1
            while len(state.seen_segments) > SEEN_SEGMENTS_LIMIT:
                state.seen_segments.popitem(last=False)

        logger.debug(
            f"Requested {len(new_segments)} new segments of {state.username} "
            f"({len(segments)} in playlist, {state.segments_requested} total)"
        )
        return True

    @staticmethod
    def __verify():
        from TwitchChannelPointsMiner.classes.Settings import Settings

        return getattr(Settings, "disable_ssl_cert_verification", False) is not True
