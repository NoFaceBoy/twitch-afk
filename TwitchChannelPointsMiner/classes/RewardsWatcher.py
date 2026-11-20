# -*- coding: utf-8 -*-
"""
Watch Streak recovery and Weekly Rewards progression for offline streamers.

Ported from mpforce1's fork (WatchStreakRecovery / WeeklyRewardsProgressor):
- Twitch lets you recover a missed Watch Streak by watching a Clip or the VOD of the
  missed broadcast (RewardList -> watchStreakMilestone.missedStreams).
- Weekly Rewards (WeeklyVisitRewardsQuery) count a "visit" per day, watching a Clip or
  a VOD of an offline channel is enough.

Every INFO_REFRESH seconds the state of every streamer is refreshed. Offline streamers that
need it get a Clip watched (up to CLIP_WATCH_SECONDS), then a VOD (up to VOD_WATCH_SECONDS)
if the Clip wasn't enough. A streamer that couldn't be progressed is put on cooldown.
"""

import copy
import json
import logging
import random
import threading
import time
from base64 import b64encode
from concurrent.futures import ThreadPoolExecutor
from secrets import token_hex

import requests

from TwitchChannelPointsMiner.constants import GQLOperations

logger = logging.getLogger(__name__)

INFO_REFRESH = 60 * 60  # seconds
INTERVAL = 30  # seconds
STARTUP_DELAY = 60  # seconds
MAX_CONCURRENT = 2
FAILURE_COOLDOWN = 60 * 60  # seconds
CLIP_WATCH_SECONDS = 30
VOD_WATCH_SECONDS = 8 * 60
VOD_CHECK_INTERVAL = 2 * 60  # seconds
MIN_CLIP_SECONDS = 5
MIN_VOD_SECONDS = 6 * 60


def encode_spade_payload(payload):
    json_event = json.dumps(payload, separators=(",", ":"))
    return {"data": (b64encode(json_event.encode("utf-8"))).decode("utf-8")}


def dig(value, *path):
    for key in path:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value


def missed_streams_from_reward_list(response):
    """Broadcast ids of the streams missed by the current Watch Streak, None if unknown."""
    if not isinstance(response, dict) or not isinstance(response.get("data"), dict):
        return None
    channel_self = dig(response, "data", "channel", "self")
    if channel_self is None:
        return None
    milestone = channel_self.get("watchStreakMilestone")
    if milestone is None:
        return set()
    ids = set()
    for stream in milestone.get("missedStreams") or []:
        for identifier in (stream or {}).get("broadcastIdentifiers") or []:
            if isinstance(identifier, dict) and identifier.get("id"):
                ids.add(identifier["id"])
    return ids


def streak_from_reward_list(response):
    """Length of the current Watch Streak, None if unknown."""
    value = dig(response, "data", "channel", "self", "watchStreakMilestone", "watchStreakMilestone", "value")
    try:
        return int(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def weekly_rewards_from_response(response):
    """The weeklyVisitRewards object, None when there's no (more) active event."""
    if not isinstance(response, dict):
        return None
    # Twitch errors out in the last week of an event once all the rewards are earned
    if response.get("errors"):
        return None
    weekly = dig(response, "data", "channel", "self", "weeklyVisitRewards")
    return weekly if isinstance(weekly, dict) else None


def needs_watch_streak_recovery(streamer):
    return (
        streamer.settings.watch_streak is True
        and streamer.is_online is False
        and len(streamer.watch_streak_missed_streams) > 0
    )


def missing_weekly_reward(streamer):
    weekly = streamer.weekly_rewards
    if streamer.settings.weekly_rewards is not True or weekly is None:
        return False
    reward_tiers = dig(weekly, "eventConfig", "rewardTiers") or []
    return (
        (weekly.get("accumulatedWeeks") or 0) < len(reward_tiers)
        and not weekly.get("hasEarnedWeeklyRewardThisWeek")
        and not weekly.get("hasVisitedToday")
    )


class RewardsWatcher(threading.Thread):
    def __init__(self, twitch, streamers):
        super().__init__(name="Watch streak/Weekly rewards", daemon=True)
        self.twitch = twitch
        self.streamers = streamers
        self.cooldowns = {}
        self.in_progress = set()
        self.lock = threading.Lock()
        self.executor = ThreadPoolExecutor(
            max_workers=MAX_CONCURRENT, thread_name_prefix="Clip/VOD watcher"
        )

    # === State === #
    def refresh_watch_streak(self, streamer):
        json_data = copy.deepcopy(GQLOperations.RewardList)
        json_data["variables"]["channelID"] = streamer.channel_id
        response = self.twitch.post_gql_request(json_data)
        missed = missed_streams_from_reward_list(response)
        if missed is None:
            return
        if streamer.streak_store is not None:
            streamer.streak_store.streak_status(
                streamer.username, streak_from_reward_list(response), missed
            )
        had_missed = len(streamer.watch_streak_missed_streams) > 0
        streamer.watch_streak_missed_streams = missed
        if had_missed and not missed:
            logger.info(
                f"Watch Streak recovered for {streamer}",
                extra={"emoji": ":ambulance:"},
            )
        elif not had_missed and missed:
            logger.info(
                f"Missed {len(missed)} stream(s) of the Watch Streak for {streamer}, trying to recover it",
                extra={"emoji": ":red_question_mark:"},
            )

    def refresh_weekly_rewards(self, streamer):
        json_data = copy.deepcopy(GQLOperations.WeeklyVisitRewardsQuery)
        json_data["variables"] = {"channelID": streamer.channel_id}
        streamer.weekly_rewards = weekly_rewards_from_response(
            self.twitch.post_gql_request(json_data)
        )

    def refresh(self, streamer):
        try:
            if streamer.settings.watch_streak is True:
                self.refresh_watch_streak(streamer)
            if streamer.settings.weekly_rewards is True:
                self.refresh_weekly_rewards(streamer)
        except Exception:
            logger.debug(f"Unable to refresh the rewards of {streamer}", exc_info=True)

    # === Clips / VODs === #
    def get_clips(self, streamer, _filter, limit):
        json_data = copy.deepcopy(GQLOperations.ClipsCards__User)
        json_data["variables"]["login"] = streamer.username
        json_data["variables"]["limit"] = limit
        json_data["variables"]["criteria"]["filter"] = _filter
        edges = dig(self.twitch.post_gql_request(json_data), "data", "user", "clips", "edges") or []
        return [e["node"] for e in edges if isinstance(e, dict) and isinstance(e.get("node"), dict)]

    def get_vods(self, streamer, limit=5):
        json_data = copy.deepcopy(GQLOperations.FilterableVideoTower_Videos)
        json_data["variables"]["channelOwnerLogin"] = streamer.username
        json_data["variables"]["limit"] = limit
        edges = dig(self.twitch.post_gql_request(json_data), "data", "user", "videos", "edges") or []
        return [e["node"] for e in edges if isinstance(e, dict) and isinstance(e.get("node"), dict)]

    def vod_viewable(self, streamer, vod):
        """VODs can be subscriber-only, check that the playlist can be accessed."""
        json_data = copy.deepcopy(GQLOperations.PlaybackAccessToken)
        json_data["variables"] = {
            "login": streamer.username,
            "isLive": False,
            "isVod": True,
            "vodID": vod["id"],
            "playerType": "site",
            "platform": "web",
        }
        token = dig(self.twitch.post_gql_request(json_data), "data", "videoPlaybackAccessToken") or {}
        if not token.get("signature") or not token.get("value"):
            return False
        try:
            response = self.twitch.request(
                "GET",
                f"https://usher.ttvnw.net/vod/v2/{vod['id']}.m3u8",
                params={"sig": token["signature"], "token": token["value"]},
                headers={"User-Agent": self.twitch.user_agent},
            )
            return response.status_code == 200
        except requests.exceptions.RequestException:
            return False

    @staticmethod
    def broadcast_id(item):
        return dig(item, "broadcastIdentifier", "id")

    def select_clip(self, streamer, recovery):
        if recovery:
            # Only a Clip of a missed broadcast recovers the streak
            missed = streamer.watch_streak_missed_streams
            for _filter, limit in (("LAST_DAY", 30), ("LAST_WEEK", 10)):
                for clip in self.get_clips(streamer, _filter, limit):
                    if self.broadcast_id(clip) in missed:
                        return clip
            return None
        for clip in self.get_clips(streamer, "ALL_TIME", 10):
            if (clip.get("durationSeconds") or 0) > MIN_CLIP_SECONDS:
                return clip
        return None

    def select_vod(self, streamer, recovery):
        for vod in self.get_vods(streamer):
            if recovery:
                if self.broadcast_id(vod) not in streamer.watch_streak_missed_streams:
                    continue
            elif (vod.get("lengthSeconds") or 0) < MIN_VOD_SECONDS:
                continue
            if self.vod_viewable(streamer, vod):
                return vod
        return None

    def send_spade(self, streamer, payload):
        self.twitch.get_spade_url(streamer)
        if streamer.stream.spade_url is None:
            return False
        try:
            response = self.twitch.request(
                "POST",
                streamer.stream.spade_url,
                data=encode_spade_payload(payload),
                headers={"User-Agent": self.twitch.user_agent},
            )
            return response.status_code == 204
        except requests.exceptions.RequestException as e:
            logger.debug(f"Unable to send spade event for {streamer}: {e}")
            return False

    def watch_clip(self, streamer, clip):
        logger.info(
            f"Watching a Clip of {streamer} for up to {CLIP_WATCH_SECONDS} seconds",
            extra={"emoji": ":paperclip:"},
        )
        user_id = self.twitch.twitch_login.get_user_id()
        play_session_id = token_hex(16)
        common = {
            "location": "vod",
            "url": clip.get("url"),
            "channel_id": streamer.channel_id,
            "vod_type": "clip",
            "vod_id": clip["id"],
            "live": False,
            "minutes_logged": 0,
            "play_session_id": play_session_id,
            "player": "site",
            "user_id": user_id,
            "clip_slug": clip.get("slug"),
        }
        self.send_spade(
            streamer,
            [{"event": "video-play", "properties": {**common, "content_mode": "clip", "vod_timestamp": 0}}],
        )
        watch_seconds = min(CLIP_WATCH_SECONDS, clip.get("durationSeconds") or CLIP_WATCH_SECONDS)
        seconds_watched = 0
        while self.twitch.running and seconds_watched < watch_seconds:
            time.sleep(5)
            seconds_watched += 5
            self.send_spade(
                streamer,
                [
                    {
                        "event": "n_second_play",
                        "properties": {
                            **common,
                            "platform": "web",
                            "seconds_after_play": seconds_watched,
                            "vod_timestamp": seconds_watched - 0.1,
                        },
                    }
                ],
            )

    def watch_vod(self, streamer, vod, done):
        logger.info(
            f"Watching a VOD of {streamer} for up to {VOD_WATCH_SECONDS // 60} minutes",
            extra={"emoji": ":clapper_board:"},
        )
        user_id = self.twitch.twitch_login.get_user_id()
        start = time.time()
        last_check = start
        while self.twitch.running and time.time() - start < VOD_WATCH_SECONDS:
            self.send_spade(
                streamer,
                [
                    {
                        "event": "minute-watched",
                        "properties": {
                            "channel_id": streamer.channel_id,
                            "broadcast_id": None,
                            "player": "site",
                            "user_id": user_id,
                            "live": False,
                            "channel": streamer.username,
                            "vod_id": vod["id"],
                            "content_mode": "vod",
                        },
                    }
                ],
            )
            time.sleep(60)
            if time.time() - last_check >= VOD_CHECK_INTERVAL:
                last_check = time.time()
                if done():
                    return True
        return done()

    # === Tasks === #
    def progress(self, streamer):
        recovery = needs_watch_streak_recovery(streamer)
        what = "recover the Watch Streak" if recovery else "progress the Weekly Rewards"

        def done():
            if recovery:
                self.refresh_watch_streak(streamer)
                return not streamer.watch_streak_missed_streams
            self.refresh_weekly_rewards(streamer)
            return not missing_weekly_reward(streamer)

        try:
            clip = self.select_clip(streamer, recovery)
            if clip is not None:
                self.watch_clip(streamer, clip)
                if done():
                    self.on_success(streamer, recovery, "Clip")
                    return
            if not self.twitch.running:
                return
            vod = self.select_vod(streamer, recovery)
            if vod is not None:
                if self.watch_vod(streamer, vod, done):
                    self.on_success(streamer, recovery, "VOD")
                    return
            if not self.twitch.running:
                return
            reason = "no suitable Clip or VOD" if clip is None and vod is None else "watching didn't help"
            logger.info(
                f"Unable to {what} for {streamer}: {reason}. Retrying in {FAILURE_COOLDOWN // 60} minutes",
                extra={"emoji": ":hourglass:"},
            )
            self.cooldowns[streamer.channel_id] = time.time() + FAILURE_COOLDOWN
        except Exception:
            logger.error(f"Error while trying to {what} for {streamer}", exc_info=True)
            self.cooldowns[streamer.channel_id] = time.time() + FAILURE_COOLDOWN
        finally:
            with self.lock:
                self.in_progress.discard(streamer.channel_id)

    def on_success(self, streamer, recovery, via):
        if recovery:
            # refresh_watch_streak has already logged the recovery
            logger.debug(f"Watch Streak of {streamer} recovered via {via}")
        else:
            weekly = streamer.weekly_rewards or {}
            required = dig(weekly, "eventConfig", "daysRequiredPerWeek")
            logger.info(
                f"Weekly Rewards visit counted for {streamer} via {via}"
                + (
                    f" ({weekly.get('daysVisitedThisWeek')}/{required} days this week)"
                    if required
                    else ""
                ),
                extra={"emoji": ":calendar:"},
            )

    def candidates(self):
        now = time.time()
        for streamer in self.streamers:
            if streamer.is_online is True:
                continue
            if self.cooldowns.get(streamer.channel_id, 0) > now:
                continue
            if streamer.channel_id in self.in_progress:
                continue
            if needs_watch_streak_recovery(streamer) or missing_weekly_reward(streamer):
                yield streamer

    def run(self):
        enabled = [
            s
            for s in self.streamers
            if s.settings.watch_streak is True or s.settings.weekly_rewards is True
        ]
        if not enabled:
            return
        time.sleep(STARTUP_DELAY)
        last_refresh = 0
        while self.twitch.running:
            try:
                if time.time() - last_refresh >= INFO_REFRESH:
                    last_refresh = time.time()
                    for streamer in self.streamers:
                        if not self.twitch.running:
                            return
                        if streamer.channel_id not in self.in_progress:
                            self.refresh(streamer)
                        time.sleep(random.uniform(0.2, 1))

                for streamer in self.candidates():
                    with self.lock:
                        if len(self.in_progress) >= MAX_CONCURRENT:
                            break
                        self.in_progress.add(streamer.channel_id)
                    self.executor.submit(self.progress, streamer)
            except Exception:
                logger.error("Error in the Watch Streak/Weekly Rewards watcher", exc_info=True)
            time.sleep(INTERVAL)
        self.executor.shutdown(wait=False)
