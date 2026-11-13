# -*- coding: utf-8 -*-
"""
Drop claiming that recognises claims which never stick.

Twitch answers DropsPage_ClaimDropRewards with status ELIGIBLE_FOR_ALL even when the
reward can't be delivered because the game account isn't linked
(isUserAccountConnected: false). The drop then comes back unclaimed on the next
inventory sync and used to be claimed again every 30 minutes, forever (#844, #845).

A claim only counts as a success for DROP_INSTANCE_ALREADY_CLAIMED, or ELIGIBLE_FOR_ALL
with a connected account. After MAX_CLAIM_ATTEMPTS attempts that did not stick, the
drop is put on a cooldown, so linking the game account later is picked up without
restarting the miner.
"""

import logging
import time

from TwitchChannelPointsMiner.classes.Settings import Events

logger = logging.getLogger(__name__)

MAX_CLAIM_ATTEMPTS = 3
CLAIM_COOLDOWN = 6 * 60 * 60  # seconds

STATUS_ALREADY_CLAIMED = "DROP_INSTANCE_ALREADY_CLAIMED"
STATUS_ELIGIBLE_FOR_ALL = "ELIGIBLE_FOR_ALL"

RESULT_CLAIMED = "claimed"
RESULT_NEEDS_ACCOUNT = "needs_account"
RESULT_FAILED = "failed"
# No usable answer (network error, empty response), not counted as an attempt
RESULT_NO_ANSWER = "no_answer"


def parse_claim_response(response):
    """
    Classifies a DropsPage_ClaimDropRewards response.
    :return: a (result, status) tuple, status is Twitch's claim status or None.
    """
    if not isinstance(response, dict) or not isinstance(response.get("data"), dict):
        return RESULT_NO_ANSWER, None

    data = response["data"]
    if data.get("errors") or response.get("errors"):
        return RESULT_FAILED, None

    payload = data.get("claimDropRewards")
    if not isinstance(payload, dict):
        return RESULT_FAILED, None

    status = payload.get("status")
    if status == STATUS_ALREADY_CLAIMED:
        return RESULT_CLAIMED, status
    if payload.get("isUserAccountConnected") is False:
        return RESULT_NEEDS_ACCOUNT, status
    if status == STATUS_ELIGIBLE_FOR_ALL:
        return RESULT_CLAIMED, status
    return RESULT_FAILED, status


class DropClaims(object):
    """Counts claim attempts per drop and puts drops that never stick on a cooldown."""

    __slots__ = ["attempts", "last_result", "cooldown_until"]

    def __init__(self):
        self.attempts = {}
        self.last_result = {}
        self.cooldown_until = {}

    def can_claim(self, drop):
        until = self.cooldown_until.get(drop.id)
        if until is None:
            return True
        if time.time() >= until:
            # Cooldown over, give it another chance (maybe the account was linked)
            del self.cooldown_until[drop.id]
            self.attempts.pop(drop.id, None)
            return True
        return False

    def claim(self, drop, post_gql_request, claim_request):
        """
        Claims the drop.
        :param post_gql_request: function that sends a GQL request.
        :param claim_request: the GQL request for the claim.
        :return: True only if the drop was really claimed.
        """
        if not self.can_claim(drop):
            logger.debug(f"Skip claim for {drop.id}: on cooldown")
            return False

        result, status = parse_claim_response(post_gql_request(claim_request))
        if result == RESULT_NO_ANSWER:
            logger.debug(f"No answer to the claim for {drop.id}, retry later")
            return False

        attempts = self.attempts.get(drop.id, 0) + 1
        self.attempts[drop.id] = attempts
        self.last_result[drop.id] = result

        if result == RESULT_CLAIMED and attempts <= MAX_CLAIM_ATTEMPTS:
            logger.info(
                f"Claim {drop}",
                extra={"emoji": ":package:", "event": Events.DROP_CLAIM},
            )
            return True

        if result == RESULT_NEEDS_ACCOUNT:
            reason = "the game account is not linked to Twitch"
        elif result == RESULT_CLAIMED:
            # Twitch reported success several times but the drop is still claimable
            reason = "the claim did not stick"
        else:
            reason = f"status {status}"
        logger.debug(
            f"Claim for {drop.id} failed ({reason}), attempt {attempts}/{MAX_CLAIM_ATTEMPTS}"
        )

        if attempts >= MAX_CLAIM_ATTEMPTS or result == RESULT_NEEDS_ACCOUNT:
            self.cooldown_until[drop.id] = time.time() + CLAIM_COOLDOWN
            logger.warning(
                f"Unable to claim drop {drop.name} ({drop.benefit}): {reason}. "
                f"Retrying in {CLAIM_COOLDOWN // 3600} hours.",
                extra={"emoji": ":package:"},
            )
        return False
