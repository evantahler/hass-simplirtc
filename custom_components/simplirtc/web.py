from __future__ import annotations

import asyncio
import logging

import aiohttp
from aiohttp import web

from homeassistant.core import HomeAssistant
from homeassistant.components.camera import DATA_COMPONENT
from homeassistant.components.http import HomeAssistantView
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .const import DOMAIN
from .camera import SimpliSafeGo2rtcCamera, SimpliSafeLiveKitCamera

_LOGGER = logging.getLogger(__name__)

# Total time to wait for an idle camera to start publishing before giving up.
FLV_START_TIMEOUT = 15.0
# Delay between retries while the camera warms up.
FLV_RETRY_DELAY = 1.0
# How long to wait for the first bytes of a single upstream attempt.
FLV_FIRST_BYTE_TIMEOUT = 8.0


class SimpliRTCStreamInfoView(HomeAssistantView):
	"""View to handle SimpliRTC stream info requests."""

	url = "/api/simplirtc_proxy/{entity_id}"
	name = f"api:{DOMAIN}:simplirtc"
	requires_auth = False

	def __init__(self, hass: HomeAssistant) -> None:
		self.hass = hass

	async def get(self, request: web.Request, entity_id: str) -> web.Response:
		"""Handle GET request for stream info."""

		if not isinstance(camera := self.hass.data[DATA_COMPONENT].get_entity(entity_id), SimpliSafeLiveKitCamera) :
			return web.Response(status=404, text=f"Entity {entity_id} is not a SimpliSafeLiveKitCamera")

		try:
			url, token = await camera._live_view()
			return web.json_response({"url": url, "token": token})
		except Exception as e:
			return web.Response(status=500, text=f"Error fetching stream info: {str(e)}")


class SimpliRTCFlvProxyView(HomeAssistantView):
	"""Proxy the authenticated SimpliSafe FLV stream for go2rtc.

	go2rtc reads this endpoint out-of-process and this view injects the current
	SimpliSafe bearer token, so the raw FLV never touches HA's in-process stream
	worker. An idle doorbell reports "online" but its FLV endpoint returns an
	immediate EOF until it starts publishing, so this view wakes the camera and
	retries the upstream fetch until real frames arrive, then relays one clean,
	flowing stream. Access is gated by the per-camera proxy token.
	"""

	url = "/api/simplirtc_flv/{entity_id}"
	name = f"api:{DOMAIN}:flv"
	requires_auth = False

	def __init__(self, hass: HomeAssistant) -> None:
		self.hass = hass

	async def get(self, request: web.Request, entity_id: str) -> web.StreamResponse:
		"""Wake the camera, wait for frames, then relay the authenticated FLV."""

		if not isinstance(
			camera := self.hass.data[DATA_COMPONENT].get_entity(entity_id),
			SimpliSafeGo2rtcCamera,
		):
			return web.Response(status=404, text=f"Entity {entity_id} is not a SimpliRTC FLV camera")

		if request.query.get("sig") != camera.proxy_token:
			return web.Response(status=401, text="Invalid signature")

		if not (token := camera.access_token):
			return web.Response(status=503, text="No SimpliSafe access token available")

		session = async_get_clientsession(self.hass)
		headers = {"Authorization": f"Bearer {token}"}
		loop = asyncio.get_running_loop()
		deadline = loop.time() + FLV_START_TIMEOUT

		while loop.time() < deadline:
			# Nudge the camera to (re)join the streaming room; debounced so this
			# fires at most once per wake window even across retries.
			await camera._async_wake_cameras()  # pyright: ignore[reportPrivateUsage]

			try:
				upstream = await session.get(camera.flv_url(), headers=headers)
			except aiohttp.ClientError as err:
				_LOGGER.debug("FLV upstream error for %s: %s", entity_id, err)
				await asyncio.sleep(FLV_RETRY_DELAY)
				continue

			if upstream.status != 200:
				upstream.close()
				await asyncio.sleep(FLV_RETRY_DELAY)
				continue

			# Peek the first bytes: an asleep camera closes immediately (empty).
			try:
				first = await asyncio.wait_for(
					upstream.content.readany(), timeout=FLV_FIRST_BYTE_TIMEOUT
				)
			except (TimeoutError, aiohttp.ClientError):
				upstream.close()
				continue

			if not first:
				upstream.close()
				await asyncio.sleep(FLV_RETRY_DELAY)
				continue

			# Real data is flowing — relay this single stream to the caller.
			response = web.StreamResponse(status=200, headers={"Content-Type": "video/x-flv"})
			await response.prepare(request)
			try:
				await response.write(first)
				async for chunk in upstream.content.iter_chunked(65536):
					await response.write(chunk)
			except (ConnectionResetError, aiohttp.ClientError):
				pass
			finally:
				upstream.close()
			return response

		return web.Response(status=504, text="Camera did not start streaming")
