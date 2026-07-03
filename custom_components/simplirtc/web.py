from __future__ import annotations

import asyncio
import logging

from aiohttp import web

from homeassistant.core import HomeAssistant
from homeassistant.components.camera import DATA_COMPONENT
from homeassistant.components.ffmpeg import get_ffmpeg_manager
from homeassistant.components.http import HomeAssistantView

from .const import DOMAIN
from .camera import SimpliSafeGo2rtcCamera, SimpliSafeLiveKitCamera

_LOGGER = logging.getLogger(__name__)


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
	"""Re-mux the SimpliSafe doorbell FLV for go2rtc.

	The doorbell's FLV carries wildly non-monotonic timestamps that freeze
	HomeKit's ffmpeg, and HA's managed go2rtc only accepts simple "ffmpeg:"
	sources via its API (no custom input args), so the fix cannot live in go2rtc.
	Instead this view runs its own ffmpeg to read the authenticated FLV
	out-of-process, rewrite timestamps to a monotonic wallclock, and stream the
	result as clean FLV. go2rtc reads this endpoint with a plain "ffmpeg:" source
	and republishes it as RTSP. ``-reconnect_at_eof`` keeps retrying until the
	woken camera starts publishing. Access is gated by the per-camera token.
	"""

	url = "/api/simplirtc_flv/{entity_id}"
	name = f"api:{DOMAIN}:flv"
	requires_auth = False

	def __init__(self, hass: HomeAssistant) -> None:
		self.hass = hass

	async def get(self, request: web.Request, entity_id: str) -> web.StreamResponse:
		"""Wake the camera and relay a timestamp-corrected FLV via ffmpeg."""

		if not isinstance(
			camera := self.hass.data[DATA_COMPONENT].get_entity(entity_id),
			SimpliSafeGo2rtcCamera,
		):
			return web.Response(status=404, text=f"Entity {entity_id} is not a SimpliRTC FLV camera")

		if request.query.get("sig") != camera.proxy_token:
			return web.Response(status=401, text="Invalid signature")

		if not (token := camera.access_token):
			return web.Response(status=503, text="No SimpliSafe access token available")

		# Nudge the camera to (re)join the streaming room before ffmpeg connects.
		await camera._async_wake_cameras()  # pyright: ignore[reportPrivateUsage]

		try:
			binary = get_ffmpeg_manager(self.hass).binary
		except Exception:  # noqa: BLE001 - fall back to PATH if ffmpeg isn't set up
			binary = "ffmpeg"

		cmd = [
			binary, "-hide_banner", "-loglevel", "error",
			# Keep retrying the source until the woken camera starts publishing.
			"-reconnect", "1", "-reconnect_at_eof", "1",
			"-reconnect_streamed", "1", "-reconnect_delay_max", "5",
			# Rewrite the FLV's non-monotonic timestamps to a monotonic clock.
			"-use_wallclock_as_timestamps", "1",
			"-headers", f"Authorization: Bearer {token}\r\n",
			"-i", camera.flv_url(),
			# Output MPEG-TS: it stores H264 as Annex-B (with in-band SPS/PPS),
			# which go2rtc can copy straight into a valid RTSP stream. FLV keeps
			# H264 as AVCC, which produced unparseable RTSP ("Invalid data").
			"-c", "copy", "-f", "mpegts", "pipe:1",
		]

		proc = await asyncio.create_subprocess_exec(
			*cmd,
			stdout=asyncio.subprocess.PIPE,
			stderr=asyncio.subprocess.DEVNULL,
		)

		response = web.StreamResponse(status=200, headers={"Content-Type": "video/mp2t"})
		await response.prepare(request)
		try:
			assert proc.stdout is not None
			while chunk := await proc.stdout.read(65536):
				await response.write(chunk)
		except (ConnectionResetError, asyncio.CancelledError):
			pass
		finally:
			if proc.returncode is None:
				try:
					proc.kill()
				except ProcessLookupError:
					pass
				await proc.wait()
		return response
