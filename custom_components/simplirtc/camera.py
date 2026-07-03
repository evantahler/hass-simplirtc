"""Component providing support to the Simplisafe camera."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
import logging
import secrets
import time
from typing import Any, TypeVar, override

import aiohttp
from pydantic import TypeAdapter
from pydantic.dataclasses import dataclass
from simplipy.device.camera import Camera, CameraTypes
from simplipy.system.v3 import SystemV3
from simplipy.websocket import (
	EVENT_CAMERA_MOTION_DETECTED,
)
import jwt

from homeassistant.config_entries import ConfigEntry
from homeassistant.components.camera import (
	Camera as CameraEntity,
	CameraEntityFeature,
	CameraEntityDescription,
	WebRTCAnswer,
	WebRTCCandidate,
	WebRTCMessage,
	WebRTCSendMessage,
)
from homeassistant.components.simplisafe import SimpliSafe
from homeassistant.components.simplisafe.entity import SimpliSafeEntity
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from webrtc_models import RTCIceCandidateInit

from .const import (
	ATTR_CONFIG_ENTRY_ID,
)
from .kinesis import KinesisSession
from .livekit import LiveKitProducer
from .sfu import RawRtpSfu
from .snapshot import DEFAULT_SNAPSHOT_TIMEOUT, Snapshotter

_LOGGER = logging.getLogger(__name__)
WEBRTC_URL_BASE = "https://app-hub.prd.aser.simplisafe.com/v2"
WAKEUP_URL_BASE = "https://app-hub.prd.aser.simplisafe.com/v1"
WAKE_DEBOUNCE_SECONDS = 10.0
# How long to let a freshly-woken camera start publishing before consumers connect.
WAKE_SETTLE_SECONDS = 5.0
# go2rtc's managed RTSP listener (see the go2rtc integration's server config).
GO2RTC_RTSP_PORT = 18554
_StreamResponseT = TypeVar("_StreamResponseT")


async def async_setup_entry(
	hass: HomeAssistant,
	entry: ConfigEntry[SimpliSafe],
	async_add_entities: AddEntitiesCallback,
) -> None:
	"""Set up a SimpliSafe Camera."""
	simplisafe = entry.runtime_data

	cameras: list[SimpliSafeCamera] = []

	for system in simplisafe.systems.values():
		if not isinstance(system, SystemV3):
			_LOGGER.warning("Skipping camera setup for V%d system: %s", system.version, system.system_id)
			continue

		for camera in system.cameras.values():
			if not isinstance(settings := camera.camera_settings.get("admin"), Mapping):
				_LOGGER.warning("Skipping camera '%s'. Unexpected settings schema.", camera.name)
				continue

			cls: type[SimpliSafeCamera]
			match settings.get("webRTCProvider"):
				case "mist":
					cls = SimpliSafeLiveKitCamera
				case "kvs":
					cls = SimpliSafeKenisisCamera
				case _ as provider:
					# Cameras without a supported WebRTC backend (e.g. the video
					# doorbell, SS002) stream over the legacy FLV media endpoint,
					# served out-of-process through go2rtc.
					if camera.camera_type in (CameraTypes.CAMERA, CameraTypes.DOORBELL):
						cls = SimpliSafeGo2rtcCamera
					else:
						_LOGGER.warning(
							"Camera '%s' has unsupported backend (provider=%s, type=%s)",
							camera.name, provider, camera.camera_type,
						)
						continue

			cameras.append(cls(simplisafe, system, camera))

	async_add_entities(cameras)


@dataclass(kw_only=True, slots=True)
class KenisisResponse:
	signedChannelEndpoint: str
	clientId: str
	iceServers: list[Any]


@dataclass(kw_only=True, slots=True)
class LiveKitResponse:
	liveKitDetails: LiveKitDetails


@dataclass(kw_only=True, slots=True)
class LiveKitDetails:
	liveKitURL: str
	userToken: str


class SimpliSafeCamera(SimpliSafeEntity, CameraEntity):
	"""An implementation of a Simplisafe camera."""

	def __init__(
		self,
		simplisafe: SimpliSafe,
		system: SystemV3,
		device: Camera,
	) -> None:
		"""Initialize the SimpliSafe camera."""
		super().__init__(
			simplisafe, system, device=device,
			additional_websocket_events=(EVENT_CAMERA_MOTION_DETECTED,)
		)
		self.entity_description = CameraEntityDescription(
			key="live_view",
		)
		CameraEntity.__init__(self)

		self._attr_unique_id = f"{super().unique_id}-camera"
		self._attr_supported_features |= CameraEntityFeature.STREAM
		self._device: Camera
		self._last_wake_monotonic: float = 0.0

	async def _async_wake_cameras(self) -> bool:
		"""Nudge idle cameras to (re)join the streaming room before a stream starts.

		A camera can report "online" yet not be publishing to the room; this
		POST signals it to join. Failures are non-fatal. Debounced so rapid
		stream starts don't spam the endpoint. Returns True if a wake was
		actually issued (the caller may then allow the camera a moment to start
		publishing), or False if the call was debounced.
		"""
		now = time.monotonic()
		if now - self._last_wake_monotonic < WAKE_DEBOUNCE_SECONDS:
			return False
		self._last_wake_monotonic = now
		path = f"ss3/subscriptions/{self._system.system_id}/camera-wakeup"
		try:
			await self._simplisafe._api.async_request(  # pyright: ignore[reportPrivateUsage]
				"post", path, url_base=WAKEUP_URL_BASE, json={"wakeAll": True}
			)
		except Exception as err:
			_LOGGER.debug("Failed to wake cameras for %s: %s", self.entity_id, err)
		return True

	async def _create_stream(self, response_type: type[_StreamResponseT]) -> _StreamResponseT:
		path = f"cameras/{self._device.serial}/{self._system.system_id}/live-view"
		return TypeAdapter(response_type).validate_python(
			await self._simplisafe._api.async_request("get", path, url_base=WEBRTC_URL_BASE)  # pyright: ignore[reportPrivateUsage]
		)

	@override
	async def async_camera_image(
		self,
		width: int | None = None,
		height: int | None = None,
	) -> bytes | None:
		"""Return a camera image from a temporary WebRTC session."""
		_ = width, height
		snapshotter = Snapshotter()
		try:
			offer_sdp = await snapshotter.make_offer()

			def send_message(message: WebRTCMessage) -> None:
				match message:
					case WebRTCAnswer(answer=answer):
						snapshotter.send_answer(answer)
					case WebRTCCandidate(candidate=RTCIceCandidateInit(
						candidate=candidate,
						sdp_mid=sdp_mid,
						sdp_m_line_index=sdp_m_line_index,
					)):
						snapshotter.send_candidate(
							candidate,
							sdp_mid=sdp_mid,
							sdp_m_line_index=sdp_m_line_index,
						)
					case _:
						_LOGGER.debug("Dropping unsupported snapshot WebRTC message type=%s", type(message).__name__)

			async with asyncio.timeout(DEFAULT_SNAPSHOT_TIMEOUT):
				await self.async_handle_async_webrtc_offer(
					offer_sdp,
					snapshotter.session_id,
					send_message,
				)
				return await snapshotter.wait_for_image()
		except TimeoutError:
			_LOGGER.debug("Timed out waiting for WebRTC camera image")
			return None
		finally:
			self.close_webrtc_session(snapshotter.session_id)
			await snapshotter.close()


class SimpliSafeGo2rtcCamera(SimpliSafeCamera):
	"""A SimpliSafe camera that streams the legacy FLV media endpoint via go2rtc.

	Cameras without a supported WebRTC backend (for example the video doorbell)
	stream over SimpliSafe's FLV media endpoint. Home Assistant's built-in
	stream worker decodes media in-process and libav crashes hard on this FLV,
	so the live view is served through the bundled go2rtc instead:
	``stream_source`` returns an ``ffmpeg:`` source pointing at a local proxy
	view, which go2rtc reads out-of-process. HA's in-process libav can never
	open the ``ffmpeg:`` scheme, so it never demuxes the FLV and cannot crash on
	it. Snapshots use the MJPEG endpoint over plain HTTP.
	"""

	def __init__(
		self,
		simplisafe: SimpliSafe,
		system: SystemV3,
		device: Camera,
	) -> None:
		"""Initialize the go2rtc-backed SimpliSafe camera."""
		super().__init__(simplisafe, system, device)
		# Shared secret so only go2rtc (which is handed the generated
		# stream_source) can pull the authenticated FLV through the proxy view.
		self._proxy_token = secrets.token_urlsafe(24)

	@property
	def proxy_token(self) -> str:
		"""Return the secret guarding this camera's FLV proxy URL."""
		return self._proxy_token

	@property
	def access_token(self) -> str | None:
		"""Return the current SimpliSafe bearer token, if available."""
		return self._simplisafe._api.access_token  # pyright: ignore[reportPrivateUsage]

	def flv_url(self, width: int = 1280) -> str:
		"""Return the authenticated SimpliSafe FLV media URL."""
		return self._device.video_url(width=width)

	@override
	async def stream_source(self) -> str | None:
		"""Return a go2rtc ffmpeg source pointing at the local FLV proxy.

		The ``ffmpeg:`` scheme routes this to go2rtc (out-of-process). HA's
		in-process stream worker cannot open the ``ffmpeg:`` scheme, so it can
		never demux the FLV and cannot segfault on it.
		"""
		if not self.access_token:
			return None
		# Pre-warm: wake the camera and let it start publishing *before* any
		# consumer connects, so the go2rtc RTSP stream is ready when HomeKit / HLS
		# probe it (RTSP consumers, unlike WebRTC, don't tolerate a cold start).
		if await self._async_wake_cameras():
			await asyncio.sleep(WAKE_SETTLE_SECONDS)
		port = self.hass.http.server_port
		proxy_url = (
			f"http://127.0.0.1:{port}/api/simplirtc_flv/{self.entity_id}"
			f"?sig={self._proxy_token}"
		)
		# Publish the FLV through go2rtc as plain RTSP so HomeKit, HLS and WebRTC
		# all consume it with no per-camera config. go2rtc decodes the FLV
		# out-of-process (HA's in-process libav only ever sees clean H264 RTSP).
		#
		# Use an explicit ffmpeg exec command: the SimpliSafe FLV carries wildly
		# non-monotonic timestamps, which freeze HomeKit's ffmpeg (it clamps the
		# DTS). ``-use_wallclock_as_timestamps 1`` on the input rewrites them to a
		# monotonic clock at ingestion, so every downstream consumer gets a clean
		# stream. ``{output}`` is substituted by go2rtc with its RTSP ingest URL.
		go2rtc_source = (
			"exec:ffmpeg -hide_banner -loglevel error "
			"-fflags nobuffer -flags low_delay -use_wallclock_as_timestamps 1 "
			f"-i {proxy_url} "
			"-c:v copy -c:a copy -rtsp_transport tcp -f rtsp {output}"
		)
		# Return the go2rtc RTSP URL, or None if go2rtc can't be reached. Never
		# fall back to the raw go2rtc source string: ffmpeg-based consumers like
		# the HomeKit bridge would try to open it as a file and fail.
		return await self._async_ensure_go2rtc_rtsp(go2rtc_source)

	async def _async_ensure_go2rtc_rtsp(self, source: str) -> str | None:
		"""Publish the FLV source through go2rtc and return its RTSP URL.

		Registers a go2rtc stream fed by the out-of-process ``ffmpeg:`` source
		and returns its managed RTSP address. Returns None if the bundled go2rtc
		client cannot be reached, so the caller can fall back to handing the
		``ffmpeg:`` source straight to go2rtc's WebRTC provider.
		"""
		try:
			from homeassistant.components.go2rtc.const import DOMAIN as GO2RTC_DOMAIN

			entries = self.hass.config_entries.async_entries(GO2RTC_DOMAIN)
			if not entries:
				return None
			client = entries[0].runtime_data._rest_client  # pyright: ignore[reportAttributeAccessIssue]
			name = f"simplirtc_{self._device.serial}"
			await client.streams.add(name, [source])
			return f"rtsp://127.0.0.1:{GO2RTC_RTSP_PORT}/{name}"
		except Exception as err:
			_LOGGER.debug(
				"go2rtc RTSP publish failed for %s; using direct go2rtc source: %s",
				self.entity_id, err,
			)
			return None

	@override
	async def async_camera_image(
		self,
		width: int | None = None,
		height: int | None = None,
	) -> bytes | None:
		"""Return a still image from the MJPEG media endpoint over plain HTTP."""
		_ = height
		if not (token := self.access_token):
			return None

		snapshot_width = width or 1280
		# Derive the media base from video_url() so we reuse whichever device
		# identifier SimpliSafe expects in the media path.
		media_base = self.flv_url(width=snapshot_width).split("/flv?", 1)[0]
		url = f"{media_base}/mjpg?x={snapshot_width}&fr=1"

		session = async_get_clientsession(self.hass)
		try:
			async with session.get(
				url, headers={"Authorization": f"Bearer {token}"}
			) as response:
				if response.status != 200:
					_LOGGER.debug(
						"Snapshot request for %s returned HTTP %s",
						self.entity_id, response.status,
					)
					return None
				return await response.read()
		except (aiohttp.ClientError, TimeoutError) as err:
			_LOGGER.debug("Snapshot request for %s failed: %s", self.entity_id, err)
			return None


class SimpliSafeLiveKitCamera(SimpliSafeCamera):
	"""An implementation of a Simplisafe camera."""

	def __init__(self, simplisafe: SimpliSafe, system: SystemV3, device: Camera) -> None:
		super().__init__(simplisafe, system, device)
		self._livekit_url: str = ""
		self._livekit_token: str = ""
		self._cache_expiration: float = 0
		self._lock = asyncio.Lock()
		self._livekit_producer = LiveKitProducer(get_connection_info=self._live_view)
		self._sfu = RawRtpSfu(
			setup_producer_pc=self._livekit_producer.setup,
		)

	@override
	async def async_handle_async_webrtc_offer(
		self, offer_sdp: str, session_id: str, send_message: WebRTCSendMessage
	) -> None:
		"""Handle a WebRTC offer with an immediately available SFU session."""
		answer_sdp = await self._sfu.create_session(
			offer_sdp,
			peer_id=session_id,
		)
		send_message(WebRTCAnswer(answer=answer_sdp))

	@override
	async def async_on_webrtc_candidate(
		self, session_id: str, candidate: RTCIceCandidateInit
	) -> None:
		"""Handle a WebRTC candidate for an SFU consumer."""
		await self._sfu.add_candidate(
			session_id,
			candidate.candidate,
			sdp_mid=candidate.sdp_mid,
			sdp_m_line_index=candidate.sdp_m_line_index,
		)

	@override
	@callback
	def close_webrtc_session(self, session_id: str) -> None:
		"""Close an SFU consumer WebRTC session."""
		self._sfu.close_session(session_id)

	async def _live_view(self) -> tuple[str, str]:
		if time.time() < self._cache_expiration:
			return self._livekit_url, self._livekit_token

		async with self._lock:
			if time.time() < self._cache_expiration:
				return self._livekit_url, self._livekit_token

			live_view = await self._create_stream(LiveKitResponse)
			self._livekit_url = live_view.liveKitDetails.liveKitURL
			self._livekit_token = live_view.liveKitDetails.userToken
			try:
				decoded_token = jwt.decode(self._livekit_token, options={"verify_signature": False})
				self._cache_expiration = decoded_token["exp"]
			except Exception as err:
				_LOGGER.warning("Failed to decode JWT token for caching: %s", err)
				self._cache_expiration = 0

		return self._livekit_url, self._livekit_token


class SimpliSafeKenisisCamera(SimpliSafeCamera):
	"""An implementation of a Simplisafe camera."""

	def __init__(
		self,
		simplisafe: SimpliSafe,
		system: SystemV3,
		device: Camera,
	) -> None:
		"""Initialize the SimpliSafe camera."""
		super().__init__(simplisafe, system, device)
		self._sessions: dict[str, asyncio.Task[KinesisSession]] = {}

	@override
	async def async_handle_async_webrtc_offer(
		self, offer_sdp: str, session_id: str, send_message: WebRTCSendMessage
	) -> None:
		"""Handle a Kinesis WebRTC offer."""

		self._sessions[session_id] = session_future = self.hass.async_create_task(
			self._create_webrtc_session(session_id, send_message)
		)
		try:
			session = await session_future
			if self._sessions.get(session_id) is session_future:
				session.start(offer_sdp)
		except Exception:
			self._sessions.pop(session_id, None)
			raise

	@override
	async def async_on_webrtc_candidate(
		self, session_id: str, candidate: RTCIceCandidateInit
	) -> None:
		"""Handle a Kinesis WebRTC candidate."""

		if not (session_future := self._sessions.get(session_id)):
			_LOGGER.debug("Ignoring WebRTC candidate for closed session %s", session_id)
			return
		try:
			session = await session_future
		except Exception as err:
			_LOGGER.debug("Ignoring WebRTC candidate for failed session %s: %s", session_id, err)
			return
		await session.send_candidate(
			candidate.candidate,
			sdp_mid=candidate.sdp_mid,
			sdp_m_line_index=candidate.sdp_m_line_index,
		)

	@override
	@callback
	def close_webrtc_session(self, session_id: str) -> None:
		"""Close a Kinesis WebRTC session."""

		if not (session_future := self._sessions.pop(session_id, None)):
			return

		async def close_session() -> None:
			if not session_future.done():
				session_future.cancel()
			try:
				session = await session_future
			except asyncio.CancelledError:
				return
			except Exception as err:
				_LOGGER.debug(
					"WebRTC session %s ended before startup completed: %s",
					session_id,
					err,
				)
				return
			session.close()

		self.hass.async_create_task(close_session())

	async def _create_webrtc_session(
		self, session_id: str, send_message: WebRTCSendMessage
	) -> KinesisSession:
		live_view = await self._create_stream(KenisisResponse)

		def send_candidate(
			candidate: str,
			sdp_mid: str | None,
			sdp_m_line_index: int | None,
		) -> None:
			send_message(WebRTCCandidate(candidate=RTCIceCandidateInit(
				candidate=candidate,
				sdp_mid=sdp_mid,
				sdp_m_line_index=sdp_m_line_index,
			)))

		return KinesisSession(
			session_id=session_id,
			channel_endpoint=live_view.signedChannelEndpoint,
			client_id=live_view.clientId,
			send_answer=lambda answer_sdp: send_message(WebRTCAnswer(answer=answer_sdp)),
			send_candidate=send_candidate,
		)
