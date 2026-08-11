import asyncio
import dataclasses
import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from tempfile import mkdtemp
from typing import Optional

import nodriver

logger = logging.getLogger('extractor')


@dataclass
class TokenInfo:
    updated: int
    potoken: str
    visitor_data: str

    def to_json(self) -> str:
        as_dict = dataclasses.asdict(self)
        as_json = json.dumps(as_dict)
        return as_json


class PotokenExtractor:
    def __init__(self, loop: asyncio.AbstractEventLoop,
                 update_interval: float = 3600,
                 browser_path: Optional[Path] = None) -> None:
        self.update_interval: float = update_interval
        self.browser_path: Optional[Path] = browser_path
        self.profile_path = mkdtemp()  # cleaned up on exit by nodriver
        self._loop = loop
        self._token_info: Optional[TokenInfo] = None
        self._ongoing_update: asyncio.Lock = asyncio.Lock()
        self._extraction_done: asyncio.Event = asyncio.Event()
        self._update_requested: asyncio.Event = asyncio.Event()

    def get(self) -> Optional[TokenInfo]:
        return self._token_info

    async def run_once(self) -> Optional[TokenInfo]:
        await self._update()
        return self.get()

    async def run(self) -> None:
        await self._update()
        while True:
            try:
                await asyncio.wait_for(self._update_requested.wait(), timeout=self.update_interval)
                logger.debug('initiating force update')
            except asyncio.TimeoutError:
                logger.debug('initiating scheduled update')
            await self._update()
            self._update_requested.clear()

    def request_update(self) -> bool:
        """Request immediate update, return False if update request is already set"""
        if self._ongoing_update.locked():
            logger.debug('update process is already running')
            return False
        if self._update_requested.is_set():
            logger.debug('force update has already been requested')
            return False

        self._loop.call_soon_threadsafe(self._update_requested.set)
        logger.debug('force update requested')
        return True

    @staticmethod
    def _extract_token(request: nodriver.cdp.network.Request) -> Optional[TokenInfo]:
        post_data = request.post_data
        try:
            post_data_json = json.loads(post_data)
            visitor_data = post_data_json['context']['client']['visitorData']
            potoken = post_data_json['serviceIntegrityDimensions']['poToken']
        except (json.JSONDecodeError, TypeError, KeyError) as e:
            logger.warning(f'failed to extract token from request: {type(e)}, {e}')
            return None

        token_info = TokenInfo(
            updated=int(time.time()),
            potoken=potoken,
            visitor_data=visitor_data
        )
        return token_info

    async def _update(self) -> None:
        try:
            await asyncio.wait_for(self._perform_update(), timeout=600)
        except asyncio.TimeoutError:
            logger.error('update failed: hard limit timeout exceeded. Browser might be failing to start properly')

    async def _perform_update(self) -> None:
        if self._ongoing_update.locked():
            logger.debug('update is already in progress')
            return

        async with self._ongoing_update:
            logger.info('update started')
            self._extraction_done.clear()

            try:
                browser = await nodriver.start(headless=False,
                                               no_sandbox=True,
                                               browser_executable_path=self.browser_path,
                                               user_data_dir=self.profile_path)
            except FileNotFoundError as e:
                msg = "could not find Chromium. Make sure it's installed or provide direct path to the executable"
                raise FileNotFoundError(msg) from e

            tab = browser.main_tab
            tab.add_handler(nodriver.cdp.network.RequestWillBeSent, self._send_handler)

            await tab.get('https://www.youtube.com/embed/jNQXAC9IVRw')
            player_clicked = await self._click_on_player(tab)

            success = False
            if player_clicked:
                success = await self._wait_for_handler()

            # DEBUG: kalau gagal (timeout / player gak ketemu), intip isi
            # halaman via JS eval -- ini kasih tahu APAKAH ada consent wall,
            # bot-check wall, atau player memang gak pernah mulai muter.
            # Log ini yang perlu di-screenshot/paste balik buat konfirmasi
            # penyebab pastinya sebelum nulis fix lanjutan.
            if not success:
                await self._log_page_diagnostic(tab)

            await tab.close()
            browser.stop()

    @staticmethod
    async def _log_page_diagnostic(tab: nodriver.Tab) -> None:
        try:
            diagnostic = await tab.evaluate("""
                (() => {
                    const bodyText = (document.body && document.body.innerText) || '';
                    const player = document.querySelector('#movie_player');
                    return JSON.stringify({
                        title: document.title,
                        url: location.href,
                        hasConsentText: /consent|before you continue|i agree|accept all/i.test(bodyText),
                        hasBotCheckText: /sign in to confirm|not a bot|verify you.re human/i.test(bodyText),
                        hasAgeGateText: /age.restrict|confirm your age/i.test(bodyText),
                        playerClasses: player ? player.className : null,
                        playerDataState: player ? player.getAttribute('data-state') : null,
                        bodyTextSnippet: bodyText.slice(0, 400)
                    });
                })()
            """)
            logger.warning(f'[diagnostic] page state at failure: {diagnostic}')
        except Exception as e:
            logger.warning(f'[diagnostic] failed to evaluate page state: {type(e)}, {e}')

    @staticmethod
    async def _click_on_player(tab: nodriver.Tab) -> bool:
        try:
            player = await tab.select('#movie_player', 10)
        except asyncio.TimeoutError:
            logger.warning('update failed: unable to locate video player on the page')
            return False
        else:
            await player.click()
            return True

    async def _wait_for_handler(self) -> bool:
        try:
            await asyncio.wait_for(self._extraction_done.wait(), timeout=30)
        except asyncio.TimeoutError:
            logger.warning('update failed: timeout waiting for outgoing API request')
            return False
        else:
            logger.info('update was succeessful')
            return True

    async def _send_handler(self, event: nodriver.cdp.network.RequestWillBeSent) -> None:
        if not event.request.method == 'POST':
            return
        if '/youtubei/v1/player' not in event.request.url:
            return

        token_info = self._extract_token(event.request)
        if token_info is None:
            return

        logger.info(f'new token: {token_info.to_json()}')
        self._token_info = token_info
        self._extraction_done.set()
