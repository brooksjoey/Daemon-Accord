class Page:
    async def goto(self, url, wait_until=None, timeout=None):
        raise NotImplementedError

    async def close(self):
        return

    async def set_viewport_size(self, viewport):
        return

    async def evaluate(self, script):
        return script


class Browser:
    async def new_page(self):
        return Page()

    async def close(self):
        return


class _Chromium:
    async def launch(self, headless=True, args=None):
        return Browser()


class _Playwright:
    def __init__(self):
        self.chromium = _Chromium()

    async def stop(self):
        return


class _PlaywrightManager:
    async def start(self):
        return _Playwright()


def async_playwright():
    return _PlaywrightManager()

