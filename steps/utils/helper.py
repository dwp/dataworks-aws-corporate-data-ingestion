from typing import Tuple

from requests import Session
from requests.adapters import HTTPAdapter
from urllib3 import Retry


class HTTPRetryHelper:
    def __init__(self, retries: int = 10, backoff: int = 0.1,
                 status_forcelist: Tuple[int] = (429, 500, 502, 503, 504),
                 methods: Tuple[str] = ("PUT",),
                 ):
        self._retry_strategy = Retry(
            total=retries,
            backoff_factor=backoff,
            status_forcelist=status_forcelist,
            allowed_methods=methods,
        )

    def retry_session(self):
        adapter = HTTPAdapter(max_retries=self._retry_strategy)
        session = Session()
        session.mount("https://", adapter)
        return session
