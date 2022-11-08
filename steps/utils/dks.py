import base64
import binascii
import os
from functools import lru_cache
from typing import Tuple

from Crypto.Cipher import AES
from Crypto.Util import Counter
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from data import EncryptionMaterials, UCMessage


class HTTPRetry:
    def __init__(
        self,
        retries: int = 10,
        backoff: int = 0.1,
        status_forcelist: Tuple[int] = (429, 500, 502, 503, 504),
        methods: Tuple[str] = ("POST", "GET"),
    ):
        self._retry_strategy = Retry(
            total=retries,
            backoff_factor=backoff,
            status_forcelist=status_forcelist,
            allowed_methods=methods,
        )

    def retry_session(self, http=False):
        adapter = HTTPAdapter(max_retries=self._retry_strategy)
        session = Session()
        session.mount("https://", adapter)
        if http:
            session.mount("http://", adapter)
        return session


class DKSService:
    def __init__(
        self,
        dks_decrypt_endpoint: str,
        dks_datakey_endpoint: str,
        certificates: tuple,
        http_retry_config: HTTPRetry = HTTPRetry(),
        dks_call_accumulator=None,
    ):
        self._dks_decrypt_endpoint = dks_decrypt_endpoint
        self._dks_datakey_endpoint = dks_datakey_endpoint
        self._http_retry = http_retry_config
        self._certificates = certificates
        self.dks_call_count = dks_call_accumulator

    def get_new_data_key(self) -> dict:
        with self._http_retry.retry_session() as session:
            response = session.get(
                url=self._dks_datakey_endpoint,
                cert=self._certificates[0],
                verify=self._certificates[1],
            )
            content = response.json()
            return content

    def _get_decrypted_key_from_dks(
        self, encrypted_data_key: str, key_encryption_key_id: str
    ) -> str:
        if self.dks_call_count is not None:
            self.dks_call_count += 1

        with self._http_retry.retry_session() as session:
            response = session.post(
                url=self._dks_decrypt_endpoint,
                params={"keyId": key_encryption_key_id, "correlationId": ""},
                data=encrypted_data_key,
                cert=self._certificates[0],
                verify=self._certificates[1],
            )
        content = response.json()
        return content

    @lru_cache(maxsize=int(os.getenv("DKS_CACHE_SIZE", "128")))
    def decrypt_data_key(self, encryption_materials: EncryptionMaterials) -> str:
        return self._get_decrypted_key_from_dks(
            encryption_materials.encryptedEncryptionKey,
            encryption_materials.keyEncryptionKeyId,
        )


class MessageCryptoHelper:
    def __init__(self, data_key_service: DKSService):
        self.data_key_service = data_key_service

    @staticmethod
    def decrypt_string(ciphertext: str, data_key: str, iv: str) -> str:
        key_bytes = base64.b64decode(data_key)
        ciphertext_bytes = base64.b64decode(ciphertext)
        iv_bytes = base64.b64decode(iv)
        iv_int = int(binascii.hexlify(iv_bytes), 16)

        counter = Counter.new(nbits=AES.block_size * 8, initial_value=iv_int)
        aes = AES.new(key=key_bytes, mode=AES.MODE_CTR, counter=counter)
        decrypted_bytes: bytes = aes.decrypt(ciphertext_bytes)
        return decrypted_bytes.decode("utf8")

    def get_decrypted_dbobject(self, message: UCMessage) -> str:
        encryption_materials = message.encryption_materials
        data_key = self.data_key_service.decrypt_data_key(encryption_materials)
        decrypted_dbobject: str = self.decrypt_string(
            ciphertext=message.encrypted_dbobject,
            data_key=data_key,
            iv=encryption_materials.initialisationVector,
        )
        return decrypted_dbobject
