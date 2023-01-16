import base64
import binascii
import os
from dataclasses import dataclass
from functools import lru_cache
from logging import getLogger
from typing import Tuple

import pyspark
from Crypto.Cipher import AES
from Crypto.Util import Counter
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from data import EncryptionMaterials, UCMessage

logger = getLogger("dks")


@dataclass
class RetryConfig:
    retries: int = 3
    backoff: int = 0.1
    status_forcelist: Tuple[int] = (429, 500, 502, 503, 504)
    methods: Tuple[str] = ("POST", "GET")


class DKSService:
    def __init__(
        self,
        dks_decrypt_endpoint: str,
        dks_datakey_endpoint: str,
        certificates: tuple,
        verify: str,
        retry_config: RetryConfig = RetryConfig(),
        dks_call_accumulator: pyspark.Accumulator = None,
    ):
        self._dks_decrypt_endpoint = dks_decrypt_endpoint
        self._dks_datakey_endpoint = dks_datakey_endpoint
        self._retry_config = retry_config
        self._certificates = certificates
        self._verify = verify
        self._http_adapter = None
        self.dks_call_count = dks_call_accumulator

    def _retry_session(self, http=False) -> Session:
        if not self._http_adapter:
            self._http_adapter = HTTPAdapter(
                max_retries=Retry(
                    total=self._retry_config.retries,
                    backoff_factor=self._retry_config.backoff,
                    status_forcelist=self._retry_config.status_forcelist,
                    allowed_methods=self._retry_config.methods,
                )
            )
        session = Session()
        session.mount("https://", self._http_adapter)
        if http:
            session.mount("http://", self._http_adapter)
        return session

    def get_new_data_key(self) -> dict:
        with self._retry_session() as session:
            response = session.get(
                url=self._dks_datakey_endpoint,
                cert=self._certificates[0],
                verify=self._certificates[1],
            )
            content = response.json()
            return content

    def _get_decrypted_key_from_dks(
        self, encrypted_data_key: str, key_encryption_key_id: str, correlation_id: str,
    ) -> str:
        with self._retry_session() as session:
            response = session.post(
                url=self._dks_decrypt_endpoint,
                params={
                    "keyId": key_encryption_key_id,
                    "correlationId": correlation_id,
                },
                data=encrypted_data_key,
                cert=self._certificates,
                verify=self._verify,
            )

        content = response.json()

        if "plaintextDataKey" not in content:
            # todo: check response code & provide detail in exception message
            raise Exception("Unable to retrieve datakey from DKS")

        if self.dks_call_count is not None:
            self.dks_call_count += 1
        return content["plaintextDataKey"]

    @lru_cache(maxsize=int(os.getenv("DKS_CACHE_SIZE", "128")))
    def decrypt_data_key(
        self, encryption_materials: EncryptionMaterials, correlation_id: str,
    ) -> str:
        return self._get_decrypted_key_from_dks(
            encryption_materials.encryptedEncryptionKey,
            encryption_materials.keyEncryptionKeyId,
            correlation_id,
        )


class MessageCryptoHelper(object):
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

    def decrypt_message_dbObject(
        self,
        message: UCMessage,
        correlation_id: str,
        record_accumulator: pyspark.Accumulator = None,
    ) -> UCMessage:
        encryption_materials = message.encryption_materials
        data_key = self.data_key_service.decrypt_data_key(
            encryption_materials, correlation_id
        )
        decrypted_dbobject: str = self.decrypt_string(
            ciphertext=message.dbobject,
            data_key=data_key,
            iv=encryption_materials.initialisationVector,
        )

        if record_accumulator:
            record_accumulator += 1

        return message.get_decrypted_uc_message(decrypted_dbobject)
