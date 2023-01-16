import zlib
from typing import Optional, List
import pyspark
from dks import MessageCryptoHelper, DKSService


class Utils(object):
    @staticmethod
    def decompress(compressed_text: bytes, accumulator: pyspark.Accumulator) -> bytes:
        """Decompresses jsonl.gz"""
        accumulator += 1
        return zlib.decompress(compressed_text, 16 + zlib.MAX_WBITS)

    @staticmethod
    def to_records(multi_record_bytes: bytes) -> List[str]:
        """Decodes, removes empty line from end of each file, and splits by line"""
        return multi_record_bytes.decode().rstrip("\n").split("\n")

    @staticmethod
    def get_decryption_helper(
        decrypt_endpoint: str,
        dks_call_accumulator: Optional[pyspark.Accumulator] = None,
    ) -> MessageCryptoHelper:
        certificates = (
            "/etc/pki/tls/certs/private_key.crt",
            "/etc/pki/tls/private/private_key.key",
        )
        verify = "/etc/pki/ca-trust/source/anchors/analytical_ca.pem"

        return MessageCryptoHelper(
            DKSService(
                dks_decrypt_endpoint=decrypt_endpoint,
                dks_datakey_endpoint="not_configured",
                certificates=certificates,
                verify=verify,
                dks_call_accumulator=dks_call_accumulator,
            )
        )
