import re
import gzip
from typing import Optional, List, Tuple
import pyspark
from dks import MessageCryptoHelper, DKSService


class ValidationError(BaseException):
    pass


class Utils(object):
    @staticmethod
    def decompress(compressed_text: bytes, accumulator: pyspark.Accumulator) -> bytes:
        """Decompresses jsonl.gz"""
        accumulator += 1
        return gzip.decompress(compressed_text)

    @staticmethod
    def to_records(record_tuple: Tuple[str, bytes]) -> List[str]:
        """Decodes, removes empty line from end of each file, and splits by line"""
        filename = record_tuple[0].split("/")[-1]
        lines = record_tuple[1].decode().rstrip("\n").split("\n")

        # Extract the offsets and calculate the number of records expected
        match = re.match(
            r"^(?P<collection>([a-zA-Z0-9\-_]+\.){1,2}[a-zA-Z0-9\-_]+)"
            r"[_-](?P<partition>\d+)"
            r"[_-](?P<start_offset>\d+)"
            r"[_-](?P<end_offset>\d+)"
            r"\.jsonl\.gz$",
            filename,
        )

        if not match:
            raise ValueError("Could not retrieve kafka offsets from filename")

        match_groups = match.groupdict()
        expected_lines = 1 + int(match_groups["end_offset"]) - int(match_groups["start_offset"])
        if expected_lines != len(lines):
            raise ValidationError(f"offset indicates '{expected_lines}' records, but '{len(lines)}' records"
                                  f" read from file: '{filename}'")
        return lines

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
