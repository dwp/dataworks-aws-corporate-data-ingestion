import gzip
import re
from typing import Optional, List, Tuple, Dict

import pyspark

from dks import MessageCryptoHelper, DKSService


class ValidationError(BaseException):
    pass


class Utils(object):
    @staticmethod
    def get_decryption_helper(
        decrypt_endpoint: str,
        correlation_id: str,
        dks_hit_acc: Optional[pyspark.Accumulator] = None,
        dks_miss_acc: Optional[pyspark.Accumulator] = None,
    ) -> MessageCryptoHelper:
        certificates = (
            "/etc/pki/tls/certs/private_key.crt",
            "/etc/pki/tls/private/private_key.key",
        )
        verify = "/etc/pki/ca-trust/source/anchors/analytical_ca.pem"

        return MessageCryptoHelper(
            data_key_service=DKSService(
                dks_decrypt_endpoint=decrypt_endpoint,
                dks_datakey_endpoint="not_configured",
                certificates=certificates,
                verify=verify,
                dks_hit_acc=dks_hit_acc,
                dks_miss_acc=dks_miss_acc,
            ),
            correlation_id=correlation_id
        )
