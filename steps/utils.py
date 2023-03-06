import csv
import io
from typing import Optional

import pyspark

from data import UCMessage
from dks import MessageCryptoHelper, DKSService


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

    @staticmethod
    def try_skip(message: UCMessage, skip_exceptions, accumulator, fn, *fn_args, **fn_kwargs):
        """Attempts to apply the given transformation.  If an Exception in `skip_exceptions`
            is encountered, the processing skips the record and logs the failure
        """
        try:
            return fn(message, *fn_args, **fn_kwargs)
        except skip_exceptions as e:
            accumulator += 1
            return message.error(e)

    @staticmethod
    def to_csv(row):
        output = io.StringIO(initial_value="", newline="")
        csv.writer(output).writerow(row)
        return output.getvalue().strip()
