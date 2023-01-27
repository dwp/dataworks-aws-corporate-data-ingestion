import base64
import binascii
from datetime import datetime, timezone
import json
from typing import Dict
from unittest import TestCase
from unittest.mock import MagicMock

import pyspark.sql
from Crypto import Random
from Crypto.Cipher import AES
from Crypto.Util import Counter

from data import UCMessage, EncryptionMaterials
from dks import DKSService, RetryConfig, MessageCryptoHelper


class TestUtils:
    @staticmethod
    def mock_decrypt(cipher_text: str, *args) -> str:
        return cipher_text + "-decrypted"

    @staticmethod
    def generate_encrypted_string(input_string: str) -> (str, EncryptionMaterials):
        datakey_bytes = Random.get_random_bytes(16)
        input_bytes = input_string.encode("utf8")
        iv_bytes = Random.new().read(AES.block_size)

        iv_int = int(binascii.hexlify(iv_bytes), 16)

        counter = Counter.new(nbits=AES.block_size * 8, initial_value=iv_int)
        aes = AES.new(key=datakey_bytes, mode=AES.MODE_CTR, counter=counter)
        ciphertext = aes.encrypt(input_bytes)

        datakey_ascii = base64.b64encode(datakey_bytes).decode("ascii")
        ciphertext_ascii = base64.b64encode(ciphertext).decode("ascii")
        iv_ascii = base64.b64encode(iv_bytes).decode("ascii")

        return (
            ciphertext_ascii,
            EncryptionMaterials(
                encryptionKeyId="not_encrypted",
                encryptedEncryptionKey=datakey_ascii,
                initialisationVector=iv_ascii,
                keyEncryptionKeyId="",
            ),
        )

    @classmethod
    def dks_mock_datakey_decrypt(cls):
        """Returns dks service with mocked datakey decryption"""
        dks_service = DKSService(
            dks_decrypt_endpoint="http://localhost:8443/datakey/actions/decrypt",
            dks_datakey_endpoint="http://localhost:8443/datakey",
            retry_config=RetryConfig(),
            certificates=(None, None),
            verify="",
        )

        dks_service._get_decrypted_key_from_dks = MagicMock(
            side_effect=cls.mock_decrypt
        )
        return dks_service

    @staticmethod
    def dks_mock_no_datakey_encryption():
        """Returns dks service with no decryption (encrypted data key == decrypted data key)"""
        dks_service = DKSService(
            dks_decrypt_endpoint="http://localhost:8443/datakey/actions/decrypt",
            dks_datakey_endpoint="http://localhost:8443/datakey",
            certificates=(None, None),
            verify="",
            retry_config=RetryConfig(),
        )
        dks_service._get_decrypted_key_from_dks = MagicMock(
            side_effect=lambda *args: args[0]
        )
        return dks_service

    @staticmethod
    def generate_test_encryption_material(index: int) -> Dict[str, str]:
        return {
            "keyEncryptionKeyId": f"KeyEncryptionKeyId-{index}",
            "initialisationVector": f"initialisationVector-{index}",
            "encryptedEncryptionKey": f"encryptedEncryptionKey-{index}",
            "encryptionKeyId": "",
        }

    @classmethod
    def generate_test_uc_message(cls, index: int) -> (UCMessage, EncryptionMaterials):
        encryption_material_dict = cls.generate_test_encryption_material(index)
        dbObject = f"__encrypted_db_object__{index}"
        message = {
            "message": {
                "encryption": encryption_material_dict,
                "dbObject": dbObject,
            }
        }
        return UCMessage(json.dumps(message)), EncryptionMaterials(**encryption_material_dict), dbObject


class TestDKSCache(TestCase):
    def test_cache(self):
        spark_session = pyspark.sql.SparkSession.builder.getOrCreate()
        hits = spark_session.sparkContext.accumulator(0)
        misses = spark_session.sparkContext.accumulator(0)

        unique_materials = 5
        repeat_materials = 30

        expected_misses = unique_materials
        expected_hits = (unique_materials * repeat_materials) - unique_materials

        # All records in 1 partition makes caching predictable
        test_rdd = spark_session.sparkContext.parallelize(
            repeat_materials * [
                EncryptionMaterials(**TestUtils.generate_test_encryption_material(index))
                for index in range(unique_materials)
            ]
        ).repartition(1)

        cache = {}
        dks_service = TestUtils.dks_mock_no_datakey_encryption()
        dks_service._dks_hit_acc = hits
        dks_service._dks_miss_acc = misses
        (
            test_rdd
            .map(
                lambda x: dks_service.decrypt_data_key(
                    encryption_materials=x,
                    correlation_id="TEST",
                    dks_key_cache=cache,
                )
            )
            .collect()
        )

        self.assertEqual(expected_misses, misses.value)
        self.assertEqual(expected_hits, hits.value)


class TestMessageDecryptionHelper(TestCase):
    def test_decrypt_string(self):
        """Test decryption of ciphertext.
        - Generates 50 plain+encrypted payloads with EncryptionMaterials
        - Decrypted value compared to initial plaintext
        """
        dks_service = TestUtils.dks_mock_no_datakey_encryption()
        decryption_helper = MessageCryptoHelper(data_key_service=dks_service, correlation_id="TEST")

        unique_messages = 50

        encrypt_decrypt_tests = [
            (
                index,
                f"TEST_PLAINTEXT_{index}",
                *TestUtils.generate_encrypted_string(f"TEST_PLAINTEXT_{index}"),
            )
            for index in range(unique_messages)
        ]

        for index, plaintext, ciphertext, encryption_materials in encrypt_decrypt_tests:
            decrypted_text = decryption_helper.decrypt_string(
                ciphertext=ciphertext,
                data_key=encryption_materials.encryptedEncryptionKey,
                iv=encryption_materials.initialisationVector,
            )

            self.assertEqual(plaintext, decrypted_text)

    def test_decrypt_dbobject(self):
        """Tests parsing of message sending ciphertext for decryption
        - UC Message generated and passed to decrypt_message function
        - parameters checked for decrypt data key
        - parameters checked for decrypt dbObject
        """
        dks_service = DKSService(
            dks_decrypt_endpoint="http://localhost:8443/datakey/actions/decrypt",
            dks_datakey_endpoint="http://localhost:8443/datakey",
            retry_config=RetryConfig(),
            certificates=(None, None),
            verify="",
        )
        decryption_helper = MessageCryptoHelper(data_key_service=dks_service, correlation_id="TEST")
        unique_messages = 50

        for index in range(unique_messages):
            # new mocks
            decryption_helper.decrypt_string = \
                MagicMock(side_effect=lambda ciphertext, data_key, iv: ciphertext + "-decrypted")
            decryption_helper.data_key_service.decrypt_data_key = \
                MagicMock(side_effect=lambda **kwargs: kwargs["encryption_materials"].encryptedEncryptionKey)
            (
                message,
                encryption_material,
                db_object,
            ) = TestUtils.generate_test_uc_message(index)
            decrypted_uc_message = decryption_helper.decrypt_dbObject(message=message, dks_key_cache={})

            self.assertIn("message", decrypted_uc_message.message_json)
            self.assertIn("dbObject", decrypted_uc_message.message_json["message"])
            self.assertNotIn("encryption", decrypted_uc_message.message_json["message"])
            self.assertEqual(db_object + "-decrypted", decrypted_uc_message.dbobject)

            decryption_helper.data_key_service.decrypt_data_key.assert_called_once_with(
                encryption_materials=encryption_material,
                correlation_id="TEST",
                dks_key_cache={},
            )
            decryption_helper.decrypt_string.assert_called_once_with(
                ciphertext=db_object,
                data_key=encryption_material.encryptedEncryptionKey,
                iv=encryption_material.initialisationVector,
            )


class TestUCMessage(TestCase):
    @staticmethod
    def get_event(
        lastModifiedDateTime=None,
        createdDateTime=None,
        kafkaTimestamp=None,
        message_type=None,
    ):
        message = {"message": {}}
        if message_type is not None:
            message["message"].update({"@type": message_type})
        if lastModifiedDateTime is not None:
            message["message"].update({"_lastModifiedDateTime": lastModifiedDateTime})
        if createdDateTime is not None:
            message["message"].update({"createdDateTime": createdDateTime})
        if kafkaTimestamp is not None:
            message.update({"timestamp": kafkaTimestamp})
        return json.dumps(message)

    def test_get_last_modified(self):
        epoch = "1980-01-01T00:00:00.000+0000"

        def format_ts(ts: datetime) -> str:
            return ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + ts.strftime("%z")

        ct = format_ts(datetime(year=2000, month=1, day=1, tzinfo=timezone.utc))
        kt = format_ts(datetime(year=2000, month=1, day=2, tzinfo=timezone.utc))
        lt = format_ts(datetime(year=2000, month=1, day=3, tzinfo=timezone.utc))

        # where the record type is MONGO_DELETE use kt > lt > ct > epoch
        self.assertEqual(
            (kt, "kafkaMessageDateTime"),
            UCMessage(self.get_event(lt, ct, kt, "MONGO_DELETE")).last_modified,
        )
        self.assertEqual(
            (lt, "_lastModifiedDateTime"),
            UCMessage(self.get_event(lt, ct, "", "MONGO_DELETE")).last_modified,
        )
        self.assertEqual(
            (lt, "_lastModifiedDateTime"),
            UCMessage(self.get_event(lt, ct, None, "MONGO_DELETE")).last_modified,
        )
        self.assertEqual(
            (ct, "createdDateTime"),
            UCMessage(self.get_event("", ct, None, "MONGO_DELETE")).last_modified,
        )
        self.assertEqual(
            (ct, "createdDateTime"),
            UCMessage(self.get_event(None, ct, None, "MONGO_DELETE")).last_modified,
        )
        self.assertEqual(
            (epoch, "epoch"),
            UCMessage(self.get_event(None, "", None, "MONGO_DELETE")).last_modified,
        )
        self.assertEqual(
            (epoch, "epoch"),
            UCMessage(self.get_event(None, None, None, "MONGO_DELETE")).last_modified,
        )
        self.assertEqual(
            (kt, "kafkaMessageDateTime"),
            UCMessage(self.get_event("", "", kt, "MONGO_DELETE")).last_modified,
        )
        self.assertEqual(
            (kt, "kafkaMessageDateTime"),
            UCMessage(self.get_event(None, None, kt, "MONGO_DELETE")).last_modified,
        )

        # where the record is not "MONGO_DELETE", use lt > ct > epoch
        self.assertEqual(
            (lt, "_lastModifiedDateTime"),
            UCMessage(self.get_event(lt, ct, kt, "UPDATE")).last_modified,
        )
        self.assertEqual(
            (ct, "createdDateTime"),
            UCMessage(self.get_event("", ct, kt, "UPDATE")).last_modified,
        )
        self.assertEqual(
            (ct, "createdDateTime"),
            UCMessage(self.get_event(None, ct, kt, "UPDATE")).last_modified,
        )
        self.assertEqual(
            (epoch, "epoch"),
            UCMessage(self.get_event(None, "", kt, "UPDATE")).last_modified,
        )
        self.assertEqual(
            (epoch, "epoch"),
            UCMessage(self.get_event(None, None, kt, "UPDATE")).last_modified,
        )
        self.assertEqual(
            (epoch, "epoch"),
            UCMessage(self.get_event(None, None, "", "UPDATE")).last_modified,
        )
        self.assertEqual(
            (epoch, "epoch"),
            UCMessage(self.get_event(None, None, None, "UPDATE")).last_modified,
        )
        self.assertEqual(
            (lt, "_lastModifiedDateTime"),
            UCMessage(self.get_event(lt, "", "", "UPDATE")).last_modified,
        )
        self.assertEqual(
            (lt, "_lastModifiedDateTime"),
            UCMessage(self.get_event(lt, None, None, "UPDATE")).last_modified,
        )

    def test_get_decrypted_uc_message(self):
        standard_test = json.dumps(
            {
                "message": {
                    "encryption": {
                        "encryptiona": "a",
                        "encryptionb": "b",
                        "encryptionc": "c",
                    },
                    "dbObject": "'encrypted dbobject'",
                }
            }
        )
        encryption_missing_test = json.dumps(
            {"message": {"dbObject": "'encrypted dbobject'"}}
        )
        dbobject_missing_test = json.dumps(
            {
                "message": {
                    "encryption": {
                        "encryptiona": "a",
                        "encryptionb": "b",
                        "encryptionc": "c",
                    },
                }
            }
        )

        results = [
            UCMessage(test).get_decrypted_uc_message("'decrypted dbObject'")
            for test in [standard_test, encryption_missing_test, dbobject_missing_test]
        ]

        for result in results:
            self.assertNotIn("encryption", result.message_json)
            self.assertIn("dbObject", result.message_json["message"])
            self.assertEqual(
                "'decrypted dbObject'", result.message_json["message"]["dbObject"]
            )

    def test_get_timestamp(self):
        # test successes (input_timestamp, expected ms_since_epoch)
        test_valid_timestamps = [
            ("2020-05-21T17:18:15.693+0000", "1590081495693"),
            ("2020-05-21T17:18:15.693000+0000", "1590081495693"),
            ("", "315532800000"),
        ]

        for test_kafka_timestamp, result in test_valid_timestamps:
            message = json.dumps(
                {
                    "message": {
                        "encryption": {},
                        "_lastModifiedDateTime": test_kafka_timestamp,
                    }
                }
            )
            self.assertEqual(result, UCMessage(message).timestamp)

        # test failures (input_timestamp, expected Exception)
        test_invalid_timestamps = [
            ("2020-05-21T17:18:15.693+00", ValueError),
            ("2020-05-21T17:18:15+0000", ValueError),
            ("2020-13-21T17:18:15+0000", ValueError),
            ("20-05-21T17:18:15+0000", ValueError),
            (123, TypeError),
            (None, TypeError),
        ]

        for test_kafka_timestamp, error in test_invalid_timestamps:
            message = json.dumps(
                {
                    "message": {
                        "encryption": {},
                        "_lastModifiedDateTime": test_kafka_timestamp,
                    }
                }
            )
            with self.assertRaises(error):
                _ = UCMessage(message).timestamp
