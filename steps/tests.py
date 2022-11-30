import base64
import binascii
import json
from unittest import TestCase
from unittest.mock import MagicMock

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

        return ciphertext_ascii, EncryptionMaterials(
            encryptionKeyId="not_encrypted",
            encryptedEncryptionKey=datakey_ascii,
            initialisationVector=iv_ascii,
            keyEncryptionKeyId="",
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

        dks_service._get_decrypted_key_from_dks = MagicMock(side_effect=cls.mock_decrypt)
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
        dks_service._get_decrypted_key_from_dks = MagicMock(side_effect=lambda *args: args[0])
        return dks_service

    @staticmethod
    def generate_test_encryption_material(index: int) -> (int, EncryptionMaterials):
        return index, EncryptionMaterials(
            keyEncryptionKeyId=f"KeyEncryptionKeyId-{index}",
            initialisationVector=f"initialisationVector-{index}",
            encryptedEncryptionKey=f"encryptedEncryptionKey-{index}",
            encryptionKeyId="",
        )

    @classmethod
    def generate_test_uc_message(cls, index: int) -> (UCMessage, EncryptionMaterials):
        _, encryption_material = cls.generate_test_encryption_material(index)
        dbObject = f"__encrypted_db_object__{index}"
        message = {
            "message": {
                "encryption": {**encryption_material.__dict__},
                "dbObject": dbObject,
            }
        }
        return UCMessage(json.dumps(message)), encryption_material, dbObject


class TestDKSCache(TestCase):
    def test_dks_cache(self):
        """LRU Cache works as intended and reduces requests to decrypt data keys"""
        unique_requests = 50

        test_encryption_materials = [TestUtils.generate_test_encryption_material(i) for i in range(unique_requests)]

        requests = [
            (index, materials, TestUtils.mock_decrypt(materials.encryptedEncryptionKey))
            for index, materials in test_encryption_materials
        ]

        dks_service = TestUtils.dks_mock_datakey_decrypt()

        for index, materials, expected_result in requests:
            # For each request, assert the result, cache hits, misses, size and requests to decrypt
            self.assertEqual(
                dks_service.decrypt_data_key(encryption_materials=materials, correlation_id=""),
                expected_result,
            )
            self.assertEqual(dks_service.decrypt_data_key.cache_info().hits, 0)
            self.assertEqual(dks_service.decrypt_data_key.cache_info().misses, index + 1)
            self.assertEqual(dks_service.decrypt_data_key.cache_info().currsize, index + 1)
            self.assertEqual(dks_service._get_decrypted_key_from_dks.call_count, index + 1)

        for index, materials, expected_result in requests:
            # Repeat the same requests again
            # For each request, assert the result, cache hits, misses, size and requests to decrypt
            self.assertEqual(
                dks_service.decrypt_data_key(encryption_materials=materials, correlation_id=""),
                expected_result,
            )
            self.assertEqual(dks_service.decrypt_data_key.cache_info().hits, index + 1)
            self.assertEqual(dks_service.decrypt_data_key.cache_info().misses, unique_requests)
            self.assertEqual(dks_service.decrypt_data_key.cache_info().currsize, unique_requests)
            self.assertEqual(dks_service._get_decrypted_key_from_dks.call_count, unique_requests)

        # Assert the total expected hits/misses, cache size before+after clearing
        self.assertEqual(dks_service.decrypt_data_key.cache_info().hits, unique_requests)
        self.assertEqual(dks_service.decrypt_data_key.cache_info().misses, unique_requests)
        self.assertEqual(dks_service.decrypt_data_key.cache_info().currsize, unique_requests)
        dks_service.decrypt_data_key.cache_clear()
        self.assertEqual(dks_service.decrypt_data_key.cache_info().hits, 0)
        self.assertEqual(dks_service.decrypt_data_key.cache_info().misses, 0)
        self.assertEqual(dks_service.decrypt_data_key.cache_info().currsize, 0)


class TestMessageDecryptionHelper(TestCase):
    def test_decrypt_string(self):
        """Test decryption of ciphertext.
        - Generates 50 plain+encrypted payloads with EncryptionMaterials
        - Decrypted value compared to initial plaintext
        """
        dks_service = TestUtils.dks_mock_no_datakey_encryption()
        decryption_helper = MessageCryptoHelper(dks_service)

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
        decryption_helper = MessageCryptoHelper(data_key_service=dks_service)
        unique_messages = 50

        for index in range(unique_messages):
            # new mocks
            decryption_helper.decrypt_string = MagicMock(
                side_effect=lambda ciphertext, data_key, iv: ciphertext + "-decrypted"
            )
            decryption_helper.data_key_service.decrypt_data_key = MagicMock(
                side_effect=lambda *args: args[0].encryptedEncryptionKey
            )

            (message, encryption_material, db_object) = TestUtils.generate_test_uc_message(index)
            decrypted_uc_message = decryption_helper.decrypt_dbobject(message=message, correlation_id="")

            self.assertIn("message", decrypted_uc_message.message_json)
            self.assertIn("dbObject", decrypted_uc_message.message_json["message"])
            self.assertNotIn("encryption", decrypted_uc_message.message_json["message"])
            self.assertEqual(db_object + "-decrypted", decrypted_uc_message.dbobject)

            decryption_helper.data_key_service.decrypt_data_key.assert_called_once_with(encryption_material, "")
            decryption_helper.decrypt_string.assert_called_once_with(
                ciphertext=db_object,
                data_key=encryption_material.encryptedEncryptionKey,
                iv=encryption_material.initialisationVector,
            )


class TestUCMessage(TestCase):
    @staticmethod
    def get_event(lastModifiedDateTime=None, createdDateTime=None, kafkaTimestamp=None, message_type=None):
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

    def test_getLastModified(self):
        epoch = "1980-01-01T00:00:00.000+0000"

        # where the record type is MONGO_DELETE use kt > lt > ct > epoch
        self.assertEqual(
            ("kt-0123", "kafkaMessageDateTime"),
            UCMessage(self.get_event("lt-0123", "ct-0123", "kt-0123", "MONGO_DELETE")).last_modified,
        )
        self.assertEqual(
            ("lt-0123", "_lastModifiedDateTime"),
            UCMessage(self.get_event("lt-0123", "ct-0123", "", "MONGO_DELETE")).last_modified,
        )
        self.assertEqual(
            ("lt-0123", "_lastModifiedDateTime"),
            UCMessage(self.get_event("lt-0123", "ct-0123", None, "MONGO_DELETE")).last_modified,
        )
        self.assertEqual(
            ("ct-0123", "createdDateTime"),
            UCMessage(self.get_event("", "ct-0123", None, "MONGO_DELETE")).last_modified,
        )
        self.assertEqual(
            ("ct-0123", "createdDateTime"),
            UCMessage(self.get_event(None, "ct-0123", None, "MONGO_DELETE")).last_modified,
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
            ("kt-0123", "kafkaMessageDateTime"),
            UCMessage(self.get_event("", "", "kt-0123", "MONGO_DELETE")).last_modified,
        )
        self.assertEqual(
            UCMessage(self.get_event(None, None, "kt-0123", "MONGO_DELETE")).last_modified,
            ("kt-0123", "kafkaMessageDateTime"),
        )

        # where the record is not "MONGO_DELETE", use lt > ct > epoch
        self.assertEqual(
            ("lt-0123", "_lastModifiedDateTime"),
            UCMessage(self.get_event("lt-0123", "ct-0123", "kt-0123", "UPDATE")).last_modified,
        )
        self.assertEqual(
            ("ct-0123", "createdDateTime"),
            UCMessage(self.get_event("", "ct-0123", "kt-0123", "UPDATE")).last_modified,
        )
        self.assertEqual(
            ("ct-0123", "createdDateTime"),
            UCMessage(self.get_event(None, "ct-0123", "kt-0123", "UPDATE")).last_modified,
        )
        self.assertEqual(
            (epoch, "epoch"),
            UCMessage(self.get_event(None, "", "kt-0123", "UPDATE")).last_modified,
        )
        self.assertEqual(
            (epoch, "epoch"),
            UCMessage(self.get_event(None, None, "kt-0123", "UPDATE")).last_modified,
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
            ("lt-0123", "_lastModifiedDateTime"),
            UCMessage(self.get_event("lt-0123", "", "", "UPDATE")).last_modified,
        )
        self.assertEqual(
            ("lt-0123", "_lastModifiedDateTime"),
            UCMessage(self.get_event("lt-0123", None, None, "UPDATE")).last_modified,
        )

    def test_get_decrypted_uc_message(self):
        standard_test = json.dumps(
            {
                "message": {
                    "encryption": {"encryptiona": "a", "encryptionb": "b", "encryptionc": "c"},
                    "dbObject": "'encrypted dbobject'",
                }
            }
        )
        encryption_missing_test = json.dumps({"message": {"dbObject": "'encrypted dbobject'"}})
        dbobject_missing_test = json.dumps(
            {
                "message": {
                    "encryption": {"encryptiona": "a", "encryptionb": "b", "encryptionc": "c"},
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
            self.assertEqual("'decrypted dbObject'", result.message_json["message"]["dbObject"])
