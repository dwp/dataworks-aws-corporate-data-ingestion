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
        dks_service._get_decrypted_key_from_dks = MagicMock(side_effect=lambda x, y: x)
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
        message = {"message": {
            "encryption": {
                **encryption_material.__dict__
            },
            "dbObject": dbObject
        }}
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
                dks_service.decrypt_data_key(encryption_materials=materials),
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
                dks_service.decrypt_data_key(encryption_materials=materials),
                expected_result,
            )
            self.assertEqual(dks_service.decrypt_data_key.cache_info().hits, index + 1)
            self.assertEqual(dks_service.decrypt_data_key.cache_info().misses, unique_requests)
            self.assertEqual(dks_service.decrypt_data_key.cache_info().currsize, unique_requests)
            self.assertEqual(dks_service._get_decrypted_key_from_dks.call_count, unique_requests)

        # Assert the total expected hits/misses, cache size before+after clearing
        self.assertEqual(
            dks_service.decrypt_data_key.cache_info().hits, unique_requests
        )
        self.assertEqual(
            dks_service.decrypt_data_key.cache_info().misses, unique_requests
        )
        self.assertEqual(
            dks_service.decrypt_data_key.cache_info().currsize, unique_requests
        )
        dks_service.decrypt_data_key.cache_clear()
        self.assertEqual(dks_service.decrypt_data_key.cache_info().hits, 0)
        self.assertEqual(dks_service.decrypt_data_key.cache_info().misses, 0)
        self.assertEqual(dks_service.decrypt_data_key.cache_info().currsize, 0)


class TestMessageDecryptionHelper(TestCase):
    def test_decrypt_string(self):
        """Test decryption of ciphertext.
            - Generates 50 plain+encrypted payloads with EncryptionMaterials
            - Data key remains plaintext (dks mocked)
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
            uc_message = MagicMock(UCMessage)
            uc_message.encryption_materials = encryption_materials
            uc_message.encrypted_dbobject = ciphertext
            self.assertEqual(plaintext, decryption_helper.decrypt_dbobject(uc_message))

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
            decryption_helper.decrypt_string = MagicMock(side_effect=lambda ciphertext, data_key, iv: ciphertext)
            decryption_helper.data_key_service.decrypt_data_key = MagicMock(side_effect=lambda x: x.encryptedEncryptionKey)

            message, encryption_material, db_object = TestUtils.generate_test_uc_message(index)
            decryption_helper.decrypt_dbobject(message=message)
            decryption_helper.data_key_service.decrypt_data_key.assert_called_once_with(encryption_material)
            decryption_helper.decrypt_string.assert_called_once_with(
                ciphertext=db_object,
                data_key=encryption_material.encryptedEncryptionKey,
                iv=encryption_material.initialisationVector,
            )


