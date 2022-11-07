from unittest import TestCase
from unittest.mock import MagicMock

from dks import DKSService, MessageDecryptionHelper
from helper import HTTPRetryHelper
from data import UCMessage
from test_data_generator import generate_test_encryption_material, generate_encrypted_string


class TestDKSCache(TestCase):
    def test_dks_cache(self):
        """LRU Cache works as intended and reduces requests to decrypt data keys"""
        unique_requests = 50
        dks_service = DKSService(
            dks_decrypt_endpoint="dks.local/data/decrypt",
            https_retry_service=HTTPRetryHelper(),
            certificates=(None, None),
        )

        def mock_decryption(*args):
            return args[0]+"-decrypted"

        dks_service._get_decrypted_key_from_dks = MagicMock(side_effect=mock_decryption)

        requests = [
            (index, materials, mock_decryption(materials.encryptedEncryptionKey))
            for index, materials in [generate_test_encryption_material(i) for i in range(unique_requests)]
        ]

        for index, materials, expected_result in requests:
            # For each request, assert the result, cache hits, misses, size and requests to decrypt
            self.assertEqual(
                dks_service.decrypt_data_key(encryption_materials=materials),
                expected_result
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
                expected_result
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
        dks_service = DKSService(
            dks_endpoint="",
            dks_decrypt_endpoint="",
            https_retry_service=HTTPRetryHelper(),
            certificates=(None, None),
        )
        # mock the data key service - data key not encrypted
        dks_service._get_decrypted_key_from_dks = MagicMock(side_effect=lambda x, y: x)
        decryption_helper = MessageDecryptionHelper(dks_service)

        unique_messages = 50

        encrypt_decrypt_tests = [
            (i, f"TEST_PLAINTEXT_{i}", *generate_encrypted_string(f"TEST_PLAINTEXT_{i}"))
            for i in range(unique_messages)
        ]

        for index, plaintext, ciphertext, encryption_materials in encrypt_decrypt_tests:
            uc_message = MagicMock(UCMessage)
            uc_message.encryption_materials = encryption_materials
            uc_message.encrypted_dbobject = ciphertext
            self.assertEqual(plaintext, decryption_helper.get_decrypted_dbobject(uc_message))
