import base64
import binascii
import json
from typing import Dict
from unittest import TestCase
from unittest.mock import MagicMock

import pyspark.sql
from Crypto import Random
from Crypto.Cipher import AES
from Crypto.Util import Counter

from data import UCMessage, EncryptionMaterials, DateWrapper, DateHelper
from dks import DKSService, RetryConfig, MessageCryptoHelper


class TestUtils:
    @staticmethod
    def mock_decrypt(cipher_text: str, *_args) -> str:
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
                "db": "test_db",
                "collection": "test_collection",
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
            uc_message_with_decrypted_record = (
                decryption_helper
                .decrypt_dbObject(message=message, dks_key_cache={})
            )

            self.assertEqual(db_object + "-decrypted", uc_message_with_decrypted_record.decrypted_record)

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
                    "dbObject": None
                }
            }
        )

        results = [
            UCMessage(test, "db:collection").set_decrypted_message("'decrypted dbObject'")
            for test in [standard_test, encryption_missing_test, dbobject_missing_test]
        ]

        for result in results:
            self.assertEqual(
                "'decrypted dbObject'", result.decrypted_record
            )


class TestUCMessageTransform(TestCase):
    def test_transform(self):
        """Test that the context element is enriched with auditType and timestamps"""
        mock_message = json.dumps({
            "message": {
                "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
                "dbObject": "mock_encrypted_dbobject"
            }
        })
        mock_audit_record = json.dumps({
            "context": {
                "AUDIT_ID": "12.0.0.1"
            },
            "auditType": "audit_type"
        })
        transformed_expected = json.dumps({
            "AUDIT_ID": "12.0.0.1",
            "AUDIT_EVENT": "audit_type",
            "TIME_STAMP": "2019-07-04T07:27:35.104+0000",
            "TIME_STAMP_ORIG": "2019-07-04T07:27:35.104+0000"
        })

        test_uc_message = UCMessage(mock_message, "data:businessAudit")
        test_uc_message.set_decrypted_message(mock_audit_record)
        test_uc_message.transform()
        self.assertEqual(test_uc_message.decrypted_record, transformed_expected)

    def test_transform_without_audit_type(self):
        """Ensure Exception raised when audit type not present"""
        mock_message = json.dumps({
            "message": {
                "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
                "dbObject": "mock_encrypted_dbobject"
            }
        })
        mock_audit_record = json.dumps({
            "context": {
                "AUDIT_ID": "12.0.0.1"
            },
            # "auditType": "audit_type"
        })
        test_uc_message = UCMessage(mock_message, "data:businessAudit")
        test_uc_message.set_decrypted_message(mock_audit_record)

        self.assertRaises(Exception, test_uc_message.transform)

    def test_transform_without_context(self):
        """Ensure exception raised when context not present"""
        mock_message = json.dumps({
            "message": {
                "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
                "dbObject": "mock_encrypted_dbobject"
            }
        })
        mock_audit_record = json.dumps({
            # "context": {
            #     "AUDIT_ID": "12.0.0.1"
            # },
            "auditType": "audit_type"
        })
        test_uc_message = UCMessage(mock_message, "data:businessAudit")
        test_uc_message.set_decrypted_message(mock_audit_record)

        self.assertRaises(Exception, test_uc_message.transform)


class TestUCMessageValidate(TestCase):
    def test_invalid_json(self):
        mock_message = json.dumps({
            "message": {
                "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
                "dbObject": "mock_encrypted_dbobject"
            }
        })
        invalid_decrypted_json = "{NOTVALID}{JSON}"

        message = UCMessage(mock_message, "some:collection")
        message.set_decrypted_message(invalid_decrypted_json)
        self.assertRaises(json.JSONDecodeError, message.validate)

    def test_record_is_primitive(self):
        mock_message = json.dumps({
            "message": {
                "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
                "dbObject": "mock_encrypted_dbobject"
            }
        })
        json_primitive = "some_normal_string"
        message = UCMessage(mock_message, "some:collection")
        message.set_decrypted_message(json_primitive)
        self.assertRaises(json.JSONDecodeError, message.validate)

    def test_should_remove_archived_ts_if_removed_ts_also_present(self):
        mock_message = json.dumps({
            "message": {
                "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
                "dbObject": "mock_encrypted_dbobject"
            }
        })
        decrypted_object = json.dumps({
            "_id": {"id": "12345"},
            "_archivedDateTime": "2021-10-10T03:35:51.145+0000",
            "_removedDateTime": "2021-10-12T10:06:01.280+0000",
            "_lastModifiedDateTime": "2021-10-02T14:02:16.653+0000"
        })
        message = UCMessage(mock_message, "some:collection")
        message.set_decrypted_message(decrypted_object)
        message.validate()

        output_decrypted_object = json.loads(message.decrypted_record)
        self.assertIn("_removedDateTime", output_decrypted_object)
        self.assertNotIn("_archivedDateTime", output_decrypted_object)

    def test_not_should_remove_archived_ts_if_removed_ts_not_present(self):
        mock_message = json.dumps({
            "message": {
                "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
                "dbObject": "mock_encrypted_dbobject"
            }
        })
        decrypted_object = json.dumps({
            "_id": {"id": "12345"},
            "_archivedDateTime": "2021-10-10T03:35:51.145+0000",
            "_lastModifiedDateTime": "2021-10-02T14:02:16.653+0000"
        })
        message = UCMessage(mock_message, "some:collection")
        message.set_decrypted_message(decrypted_object)
        message.validate()

        output_decrypted_object = json.loads(message.decrypted_record)
        self.assertIn("_archivedDateTime", output_decrypted_object)

    def test_should_tolerate_absent_id(self):
        mock_message = json.dumps({
            "message": {
                "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
                "dbObject": "mock_encrypted_dbobject"
            }
        })
        decrypted_object = json.dumps({
            "_id1": {"test_key_a": "test_value_a", "test_key_b": "test_value_b"},
            "_lastModifiedDateTime": "2018-12-14T15:01:02.000+0000"
        })
        message = UCMessage(mock_message, "some:collection")
        message.set_decrypted_message(decrypted_object)
        message.validate()

        expected_decrypted_record = json.dumps({
            "_id1": {
                "test_key_a": "test_value_a",
                "test_key_b": "test_value_b"
            },
            "_lastModifiedDateTime": {"$date": "2018-12-14T15:01:02.000Z"}
        })

        self.assertEqual(expected_decrypted_record, message.decrypted_record)

    def test_primitive_id(self):
        """Wrap json primitive IDs"""
        for primitive_id in ("PRIMITIVE_ID", 1234):
            mock_message = json.dumps({
                "message": {
                    "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
                    "dbObject": "mock_encrypted_dbobject"
                }
            })
            decrypted_object = json.dumps({
                "_id": primitive_id,
            })
            message = UCMessage(mock_message, "some:collection")
            message.set_decrypted_message(decrypted_object)
            message.validate()

            expected_decrypted_record_id = {
                "_id": {"$oid": str(primitive_id)},
            }

            self.assertDictEqual(expected_decrypted_record_id["_id"], json.loads(message.decrypted_record)["_id"])

    def test_json_id(self):
        """Don't wrap a json ID"""
        mock_message = json.dumps({
            "message": {
                "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
                "dbObject": "mock_encrypted_dbobject"
            }
        })
        decrypted_object = json.dumps({
            "_id": {"some_id": "actual_id"},
            "_archivedDateTime": "2021-10-10T03:35:51.145+0000",
            "_removedDateTime": "2021-10-12T10:06:01.280+0000",
            "_lastModifiedDateTime": "2021-10-02T14:02:16.653+0000"
        })
        message = UCMessage(mock_message, "some:collection")
        message.set_decrypted_message(decrypted_object)
        message.validate()

        expected_decrypted_record_id = {
            "_id": {"some_id": "actual_id"},
        }

        self.assertDictEqual(expected_decrypted_record_id["_id"], json.loads(message.decrypted_record)["_id"])

    def test_no_id(self):
        """Don't wrap an id that doesn't exist"""
        mock_message = json.dumps({
            "message": {
                "_lastModifiedDateTime": "2019-07-04T07:27:35.104+0000",
                "dbObject": "mock_encrypted_dbobject"
            }
        })
        decrypted_object = json.dumps({"some_key": "some_value"})
        message = UCMessage(mock_message, "some:collection")
        message.set_decrypted_message(decrypted_object)
        message.validate()

        self.assertIsNone(json.loads(message.decrypted_record).get("_id"))


class TestDateWrapper(TestCase):
    def test_process_nested_dates(self):
        """Check all dates processed except top level, with ignore flag applied"""
        date_key = "$date"
        test_object = {
            "_lastModifiedDateTime": {
                date_key: "2001-12-14T15:01:02.000+0000"
            },
            "notDate1": 123,
            "notDate2": "abc",
            "parentDate": "2017-12-14T15:01:02.000+0000",
            "childObjectWithDates": {
                "_lastModifiedDateTime": {
                    date_key: "1980-12-14T15:01:02.000+0000"
                },
                "grandChildObjectWithDate": {
                    "notDate1": 123,
                    "notDate2": "abc",
                    "grandChildDate1": "2019-12-14T15:01:02.000+0000"
                },
                "childDate": "2018-12-14T15:01:02.000+0000",
                "arrayWithDates": [
                    789,
                    "xyz",
                    "2010-12-14T15:01:02.000+0000",
                    [
                        "2011-12-14T15:01:02.000+0000",
                        "qwerty"
                    ],
                    {
                        "grandChildDate3": "2012-12-14T15:01:02.000+0000",
                        "_lastModifiedDateTime": "1995-12-14T15:01:02.000+0000"
                    }
                ]
            }
        }

        DateWrapper.process_object(test_object, False)

        expected_wrapped_object = {
            "_lastModifiedDateTime": {
                date_key: "2001-12-14T15:01:02.000+0000"
            },
            "notDate1": 123,
            "notDate2": "abc",
            "parentDate": {
                date_key: "2017-12-14T15:01:02.000Z"
            },
            "childObjectWithDates": {
                "_lastModifiedDateTime": {
                    date_key: "1980-12-14T15:01:02.000Z"
                },
                "grandChildObjectWithDate": {
                    "notDate1": 123,
                    "notDate2": "abc",
                    "grandChildDate1": {
                        date_key: "2019-12-14T15:01:02.000Z"
                    }
                },
                "childDate": {
                    date_key: "2018-12-14T15:01:02.000Z"
                },
                "arrayWithDates": [
                    789,
                    "xyz",
                    {date_key: "2010-12-14T15:01:02.000Z"},
                    [
                        {date_key: "2011-12-14T15:01:02.000Z"},
                        "qwerty"
                    ],
                    {
                        "grandChildDate3": {date_key: "2012-12-14T15:01:02.000Z"},
                        "_lastModifiedDateTime": {date_key: "1995-12-14T15:01:02.000Z"}
                    }
                ]
            }
        }
        self.assertEqual(json.dumps(expected_wrapped_object), json.dumps(test_object))

    def test_ignores_last_modified_date(self):
        test_string = json.dumps({"_lastModifiedDateTime": "2001-12-14T15:01:02.000+0000"})
        test_object = json.loads(test_string)

        DateWrapper.process_object(test_object, False)
        self.assertEqual(test_string, json.dumps(test_object))

    def test_wraps_common_dates(self):
        test_object = {
            "_lastModifiedDateTime": "2001-12-14T15:01:02.000+0000",
            "createdDateTime": "2001-12-01T15:01:02.000+0000",
            "_removedDateTime": "2001-12-02T15:01:02.000+0000",
            "_archivedDateTime": "2001-12-03T15:01:02.000+0000"
        }

        expected_string = json.dumps({
            "_lastModifiedDateTime": {"$date": "2001-12-14T15:01:02.000Z"},
            "createdDateTime": {"$date": "2001-12-01T15:01:02.000Z"},
            "_removedDateTime": {"$date": "2001-12-02T15:01:02.000Z"},
            "_archivedDateTime": {"$date": "2001-12-03T15:01:02.000Z"}
        })

        DateWrapper.process_object(test_object)
        self.assertEqual(expected_string, json.dumps(test_object))

    def test_non_utc(self):
        test_object = {"dateTime": "2001-12-01T15:01:02.000+0100"}
        expected_string = json.dumps({"dateTime": {"$date": "2001-12-01T14:01:02.000Z"}})

        DateWrapper.process_object(test_object)
        self.assertEqual(expected_string, json.dumps(test_object))

    def test_rewraps_mongo_dates(self):
        test_object = {"dateTime": {"$date": "2001-12-01T15:01:02.000+0000"}}
        expected_string = json.dumps({"dateTime": {"$date": "2001-12-01T15:01:02.000Z"}})

        DateWrapper.process_object(test_object)
        self.assertEqual(expected_string, json.dumps(test_object))

    def test_wraps_id_dates(self):
        test_object = {"_id": {
            "_lastModifiedDateTime": "2001-12-14T15:01:02.000+0000",
            "createdDateTime": "2001-12-01T15:01:02.000+0000",
            "_removedDateTime": "2001-12-02T15:01:02.000+0000",
            "_archivedDateTime": "2001-12-03T15:01:02.000+0000",
            "someOtherDate": "1990-12-02T15:01:02.000+0000"
        }}
        expected_string = json.dumps({"_id": {
            "_lastModifiedDateTime": {"$date": "2001-12-14T15:01:02.000Z"},
            "createdDateTime": {"$date": "2001-12-01T15:01:02.000Z"},
            "_removedDateTime": {"$date": "2001-12-02T15:01:02.000Z"},
            "_archivedDateTime": {"$date": "2001-12-03T15:01:02.000Z"},
            "someOtherDate": {"$date": "1990-12-02T15:01:02.000Z"}
        }})

        DateWrapper.process_object(test_object)
        self.assertEqual(expected_string, json.dumps(test_object))

    def test_should_wrap_all_dates(self):
        date_one = "2019-12-14T15:01:02.000Z"
        date_two = "2018-12-14T15:01:02.000Z"
        date_three = "2017-12-14T15:01:02.000Z"
        date_four = "2016-12-14T15:01:02.000Z"
        date_key = "$date"

        decrypted_object = {
            "_id": {"test_key_a": "test_value_a", "test_key_b": "test_value_b"},
            "_lastModifiedDateTime": date_one,
            "createdDateTime": date_two,
            "_removedDateTime": date_three,
            "_archivedDateTime": date_four
        }

        expected_record = json.dumps({
            "_id": {"test_key_a": "test_value_a", "test_key_b": "test_value_b"},
            "_lastModifiedDateTime": {date_key: date_one},
            "createdDateTime": {date_key: date_two},
            "_removedDateTime": {date_key: date_three},
            "_archivedDateTime": {date_key: date_four}
        })

        DateWrapper.process_object(decrypted_object)
        self.assertEqual(expected_record, json.dumps(decrypted_object))

    def test_should_not_wrap_dates_in_broader_text(self):
        date_one = "2019-12-14T15:01:02.000Z"
        date_two = "2018-12-14T15:01:02.000Z"
        date_three = "2017-12-14T15:01:02.000Z"
        date_four = "2016-12-14T15:01:02.000Z"
        date_key = "$date"

        decrypted_object = {
            "_id": {"test_key_a": "test_value_a", "test_key_b": "test_value_b"},
            "_lastModifiedDateTime": date_one,
            "createdDateTime": date_two,
            "_removedDateTime": date_three,
            "bodyOfText": f"{date_four} This text starts with a date, but is not a date",
            "bodyOfText2": f"This text ends with a date, but is not a date {date_four}",
            "bodyOfText3": f"This text includes a date, {date_four}, but is not a date",
        }

        expected_record = json.dumps({
            "_id": {"test_key_a": "test_value_a", "test_key_b": "test_value_b"},
            "_lastModifiedDateTime": {date_key: date_one},
            "createdDateTime": {date_key: date_two},
            "_removedDateTime": {date_key: date_three},
            "bodyOfText": f"{date_four} This text starts with a date, but is not a date",
            "bodyOfText2": f"This text ends with a date, but is not a date {date_four}",
            "bodyOfText3": f"This text includes a date, {date_four}, but is not a date",
        })

        DateWrapper.process_object(decrypted_object)
        self.assertEqual(expected_record, json.dumps(decrypted_object))

    def test_should_format_all_unwrapped_dates(self):
        date_one = "2019-12-14T15:01:02.000+0000"
        date_two = "2018-12-14T15:01:02.000+0000"
        date_three = "2017-12-14T15:01:02.000+0000"
        date_four = "2016-12-14T15:01:02.000+0000"
        date_key = "$date"
        decrypted_record = {
            "_id": {"test_key_a": "test_value_a", "test_key_b": "test_value_b"},
            "_lastModifiedDateTime": date_one,
            "createdDateTime": date_two,
            "_removedDateTime": date_three,
            "_archivedDateTime": date_four
        }
        formatted_date_one = "2019-12-14T15:01:02.000Z"
        formatted_date_two = "2018-12-14T15:01:02.000Z"
        formatted_date_three = "2017-12-14T15:01:02.000Z"
        formatted_date_four = "2016-12-14T15:01:02.000Z"
        expected_record = {
            "_id": {"test_key_a": "test_value_a", "test_key_b": "test_value_b"},
            "_lastModifiedDateTime": {date_key: formatted_date_one},
            "createdDateTime": {date_key: formatted_date_two},
            "_removedDateTime": {date_key: formatted_date_three},
            "_archivedDateTime": {date_key: formatted_date_four},
        }

        DateWrapper.process_object(decrypted_record)
        self.assertEqual(json.dumps(expected_record), json.dumps(decrypted_record))

    def test_should_keep_dates_within_wrapper(self):
        date_one = "2019-12-14T15:01:02.000Z"
        date_two = "2018-12-14T15:01:02.000Z"
        date_three = "2017-12-14T15:01:02.000Z"
        date_four = "2016-12-14T15:01:02.000Z"
        date_key = "$date"
        decrypted_record = {
            "_id": {"test_key_a": "test_value_a", "test_key_b": "test_value_b"},
            "_lastModifiedDateTime": {"$date": date_one},
            "createdDateTime": {"$date": date_two},
            "_removedDateTime": {"$date": date_three},
            "_archivedDateTime": {"$date": date_four}
        }
        expected_record = {
            "_id": {"test_key_a": "test_value_a", "test_key_b": "test_value_b"},
            "_lastModifiedDateTime": {date_key: date_one},
            "createdDateTime": {date_key: date_two},
            "_removedDateTime": {date_key: date_three},
            "_archivedDateTime": {date_key: date_four},
        }

        DateWrapper.process_object(decrypted_record)
        self.assertEqual(json.dumps(expected_record), json.dumps(decrypted_record))

    def test_should_format_all_wrapped_dates(self):
        date_one = "2019-12-14T15:01:02.000+0000"
        date_two = "2018-12-14T15:01:02.000+0000"
        date_three = "2017-12-14T15:01:02.000+0000"
        date_four = "2016-12-14T15:01:02.000+0000"
        decrypted_record = {
            "_id": {"test_key_a": "test_value_a", "test_key_b": "test_value_b"},
            "_lastModifiedDateTime": {"$date": date_one},
            "createdDateTime": {"$date": date_two},
            "_removedDateTime": {"$date": date_three},
            "_archivedDateTime": {"$date": date_four}
        }
        formatted_date_one = "2019-12-14T15:01:02.000Z"
        formatted_date_two = "2018-12-14T15:01:02.000Z"
        formatted_date_three = "2017-12-14T15:01:02.000Z"
        formatted_date_four = "2016-12-14T15:01:02.000Z"
        dateKey = "$date"
        expected_record = {
            "_id": {"test_key_a": "test_value_a", "test_key_b": "test_value_b"},
            "_lastModifiedDateTime": {dateKey: formatted_date_one},
            "createdDateTime": {dateKey: formatted_date_two},
            "_removedDateTime": {dateKey: formatted_date_three},
            "_archivedDateTime": {dateKey: formatted_date_four},
        }
        DateWrapper.process_object(decrypted_record)
        self.assertEqual(json.dumps(expected_record), json.dumps(decrypted_record))

    def test_should_allow_for_missing_created_removed_and_archived_dates(self):
        date_one = "2019-12-14T15:01:02.000Z"
        decrypted_record = {
            "_id": {"test_key_a": "test_value_a", "test_key_b": "test_value_b"},
            "_lastModifiedDateTime": date_one
        }
        expected_record = {
            "_id": {"test_key_a": "test_value_a", "test_key_b": "test_value_b"},
            "_lastModifiedDateTime": {"$date": date_one}
        }

        DateWrapper.process_object(decrypted_record)
        self.assertDictEqual(expected_record, decrypted_record)

    def test_should_allow_for_empty_created_removed_and_archived_dates(self):
        date_one = "2019-12-14T15:01:02.000Z"
        decrypted_record = {
            "_id": {"test_key_a": "test_value_a", "test_key_b": "test_value_b"},
            "_lastModifiedDateTime": date_one,
            "createdDateTime": "",
            "_removedDateTime": "",
            "_archivedDateTime": ""
        }
        expected_record = {
            "_id": {"test_key_a": "test_value_a", "test_key_b": "test_value_b"},
            "_lastModifiedDateTime": {"$date": date_one},
            "createdDateTime": "",
            "_removedDateTime": "",
            "_archivedDateTime": ""
        }

        DateWrapper.process_object(decrypted_record)
        self.assertDictEqual(expected_record, decrypted_record)

    def test_should_allow_for_null_created_removed_and_archived_dates(self):
        date_one = "2019-12-14T15:01:02.000Z"
        decrypted_record = {
            "_id": {"test_key_a": "test_value_a", "test_key_b": "test_value_b"},
            "_lastModifiedDateTime": date_one,
            "createdDateTime": None,
            "_removedDateTime": None,
            "_archivedDateTime": None
        }
        expected_record = {
            "_id": {"test_key_a": "test_value_a", "test_key_b": "test_value_b"},
            "_lastModifiedDateTime": {"$date": date_one},
            "createdDateTime": None,
            "_removedDateTime": None,
            "_archivedDateTime": None
        }

        DateWrapper.process_object(decrypted_record)
        self.assertDictEqual(expected_record, decrypted_record)

    def test_should_create_last_modified_if_missing(self):
        mock_message = json.dumps({
            "message": {
                "db": "db",
                "collection": "collection",
                "dbObject": None

            }
        })
        epoch = "1980-01-01T00:00:00.000Z"

        decrypted_record = "{}"

        expected_record = {
            "_lastModifiedDateTime": {"$date": epoch}
        }
        message = UCMessage(mock_message)
        message.set_decrypted_message(decrypted_record)
        message.validate()
        self.assertDictEqual(expected_record, json.loads(message.decrypted_record))


class TestDateHelper(TestCase):
    def test_should_convert_incoming_to_outgoing(self):
        date_one = "2019-12-14T15:01:02.000+0000"
        expected = "2019-12-14T15:01:02.000Z"

        actual = DateHelper.from_incoming_format(date_one).to_outgoing_format()
        self.assertEqual(expected, actual)

    def test_should_not_change_date_already_in_outgoing_format(self):
        date_one = "2019-12-14T15:01:02.000Z"
        expected = "2019-12-14T15:01:02.000Z"

        actual = DateHelper.from_incoming_format(date_one).to_outgoing_format()
        self.assertEqual(expected, actual)

    def test_should_change_positive_offset_to_utc(self):
        date_one = "2019-12-14T15:01:02.000+0100"
        expected = "2019-12-14T14:01:02.000Z"

        actual = DateHelper.from_incoming_format(date_one).to_outgoing_format()
        self.assertEqual(expected, actual)

    def test_should_change_negative_offset_to_utc(self):
        date_one = "2019-12-14T15:01:02.000-0100"
        expected = "2019-12-14T16:01:02.000Z"

        actual = DateHelper.from_incoming_format(date_one).to_outgoing_format()
        self.assertEqual(expected, actual)


class TestUCMessageSanitise(TestCase):
    def test_should_remove_chars_in_all_collections(self):
        mock_message = json.dumps({
            "message": {
                "db": "db",
                "collection": "collection",
                "dbObject": None

            }
        })
        decrypted_record = json.dumps({"fieldA": "a$\u0000", "_archivedDateTime": "b", "_archived": "c"})
        expected = {"fieldA": "ad_", "_removedDateTime": "b", "_removed": "c"}

        uc_message = UCMessage(mock_message)
        uc_message.set_decrypted_message(decrypted_record)
        uc_message.sanitise()

        self.assertDictEqual(expected, json.loads(uc_message.decrypted_record))
