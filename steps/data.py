import datetime as dt
import json
from copy import deepcopy
from dataclasses import dataclass
from typing import Dict, List


@dataclass
class ConfigurationFile:
    """Class for keeping configuration read from terraform-interpolated configuration file."""

    s3_corporate_bucket: str
    s3_published_bucket: str
    dks_decrypt_endpoint: str
    dks_max_retries: str
    extra_python_files: list


@dataclass
class Configuration:
    """Class for keeping application configuration."""

    correlation_id: str
    run_timestamp: str  # format: "%Y-%m-%d_%H-%M-%S"
    start_date: str  # format: "%Y-%m-%d"
    end_date: str  # format: "%Y-%m-%d"
    collection_names: List[str]
    override_ingestion_class: str
    source_s3_prefix: str
    destination_s3_prefix: str
    concurrency: int
    configuration_file: ConfigurationFile
    export_date: str = ""  # format: "%Y-%m-%d"


@dataclass(eq=True, frozen=True)
class EncryptionMaterials:
    encryptionKeyId: str
    encryptedEncryptionKey: str
    initialisationVector: str
    keyEncryptionKeyId: str


class UCMessage:
    _py_date_format = "%Y-%m-%dT%H:%M:%S.%f%z"
    KEY_AUDIT_CONTEXT = "context"
    KEY_AUDIT_TYPE = "auditType"
    KEY_AUDIT_EVENT = "AUDIT_EVENT"
    KEY_TIME_STAMP = "TIME_STAMP"
    KEY_TIME_STAMP_ORIG = "TIME_STAMP_ORIG"

    def __init__(self, kafka_message_string: str, collection_name=None):
        """Collection name optional, in format `db:collection`"""
        self._kafka_message_string = kafka_message_string
        self.kafka_message_json = json.loads(self._kafka_message_string)
        self.db, self.collection = self._get_db_collection_name(collection_name)
        self.encrypted_db_object = self.kafka_message_json["message"]["dbObject"]
        self.decrypted_record = None

    @property
    def id(self) -> str:
        return self.kafka_message_json["message"]["_id"]

    @property
    def encryption_materials(self) -> EncryptionMaterials:
        return EncryptionMaterials(**self.kafka_message_json["message"]["encryption"])

    def _get_db_collection_name(self, collection_name=None):
        message_element = self.kafka_message_json.get("message", {})
        db = message_element.get("db")
        collection = message_element.get("collection")
        if not db or not collection:
            db, collection = collection_name.split(":")
        return db, collection

    def set_decrypted_message(self, decrypted_dbobject: str):
        """Returns new UCMessage object, replacing encrypted dbObject attribute with the decrypted
        dbObject provided.  Removes encryption materials.
        """
        self.decrypted_record = decrypted_dbobject
        return self

    def transform(self):
        """Only applies to data.businessAudit messages
            - adds data to the context element
            - unwraps the context element
        """
        if self.db == "data" and self.collection == "businessAudit":
            last_modified_timestamp = self.kafka_message_json.get("message").get("_lastModifiedDateTime", "")
            # Test for json primitives per HTME
            if isinstance(last_modified_timestamp, (int, dict, float, complex, bool, str)):
                last_modified_timestamp = str(last_modified_timestamp)
            else:
                last_modified_timestamp = ""

            decrypted_db_object = json.loads(self.decrypted_record)
            context_element = decrypted_db_object.get(self.KEY_AUDIT_CONTEXT)
            audit_type = decrypted_db_object.get(self.KEY_AUDIT_TYPE)
            if not audit_type or not context_element:
                raise Exception("Audit elements not found (`context` or `auditType`")
            else:
                context_element[self.KEY_AUDIT_EVENT] = audit_type
                context_element[self.KEY_TIME_STAMP] = last_modified_timestamp
                context_element[self.KEY_TIME_STAMP_ORIG] = last_modified_timestamp
            self.decrypted_record = json.dumps(context_element)
        return self

