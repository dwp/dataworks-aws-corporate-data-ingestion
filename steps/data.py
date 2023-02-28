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

    def __init__(self, kafka_message_string: str):
        self._kafka_message_string = kafka_message_string
        self._kafka_message_json = json.loads(self._kafka_message_string)
        self.decrypted_record = None

    @property
    def message_json(self) -> Dict:
        return self._kafka_message_json

    @property
    def id(self) -> str:
        return self.message_json["message"]["_id"]

    @property
    def encryption_materials(self) -> EncryptionMaterials:
        return EncryptionMaterials(**self.message_json["message"]["encryption"])

    @property
    def dbobject(self) -> str:
        return self.message_json["message"]["dbObject"]

    def set_decrypted_message(self, decrypted_dbobject: str):
        """Returns new UCMessage object, replacing encrypted dbObject attribute with the decrypted
        dbObject provided.  Removes encryption materials.
        """
        self.decrypted_record = decrypted_dbobject
        return self
