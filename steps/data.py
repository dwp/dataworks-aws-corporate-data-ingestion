import json
from dataclasses import dataclass
from typing import Dict


@dataclass
class ConfigurationFile:
    """Class for keeping configuration read from terraform-interpolated configuration file."""

    s3_corporate_bucket: str
    s3_published_bucket: str
    dks_decrypt_endpoint: str


@dataclass
class Configuration:
    """Class for keeping application configuration."""

    correlation_id: str
    run_timestamp: str  # format: "%Y-%m-%d_%H-%M-%S"
    collection_name: str
    source_s3_prefix: str
    destination_s3_prefix: str
    configuration_file: ConfigurationFile


@dataclass(eq=True, frozen=True)
class EncryptionMaterials:
    encryptionKeyId: str
    encryptedEncryptionKey: str
    initialisationVector: str
    keyEncryptionKeyId: str


class UCMessage:
    def __init__(self, message_string: str):
        self._message_string = message_string
        self._message_json = json.loads(self._message_string)

    @property
    def message_json(self) -> Dict:
        return self._message_json

    @property
    def encryption_materials(self) -> EncryptionMaterials:
        return EncryptionMaterials(**self.message_json["message"]["encryption"])

    @property
    def encrypted_dbobject(self) -> str:
        return self.message_json["message"]["dbObject"]
