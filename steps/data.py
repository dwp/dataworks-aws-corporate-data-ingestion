import json
from copy import deepcopy
from dataclasses import dataclass
from typing import Dict


@dataclass
class ConfigurationFile:
    """Class for keeping configuration read from terraform-interpolated configuration file."""

    s3_corporate_bucket: str
    s3_published_bucket: str
    dks_decrypt_endpoint: str
    dks_max_retries: str


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
    def dbobject(self) -> str:
        return self.message_json["message"]["dbObject"]

    @property
    def last_modified(self) -> (str, str):
        record_type = self.message_json.get("message", {}).get("@type", "NOT_SET")

        epoch = "1980-01-01T00:00:00.000+0000"
        kafka_timestamp = self.message_json.get("timestamp", "")
        last_modified_timestamp = self.message_json.get("message", {}).get("_lastModifiedDateTime", "")
        created_timestamp = self.message_json.get("message", {}).get("createdDateTime", "")

        if record_type == "MONGO_DELETE" and kafka_timestamp != "" and isinstance(kafka_timestamp, str):
            return kafka_timestamp, "kafkaMessageDateTime"

        if last_modified_timestamp != "":
            return last_modified_timestamp, "_lastModifiedDateTime"

        if created_timestamp != "":
            return created_timestamp, "createdDateTime"

        return epoch, "epoch"

    def get_decrypted_uc_message(self, decrypted_dbobject: str):
        """Returns new UCMessage object, replacing encrypted dbObject attribute with the decrypted
        dbObject provided.  Removes encryption materials.
        """
        json_message = deepcopy(self.message_json)
        json_message["message"].update([("dbObject", decrypted_dbobject)])
        json_message["message"].pop("encryption", None)
        return UCMessage(json.dumps(json_message))
