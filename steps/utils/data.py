import json
from dataclasses import dataclass
from typing import Dict


@dataclass(eq=True, frozen=True)
class EncryptionMaterials:
    keyEncryptionKeyId: str
    initialisationVector: str
    encryptedEncryptionKey: str


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
