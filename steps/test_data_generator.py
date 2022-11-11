import base64
import binascii

from dks import EncryptionMaterials
from Crypto import Random
from Crypto.Util import Counter
from Crypto.Cipher import AES


def generate_test_encryption_material(index: int) -> (int, EncryptionMaterials):
    return index, EncryptionMaterials(
        keyEncryptionKeyId=f"KeyEncryptionKeyId-{index}",
        initialisationVector=f"initialisationVector-{index}",
        encryptedEncryptionKey=f"encryptedEncryptionKey-{index}",
        encryptionKeyId="",
    )


def generate_encrypted_string(input_string: str) -> (str, EncryptionMaterials):
    datakey_bytes = Random.get_random_bytes(16)
    iv_bytes = Random.new().read(AES.block_size)
    input_bytes = input_string.encode("utf8")
    iv_int = int(binascii.hexlify(iv_bytes), 16)

    counter = Counter.new(AES.block_size * 8, initial_value=iv_int)
    aes = AES.new(datakey_bytes, AES.MODE_CTR, counter=counter)
    ciphertext = aes.encrypt(input_bytes)

    iv_ascii = base64.b64encode(iv_bytes).decode("ascii")
    datakey_ascii = base64.b64encode(datakey_bytes).decode("ascii")
    ciphertext_ascii = base64.b64encode(ciphertext).decode("ascii")

    return ciphertext_ascii, EncryptionMaterials(
        "not_encrypted", iv_ascii, datakey_ascii, ""
    )
