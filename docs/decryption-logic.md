# Decryption Logic

<!-- TOC -->
* [Decryption Logic](#decryption-logic)
    * [Purpose](#purpose)
    * [Summary](#summary)
    * [MessageCryptoHelper](#messagecryptohelper)
    * [DKSService](#dksservice)
<!-- TOC -->

### Purpose
This documentation was written to outline the message decryption process & 
interaction with DKS

### Summary
Data in the corporate store bucket, received over kafka, is encrypted:
- The **files** are encrypted with KMS keys.  This is handled using IAM and 
transparent to the cluster.
- The files contain records, and each record has an encrypted 'dbObject'.  The
materials used to encrypt the dbObject are included in the message, and are
themselves encrypted.

The DKSService and MessageCryptoHelper in `steps/dks.py` handle interaction 
with DKS (Data Key Service) and decryption of the 'dbObject'.


### MessageCryptoHelper
The MessageCryptoHelper is used to decrypt the 'dbObject' in a UC Message. It 
uses the DKSService to obtain a decrypted datakey with which the 'dbObject' in 
a specific message can be decrypted.

The most important method is `.decrypt_dbObject()`.  It takes a UCMessage object
and dks_key_cache as parameters.  It does the following:
- Extracts EncryptionMaterials from the UCMessage
- Uses DKSService to obtain a decrypted datakey for the dbObject
- Decrypts the dbObject using initialisation vector and plaintext datakey

The `.decrypt_string()` method is called by `.decrypt_dbObject()`

### DKSService
We must interact with DKS to obtain a decrypted datakey. Datakeys can be used
for more than one message and so, in order to reduce the load we place on DKS,
caching has been implemented.

The most important method is `decrypt_data_key()`.  This is called by the
MessageCryptoHelper.  It does the following:
1. Checks the cache and returns plaintext key if present; otherwise
2. Uses the dks `/decrypt` endpoint to obtain a decrypted datakey
3. Stores in cache, and returns to MessageCryptoHelper
