import datetime as dt
import json
from dataclasses import dataclass
from typing import Dict, List
import re

JSON_PRIMITIVES = (int, dict, float, complex, bool, str)


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

    KEY_ARCHIVED_DT = "_archivedDateTime"
    KEY_LAST_MODIFIED_DT = "_lastModifiedDateTime"
    KEY_CREATED_DT = "createdDateTime"
    KEY_REMOVED_DT = "_removedDateTime"

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
            if isinstance(last_modified_timestamp, JSON_PRIMITIVES):
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

    def validate(self):
        db_object = json.loads(self.decrypted_record)

        # Wraps the last modified, creates from other dates if not present
        prioritised_last_modified = self._get_last_modified(db_object)
        if prioritised_last_modified:
            formatted_date = DateHelper.from_incoming_format(prioritised_last_modified).to_outgoing_format()
            db_object[self.KEY_LAST_MODIFIED_DT] = {"$date": formatted_date}

        # Wraps all dates in the record
        DateWrapper.process_object(db_object)

        # Older records have Archived-datetime, which was renamed in this pipeline to the same
        # as Removed-datetime.  If Removed-datetime is already present, then renaming Archived-datetime
        # would introduce duplicate, and is not necessary
        if self.KEY_ARCHIVED_DT in db_object and self.KEY_REMOVED_DT in db_object:
            db_object.pop(self.KEY_ARCHIVED_DT)

        id_element = db_object.get("_id")
        if isinstance(id_element, JSON_PRIMITIVES):
            new_id_element = {"$oid": str(id_element)}
            db_object["_id"] = new_id_element

        self.decrypted_record = json.dumps(db_object)
        return self

    def sanitise(self):
        self.sanitise_collection_specific()
        db_object = self.decrypted_record
        db_object = (
            db_object
            .replace("$", "d_")
            .replace("\\u0000", "")
            .replace("_archivedDateTime", "_removedDateTime")
            .replace("_archived", "_removed")
        )
        self.decrypted_record = db_object
        return self

    def sanitise_collection_specific(self):
        require_specific_sanitising = (
            ("penalties-and-deductions", "sanction"),
            ("core", "healthAndDisabilityDeclaration"),
            ("accepted-data", "healthAndDisabilityCircumstances"),
        )
        for db, collection in require_specific_sanitising:
            if self.db == db and self.collection == collection:
                raise NotImplementedError("This collection requires specific sanitising which has not yet been "
                                          "implemented.  Check HTME SanitisationProcessor for details")

    @classmethod
    def _get_last_modified(cls, dbobject: dict):
        """Gets last modified in following priority order:
            $.message._lastModifiedDateTime
            > $.message._removedDateTime
            > $.message.createdDateTime
            > epoch (="1980-01-01T00:00:00.000Z")
        """
        epoch = "1980-01-01T00:00:00.000Z"
        last_modified = cls._retrieve_date_time_element(dbobject, cls.KEY_LAST_MODIFIED_DT)
        removed = cls._retrieve_date_time_element(dbobject, cls.KEY_REMOVED_DT)
        created = cls._retrieve_date_time_element(dbobject, cls.KEY_CREATED_DT)

        if last_modified:
            return last_modified
        elif removed:
            return removed
        elif created:
            return created
        else:
            return epoch

    @staticmethod
    def _retrieve_date_time_element(json_object, key):
        """If datetime is wrapped in mongo {$date: ""} object, returns the inner string"""
        date_element = json_object.get(key)
        if date_element:
            if isinstance(date_element, dict):
                date = date_element.get("$date")
                if date:
                    return str(date)
            else:
                return str(date_element)
        return ""


class DateWrapper:
    DATE_KEY = "$date"

    @classmethod
    def process_object(cls, json_object: Dict, include_last_modified=True):
        """Iterates through a json_object (dictionary) finding string elements
            that match regex in the DateHelper.  If a date is already part of
            a mongo date object, the date is formatted.  If not part of a mongo
            date object, the date is wrapped and formatted in place

            { "created_date": "{date in format A}"}
            becomes
            { "created_date": {"$date": "{date in format B}"}
        """
        for key in json_object.keys():
            if include_last_modified or key != "_lastModifiedDateTime":
                cls.process_element(json_object, key, json_object[key])

    @classmethod
    def process_list(cls, json_list: List):
        for i in range(0, len(json_list)):
            value = json_list[i]
            if isinstance(value, dict):
                cls.process_object(value)
            elif isinstance(value, list):
                cls.process_list(value)
            elif isinstance(value, str) and DateHelper.is_date_string(value):
                json_list[i] = {"$date": DateHelper.from_incoming_format(value).to_outgoing_format()}

    @classmethod
    def process_string(cls, parent, key, json_element: str):
        # Replace a simple date string with a date object {"$date": "original value re-formatted"}
        if DateHelper.is_date_string(json_element):
            parent[key] = {"$date": DateHelper.from_incoming_format(json_element).to_outgoing_format()}

    @classmethod
    def process_mongo_date_object(cls, json_element: dict):
        date_str = json_element[cls.DATE_KEY]
        json_element[cls.DATE_KEY] = DateHelper.from_incoming_format(date_str).to_outgoing_format()

    @classmethod
    def process_element(cls, parent, key, json_element):
        if cls.is_mongo_date_object(json_element):
            cls.process_mongo_date_object(json_element)
        elif isinstance(json_element, dict):
            cls.process_object(json_element)
        elif isinstance(json_element, list):
            cls.process_list(json_element)
        elif isinstance(json_element, str):
            cls.process_string(parent, key, json_element)

    @classmethod
    def is_mongo_date_object(cls, json_element) -> bool:
        return (
                json_element
                and isinstance(json_element, dict)
                and len(json_element.keys()) == 1
                and json_element.get(cls.DATE_KEY, None)
                and isinstance(json_element.get(cls.DATE_KEY), JSON_PRIMITIVES)
        )


class DateHelper:
    KAFKA_INCOMING_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"
    KAFKA_OUTGOING_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
    incoming_matcher = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}\+\d{4}")
    outgoing_matcher = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z")
    date_matcher = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}((Z)|(\+\d{4}))")

    def __init__(self, datetime: dt.datetime):
        self.dt_object = datetime.astimezone(dt.timezone.utc)

    @classmethod
    def is_date_string(cls, possible_date: str):
        return bool(cls.date_matcher.match(possible_date))

    @classmethod
    def from_incoming_format(cls, kafka_timestamp):
        dt_object = dt.datetime.strptime(kafka_timestamp, cls.KAFKA_INCOMING_FORMAT)
        return cls(dt_object)

    def to_incoming_format(self):
        # python provides 6 digits for %f, here it's truncated to 3
        return self.dt_object.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + self.dt_object.strftime("%z")

    def to_outgoing_format(self):
        # python provides 6 digits for %f, here it's truncated to 3
        return self.dt_object.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    def to_timestamp(self):
        return str(round(1000 * self.dt_object.timestamp()))
