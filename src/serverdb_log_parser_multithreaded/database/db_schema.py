from mongoengine import *


class SyncMode:
    SYNCFROM = "SYNCFROM"
    SYNCINTO = "SYNCINTO"


class Modification:
    MODIFIED = "MODIFIED"
    NEW = "NEW"
    DELETE = "DELETE"


class FileVersionData(Document):
    user_name = StringField(required=True)
    file_name = StringField(max_length=256)
    file_path = StringField(max_length=256)
    date_parsed = DateTimeField()
    file_hash = StringField(max_length=100)
    is_parsing_complete = BooleanField()


class LogData(Document):
    file_version_data = ReferenceField(FileVersionData),
    file_name = StringField(max_length=256),
    file_path = StringField(max_length=256),
    user_name = StringField(max_length=50)
    date_time = DateTimeField()
    database_name = StringField(max_length=50)
    sync_mode = StringField(max_length=20)
    author = StringField(max_length=50)
    modification_type = StringField(max_length=20)
    document_id = StringField(max_length=256)
    is_skipped = BooleanField()
    is_error = BooleanField()
    error_text = StringField(max_length=256)


class UnparsedData(Document):
    file_version_data = ReferenceField(FileVersionData)
    text = StringField(max_length=1024)
