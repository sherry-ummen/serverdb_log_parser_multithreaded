from serverdb_log_parser_multithreaded.rop.result import Result
import hashlib
import datetime
import os
import re
import threading
from pathlib import Path
from serverdb_log_parser_multithreaded.database.db_schema import FileVersionData, SyncMode, Modification, LogData, UnparsedData

Sync_log_entry_pattern = re.compile(
    "^(?P<syncdatetime>.*?) INFO \[(?P<database_name>.*?)\] \((?P<sync_mode>.*?)\) Author\[(?P<author>.*?)\] Mod:\'(?P<modification_type>.*?)\' Doc ID:(?P<doc_id>.*)"
)
Sync_log_entry_error_pattern = re.compile(
    "^(?P<syncdatetime>.*?) ERROR (?P<error_message>.*)"
)

Sync_log_entry_to_be_no_parsed = re.compile(
    "(Starting to sync|Starting at|Sync from Master done|Sync from Master started|Sync into Master|Stopping the Sync|Ending at)"
)

Sync_log_entry_pattern_skipped = re.compile(
    "^(?P<syncdatetime>.*?) INFO \[(?P<database_name>.*?)\] \((?P<sync_mode>.*?)\) \[Skipped\].(?P<error_message>.*?) Doc ID:(?P<doc_id>.*)"
)


class Parser:

    def __init__(self, file_path: str, user_name: str):
        self._data: dict = {str(LogData): [], str(UnparsedData): []}
        self.file_path = file_path
        self.user_name = user_name

    async def parse(self):
        self.worker_id = threading.current_thread().name
        file_hash = hashlib.md5(open(self.file_path, 'rb').read()).hexdigest()

        entries = FileVersionData.objects(
            file_name=os.path.basename(self.file_path),
            file_hash=file_hash,
            user_name=self.user_name, is_parsing_complete=True)
        if any(entries):
            self.print(f"File:{self.file_path} already parsed. Skipping!!")
            return

        file_version_data = FileVersionData(user_name=self.user_name,
                                            file_name=os.path.basename(
                                                self.file_path),
                                            file_path=str(os.path.abspath(self.file_path)), date_parsed=datetime.datetime.today(),
                                            file_hash=file_hash, is_parsing_complete=False)
        file_version_data.save()
        try:
            with open(self.file_path, 'r') as reader:
                for line in reader:
                    self.match_sync_entry(line, file_version_data) \
                        .on_failure(lambda: self.match_sync_skipped_entry(line, file_version_data))\
                        .on_failure(lambda: self.match_sync_error_entry(line, file_version_data)) \
                        .on_failure(lambda: self.unparsed_data(line, file_version_data))
            self.print(f"Finished parsing file: {self.file_path}")
            # self._session.commit()
            file_version_data.is_parsing_complete = True
            file_version_data.save()

            self.print(f"Finished updating FileVersionData")
            self.print("Done!")
        except:
            self.print(f"Failed to save data for file: {self.file_path}")
            raise

    def print(self, message):
        print(f"Worker Id: {self.worker_id} {message}")

    def match_sync_entry(self, line: str, file_version_data: FileVersionData) -> Result:
        try:
            match = Sync_log_entry_pattern.match(line)
            if match == None:
                return Result.fail('log entry not matched!')
            dbname = match.group('database_name')
            syncdatetime = datetime.datetime.strptime(
                match.group('syncdatetime'), "%Y-%m-%d %H:%M:%S.%f")
            sync_mode = SyncMode.SYNCFROM if match.group(
                'sync_mode') == 'SYNC FROM' else SyncMode.SYNCINTO
            author = match.group('author')
            modification = self._convert_string_to_modification_type(
                match.group('modification_type'))
            document_id = match.group('doc_id')
            is_skipped = False
            is_error = False
            log_data = LogData(file_version_data=file_version_data, \
                               # sync_batch_id=self._batch_data[dbname], \
                               user_name=file_version_data.user_name, \
                               date_time=syncdatetime, \
                               database_name=dbname, \
                               sync_mode=sync_mode, \
                               author=author, \
                               modification_type=modification, \
                               document_id=document_id, \
                               is_skipped=is_skipped, \
                               is_error=is_error)
            log_data.save()
            return Result.ok()
        except Exception as e:
            msg = f"Failed to parse line:\n{line}\n\nFile:{file_version_data.file_name}\n\nException:{e}\n\n"
            self.print(msg)
            return Result.fail(msg)

    def match_sync_skipped_entry(self, line: str, file_version_data: FileVersionData) -> Result:
        match = Sync_log_entry_pattern_skipped.match(line)
        if match == None:
            return Result.fail('log entry for skipped not matched!')
        syncdatetime = datetime.datetime.strptime(
            match.group('syncdatetime'), "%Y-%m-%d %H:%M:%S.%f")
        error_text = match.group('error_message')
        sync_mode = SyncMode.SYNCFROM if match.group(
            'sync_mode') == 'SYNC FROM' else SyncMode.SYNCINTO
        log_data = LogData(file_version_data=file_version_data,
                           user_name=file_version_data.user_name,
                           date_time=syncdatetime,
                           sync_mode=sync_mode,
                           is_skipped=True,
                           error_text=error_text)
        log_data.save()
        return Result.ok()

    def match_sync_error_entry(self, line: str, file_version_data: FileVersionData) -> Result:
        match = Sync_log_entry_error_pattern.match(line)
        if match == None:
            return Result.fail('log entry for error not matched!')
        syncdatetime = datetime.datetime.strptime(
            match.group('syncdatetime'), "%Y-%m-%d %H:%M:%S.%f")
        error_text = match.group('error_message')

        log_data = LogData(file_version_data=file_version_data,
                           user_name=file_version_data.user_name,
                           date_time=syncdatetime,
                           is_error=True,
                           error_text=error_text)
        log_data.save()
        return Result.ok()

    def unparsed_data(self, line: str, file_version_data: FileVersionData) -> Result:
        match = Sync_log_entry_to_be_no_parsed.findall(line)
        if any(match):
            return Result.ok()
        unparsed_data = UnparsedData(
            text=line, file_version_data=file_version_data)
        unparsed_data.save()
        return Result.ok()

    def _convert_string_to_modification_type(self, value: str) -> str:
        if value == 'M':
            return Modification.MODIFIED
        if value == 'N':
            return Modification.NEW
        if value == 'D':
            return Modification.DELETE
        return Modification.UNKNOWN
