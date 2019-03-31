from pathlib import Path
from serverdb_log_parser.database.db_schema import FileVersionData, SyncMode, Modification, LogData, UnparsedData
import re
import os
import datetime
import hashlib
from serverdb_log_parser.rop.result import Result


class Parser:

    def __init__(self, folder_path: str):
        self._data: dict = {str(LogData): [], str(UnparsedData): []}
        self.folder_path = folder_path
        self.__sync_log_entry_pattern = re.compile(
            "^(?P<syncdatetime>.*?) INFO \[(?P<database_name>.*?)\] \((?P<sync_mode>.*?)\) Author\[(?P<author>.*?)\] Mod:\'(?P<modification_type>.*?)\' Doc ID:(?P<doc_id>.*)"
        )

        self._sync_log_entry_error_pattern = re.compile(
            "^(?P<syncdatetime>.*?) ERROR (?P<error_message>.*)"
        )

        self._sync_log_entry_sync_from_id_pattern = re.compile(
            "(Starting to sync|Starting at|Sync from Master done|Sync from Master started|Sync into Master|Stopping the Sync|Ending at)"
        )

        self.__sync_log_entry_pattern_skipped = re.compile(
            "^(?P<syncdatetime>.*?) INFO \[(?P<database_name>.*?)\] \((?P<sync_mode>.*?)\) \[Skipped\].(?P<error_message>.*?) Doc ID:(?P<doc_id>.*)"
        )

    def parse(self):
        path = Path(self.folder_path)
        folders = [x for x in path.iterdir() if x.is_dir()]
        for folder in folders:
            # Required folder structure
            username: str = os.path.basename(folder)
            for file in Path.glob(folder, 'serverdb_*.log'):
                file_hash = hashlib.md5(open(file, 'rb').read()).hexdigest()

                entries = FileVersionData.objects(
                    file_name=os.path.basename(file),
                    file_hash=file_hash,
                    user_name=username, is_parsing_complete=True)
                if any(entries):
                    print(f"File:{file} already parsed. Skipping!!")
                    continue

                file_version_data = FileVersionData(user_name=username,
                                                    file_name=os.path.basename(
                                                        file),
                                                    file_path=str(os.path.abspath(file)), date_parsed=datetime.datetime.today(),
                                                    file_hash=file_hash, is_parsing_complete=False)
                file_version_data.save()
                try:
                    with open(file, 'r') as reader:
                        for line in reader:
                            self.match_sync_entry(line, file_version_data) \
                                .on_failure(lambda: self.match_sync_skipped_entry(line, file_version_data))\
                                .on_failure(lambda: self.match_sync_error_entry(line, file_version_data)) \
                                .on_failure(lambda: self.unparsed_data(line, file_version_data))
                    print(f"Finished parsing file: {file}")
                    # self._session.commit()
                    file_version_data.is_parsing_complete = True
                    file_version_data.save()

                    print(f"Finished updating FileVersionData")
                    print("Done!")
                except:
                    print(f"Failed to save data for file: {file}")
                    raise

    def match_sync_entry(self, line: str, file_version_data: FileVersionData) -> Result:
        try:
            match = self.__sync_log_entry_pattern.match(line)
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
                               file_name=file_version_data.file_name, \
                               file_path=file_version_data.file_path, \
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
            print(msg)
            return Result.fail(msg)

    def match_sync_skipped_entry(self, line: str, file_version_data: FileVersionData) -> Result:
        match = self.__sync_log_entry_pattern_skipped.match(line)
        if match == None:
            return Result.fail('log entry for skipped not matched!')
        syncdatetime = datetime.datetime.strptime(
            match.group('syncdatetime'), "%Y-%m-%d %H:%M:%S.%f")
        error_text = match.group('error_message')
        sync_mode = SyncMode.SYNCFROM if match.group(
            'sync_mode') == 'SYNC FROM' else SyncMode.SYNCINTO
        log_data = LogData(file_version_data=file_version_data,
                           file_name=file_version_data.file_name,
                           file_path=file_version_data.file_path,
                           user_name=file_version_data.user_name,
                           date_time=syncdatetime,
                           sync_mode=sync_mode,
                           is_skipped=True,
                           error_text=error_text)
        log_data.save()
        return Result.ok()

    def match_sync_error_entry(self, line: str, file_version_data: FileVersionData) -> Result:
        match = self._sync_log_entry_error_pattern.match(line)
        if match == None:
            return Result.fail('log entry for error not matched!')
        syncdatetime = datetime.datetime.strptime(
            match.group('syncdatetime'), "%Y-%m-%d %H:%M:%S.%f")
        error_text = match.group('error_message')

        log_data = LogData(file_version_data=file_version_data,
                           file_name=file_version_data.file_name,
                           file_path=file_version_data.file_path,
                           user_name=file_version_data.user_name,
                           date_time=syncdatetime,
                           is_error=True,
                           error_text=error_text)
        log_data.save()
        return Result.ok()

    def unparsed_data(self, line: str, file_version_data: FileVersionData) -> Result:
        match = self._sync_log_entry_sync_from_id_pattern.findall(line)
        if any(match):
            return Result.ok()
        unparsed_data = UnparsedData(
            text=line, file_version_data=file_version_data)
        unparsed_data.save()
        return Result.ok()

    def _convert_string_to_modification_type(self, value: str) -> Modification:
        try:
            return {
                'M': Modification.MODIFIED,
                'N': Modification.NEW,
                'D': Modification.DELETE
            }[value]
        except:
            print(f"Error in converting to modification type. Value : {value}")
            raise
