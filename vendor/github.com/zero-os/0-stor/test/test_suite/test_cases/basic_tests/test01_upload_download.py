from test_suite.test_cases.testcases_base import TestcasesBase
from random import randint


class UploadDownload(TestcasesBase):
    def test001_upload_file(self):
        print('\n')
        self.default_writer()
        self.get_uploaded_files_keys()
        self.default_reader()
        self.get_download_files_paths_from_threads()
        result = self.check_md5(self.downloaded_files_info)
        if result:
            self.utiles.print_pretty_dict(dict=result, color='red')
            self.assertFalse(result)

    def test002_download_file(self):
        pass

    def test003_upload_file_with_small_chunck_size(self):
        pass

    def test004_upload_file_with_exact_chunck_size(self):
        pass

    def test005_upload_file_with_greater_chunck_size(self):
        pass

    def test006_upload_file_with_extremly_small_size(self):
        pass

    def test007_upload_non_exist_file(self):
        pass

    def test009_upload_file_with_write_permission(self):
        pass

    def test010_upload_file_with_read_permission(self):
        pass

    def test011_upload_file_with_read_write_permission(self):
        pass

    def test012_upload_file_with_admin_permission(self):
        pass

    def test013_download_non_exist_file(self):
        pass

    def test014_download_file_with_write_permission(self):
        pass

    def test015_download_file_with_read_permission(self):
        pass

    def test015_download_file_with_admin_permission(self):
        pass
