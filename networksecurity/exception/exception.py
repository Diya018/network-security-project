import sys

class NetworkSecurityException(Exception):
    def __init__(self, error_message, error_detail: sys):
        super().__init__(error_message)
        _, _, exc_tb = sys.exc_info()

        # Protect against NoneType
        if exc_tb is not None:
            self.line_number = exc_tb.tb_lineno
            self.file_name = exc_tb.tb_frame.f_code.co_filename
        else:
            self.line_number = None
            self.file_name = None

        self.error_message = f"error occured in python script name [{self.file_name}] line no [{self.line_number}] error message[{error_message}]"

    def __str__(self):
        return self.error_message
