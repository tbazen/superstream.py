import io
import sys

from superstream.constants import EnvVars


class SuperstreamStd:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(SuperstreamStd, cls).__new__(cls)
        return cls._instance

    def __init__(self, disable_stdout: bool = False, disable_stderr: bool = False):
        if not hasattr(self, "_initialized"):
            disable_stdout, disable_stderr = SuperstreamStd.check_stdout_env_var()
            self._is_stdout_disabled = disable_stdout
            self._is_std_err_disabled = disable_stderr

            self.stdout = sys.stdout
            self.stderr = sys.stderr

            self.superstream_stdout = io.StringIO()
            self.superstream_stderr = io.StringIO()

            # sys.stdout = self.superstream_stdout
            # sys.stderr = self.superstream_stderr

            self._initialized = True

    def write(self, *args, **kwargs):
        if self._is_stdout_disabled:
            return
        self.stdout.write(*args, **kwargs)

    def writelines(self, *args, **kwargs):
        if self._is_stdout_disabled:
            return
        self.stdout.writelines(*args, **kwargs)

    def error(self, *args, **kwargs):
        if self._is_std_err_disabled:
            return
        self.stderr.write(*args, **kwargs)

    def errorlines(self, *args, **kwargs):
        if self._is_std_err_disabled:
            return
        self.stderr.writelines(*args, **kwargs)

    def flush(self):
        self.stdout.flush()
        self.stderr.flush()

        sys.stdout = self.original_stdout
        sys.stderr = self.original_stderr

    @staticmethod
    def check_stdout_env_var():
        if EnvVars.SUPERSTREAM_DEBUG:
            is_stdout_disabled = True
            is_std_err_disabled = True
        else:
            is_stdout_disabled = False
            is_std_err_disabled = False
        return is_stdout_disabled, is_std_err_disabled
