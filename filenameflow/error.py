"""Error Class"""


class FileNameFlowError(Exception):
    """Basic exception for FileNameFlow module"""


class FileNameFlowDataError(FileNameFlowError):
    """Error in exeution the pipeline"""


class FileNameFlowAssert(FileNameFlowError):
    """Mostly this is internal error"""
