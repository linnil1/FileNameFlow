class NamePipeError(Exception):
    """Basic exception for namepipe module"""


class NamePipeDataError(NamePipeError):
    """Raise this when the name-based concept cannot work"""


class NamePipeAssert(NamePipeError):
    """Mostly this is internal error"""


class NamePipeNotImplemented(NamePipeError):
    """Non-implementated error(Mostly related to kwargs in path)"""
