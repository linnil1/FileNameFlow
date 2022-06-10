class NamePipeError(Exception):
    """ Basic exception for namepipe module """
    pass


class NamePipeDataError(NamePipeError):
    pass


class NamePipeAssert(NamePipeError):
    """ Mostly this is internal error """
    pass


class NamePipeNotImplemented(NamePipeError):
    pass
