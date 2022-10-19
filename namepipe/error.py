class NamePipeError(Exception):
    """ Basic exception for namepipe module """
    pass


class NamePipeDataError(NamePipeError):
    """ Raise this when the name-based concept cannot work """
    pass


class NamePipeAssert(NamePipeError):
    """ Mostly this is internal error """
    pass


class NamePipeNotImplemented(NamePipeError):
    """ Not implementated code """
    pass
