class PiDALError(Exception):
    pass


class Warning(Warning, PiDALError):
    pass


class Error(PiDALError):
    pass


class ClientPackageExceedsLength(Error):
    pass


class OperationalError(Error):
    pass


class InternalError(Error):
    pass


class ProgrammingError(Error):
    pass
