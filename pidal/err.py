class PiDALError(Exception):
    pass


class Warning(Warning, PiDALError):
    pass


class Error(PiDALError):
    pass


class ClientPackageExceedsLength(Error):
    pass
