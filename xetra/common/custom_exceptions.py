"""Custom Exceptions"""


class WrongFormatException(Exception):
    """
    WrongFormatException class

    Exception that can be raised when the format type 
    given as parameter is not supported
    """

class WrongMetaFileException(Exception):
    """
    WrongMetaFIleException class

    Exception that can be raised when the incorrect metafile format is found
    """