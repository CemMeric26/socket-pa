import hashlib
import datetime

def calculateMD5Checksum(data):
    """
    Calculates the MD5 checksum of the given data.
    """
    checksum = hashlib.md5()
    return checksum.digest()

def give_time():
    return datetime.datetime.utcnow().timestamp()

def control_checksum(data, checksum):
    """
    Calculates the MD5 checksum of the given data and compares it with the given checksum.
    """
    return calculateMD5Checksum(data) == checksum