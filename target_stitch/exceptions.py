class TargetStitchException(Exception):
    '''A known exception for which we don't need to print a stack trace'''
    pass

class BatchTooLargeException(TargetStitchException):
    '''Exception for when the records and schema are so large that we can't
    create a batch with even one record.'''
    pass
