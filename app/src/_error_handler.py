from pydantic import BaseModel, StrictStr, field_validator
from _logger import get_logger

logger = get_logger() 

#TODO: Need to understand how i'm gonna handle the errors
class ErrorHandler(BaseModel):
    errType: Exception
    errMsg: StrictStr

    @field_validator("errType")
    def validate_errType(cls, value):
        if not isinstance(value, Exception):
            logger.error("TypeError: Handler is not an instance of Exception class")
        return value
