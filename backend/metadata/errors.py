from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException

from metadata.operations import OperationError


def error_response(status: int, code: str, message: str, details: dict | None = None):
    return JSONResponse(
        status_code=status,
        content={"error": {"code": code, "message": message, "details": details or {}}},
    )


async def validation_error(request: Request, exc: RequestValidationError):
    # Do not echo request bodies or raw exception contexts (may contain secrets).
    details = {"fields": [{"loc": list(e["loc"]), "type": e["type"]} for e in exc.errors()]}
    return error_response(422, "VALIDATION_ERROR", "Dữ liệu yêu cầu không hợp lệ.", details)


async def http_error(request: Request, exc: HTTPException):
    code = "NOT_FOUND" if exc.status_code == 404 else "INVALID_REQUEST"
    return error_response(exc.status_code, code, "Không thể xử lý yêu cầu.")


async def operation_error(request: Request, exc: OperationError):
    return error_response(exc.status_code, exc.code, str(exc))
