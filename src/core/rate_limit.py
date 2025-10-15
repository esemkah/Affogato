from slowapi import Limiter
from slowapi.util import get_remote_address


def get_request_key(request):
    # Allow tests to set a unique key via header to isolate rate limits per TestClient
    try:
        test_id = request.headers.get("x-test-id")
        if test_id:
            return test_id
    except Exception:
        pass
    return get_remote_address(request)


# Shared limiter instance used across the app and endpoints
limiter = Limiter(key_func=get_request_key)
