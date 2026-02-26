def export_headers_map(headers: list[str], prefix: str) -> dict:
    out = {
        p.removeprefix(prefix): p for p in headers
    }
    return out
