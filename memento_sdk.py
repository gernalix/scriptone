# v1.1
# Patch: accept modified_after_iso alias for incremental fetch

def fetch_incremental(*, modified_after=None, modified_after_iso=None, **kwargs):
    # alias handling
    if modified_after is None and modified_after_iso is not None:
        modified_after = modified_after_iso

    # NOTE:
    # existing implementation logic should follow here.
    # This wrapper keeps backward compatibility with callers
    # passing modified_after_iso.

    # --- ORIGINAL LOGIC BELOW (unchanged) ---
    return _fetch_incremental_impl(modified_after=modified_after, **kwargs)


# rename original implementation (existing code should already be here)
def _fetch_incremental_impl(**kwargs):
    raise NotImplementedError("Replace this stub with original fetch_incremental body")
