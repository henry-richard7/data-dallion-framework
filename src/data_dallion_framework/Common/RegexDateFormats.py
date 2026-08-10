def get_date_regex(qc_param: str) -> str:
    """
    Return a regex pattern corresponding to the specified date/time format.

    This function maps common date and datetime format strings to their corresponding
    regular expression patterns for validation or parsing purposes. It supports multiple
    datetime formats including ISO 8601 variants, US date formats, and custom timestamp formats.
    It primarily supports Spark / Java SimpleDateFormat patterns, but retains support for legacy
    Python strptime formats.

    Supported Formats (Spark Format Standard) and Their Regex Equivalents:
        - `yyyy-MM-dd'T'HH:mm:ssZ` → ISO 8601 basic datetime with offset (e.g., 2026-08-10T06:33:06+0000)
        - `yyyy` → 4-digit year (e.g., 2026)
        - `yyyy-MM-dd'T'HH:mm:ss.SSSZ` → ISO 8601 datetime with milliseconds and offset (e.g., 2026-08-10T06:33:06.123+0000)
        - `MM/dd/yyyy` → U.S. date format (e.g., 08/10/2026)
        - `yyyy-MM-dd HH:mm:ss` → Standard SQL date-time format (e.g., 2026-08-10 06:33:06)
        - `yyyy-MM-dd'T'HH:mm:ss.SSS'Z'` → UTC Zulu time format (e.g., 2026-08-10T06:33:06.123Z)
        - `yyyyMMdd` → Compact 8-digit date (e.g., 20260810)
        - `yyyy-MM-dd HH:mm:ss.SSSSSSSXXX` → Extended .NET datetime format

    Args:
        qc_param (str): The date format string to match against known patterns.

    Returns:
        str: A regex pattern that matches the provided date/time format.
             Defaults to `MM/dd/yyyy` if no match is found.

    Examples:
        >>> get_date_regex("yyyy-MM-dd'T'HH:mm:ssZ")
        '([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\+[0-9]{4})'

        >>> get_date_regex("MM/dd/yyyy")
        '([0-9]{2}/[0-9]{2}/[0-9]{4})'
    """
    date_formats_map = {
        # Spark / Java style formats (Standard)
        r"yyyy-MM-dd'T'HH:mm:ssZ": r"([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\+[0-9]{4})",
        "yyyy": r"([0-9]{4})",
        r"yyyy-MM-dd'T'HH:mm:ss.SSSZ": r"([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}\+[0-9]{4})",
        "MM/dd/yyyy": r"([0-9]{2}/[0-9]{2}/[0-9]{4})",
        "yyyy-MM-dd HH:mm:ss": r"([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})",
        "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'": r"([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}Z)",
        "yyyyMMdd": r"([0-9]{4})([0-9]{2})([0-9]{2})",
        "yyyy-MM-dd HH:mm:ss.SSSSSSSXXX": r"([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{1,7}? [+-][0-9]{2}:[0-9]{2})",

        # Legacy Python formats support (fallback)
        r"%Y-%m-%dT%H:%M:%S+0000": r"([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\+[0-9]{4})",
        "%Y": r"([0-9]{4})",
        r"%Y-%m-%dT%H:%M:%S.%f+0000": r"([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}\+[0-9]{4})",
        "%Y-%m-%dT%H:%M:%S.000Z": r"([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}Z)",
        "MM/DD/YYYY": r"([0-9]{2}/[0-9]{2}/[0-9]{4})",
        "YYYY-MM-DD HH24:MI:SS": r"([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})",
        "YYYYMMDD": r"([0-9]{4})([0-9]{2})([0-9]{2})",
        "yyyy-MM-dd HH:mm:ss.nnnnnnn {+|-}hh:mm": r"([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{1,7}? [+-][0-9]{2}:[0-9]{2})",
    }

    return date_formats_map.get(qc_param, r"([0-9]{2}/[0-9]{2}/[0-9]{4})")
