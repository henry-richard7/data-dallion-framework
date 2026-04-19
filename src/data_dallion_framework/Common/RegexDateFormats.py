def get_date_regex(qc_param: str) -> str:
    """
    Return a regex pattern corresponding to the specified date/time format.

    This function maps common date and datetime format strings to their corresponding
    regular expression patterns for validation or parsing purposes. It supports multiple
    datetime formats including ISO 8601 variants, US date formats, and custom timestamp formats.

    Supported Formats and Their Regex Equivalents:
        - `%Y-%m-%dT%H:%M:%S+0000` → ISO 8601 basic datetime with offset
        - `%Y` → 4-digit year
        - `%Y-%m-%dT%H:%M:%S.%f+0000` → ISO 8601 datetime with milliseconds and offset
        - `MM/DD/YYYY` → U.S. date format
        - `YYYY-MM-DD HH24:MI:SS` → Standard SQL date-time format
        - `%Y-%m-%dT%H:%M:%S.000Z` → UTC Zulu time format
        - `YYYYMMDD` → Compact 8-digit date
        - `yyyy-MM-dd HH:mm:ss.nnnnnnn {+|-}hh:mm` → Extended .NET datetime format

    Args:
        qc_param (str): The date format string to match against known patterns.

    Returns:
        str: A regex pattern that matches the provided date/time format.
             Defaults to `MM/DD/YYYY` if no match is found.

    Examples:
        >>> get_date_regex("%Y-%m-%dT%H:%M:%S+0000")
        '([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\+[0-9]{4})'

        >>> get_date_regex("MM/DD/YYYY")
        '([0-9]{2}/[0-9]{2}/[0-9]{4})'
    """
    date_formats_map = {
        r"%Y-%m-%dT%H:%M:%S+0000": r"([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\+[0-9]{4})",
        "%Y": r"([0-9]{4})",
        r"%Y-%m-%dT%H:%M:%S.%f+0000": r"([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}\+[0-9]{4})",
        "MM/DD/YYYY": r"([0-9]{2}/[0-9]{2}/[0-9]{4})",
        "YYYY-MM-DD HH24:MI:SS": r"([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})",
        "%Y-%m-%dT%H:%M:%S.000Z": r"([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}Z)",
        "YYYYMMDD": r"([0-9]{4})([0-9]{2})([0-9]{2})",
        "yyyy-MM-dd HH:mm:ss.nnnnnnn {+|-}hh:mm": r"([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{1,7}? [+-][0-9]{2}:[0-9]{2})",
    }

    return date_formats_map.get(qc_param, r"([0-9]{2}/[0-9]{2}/[0-9]{4})")
