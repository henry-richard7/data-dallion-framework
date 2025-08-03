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
    DATE_TIME_FORMAT_1 = r"%Y-%m-%dT%H:%M:%S+0000"
    DATE_TIME_FORMAT_1_REGEX = (
        r"([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\+[0-9]{4})"
    )

    DATE_TIME_FORMAT_2 = "%Y"
    DATE_TIME_FORMAT_2_REGEX = r"([0-9]{4})"

    DATE_TIME_FORMAT_3 = r"%Y-%m-%dT%H:%M:%S.%f+0000"
    DATE_TIME_FORMAT_3_REGEX = (
        r"([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}\+[0-9]{4})"
    )

    DATE_TIME_FORMAT_4 = "MM/DD/YYYY"
    DATE_TIME_FORMAT_4_REGEX = r"([0-9]{2}/[0-9]{2}/[0-9]{4})"

    DATE_TIME_FORMAT_5 = "YYYY-MM-DD HH24:MI:SS"
    DATE_TIME_FORMAT_5_REGEX = "([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})"

    DATE_TIME_FORMAT_6 = "%Y-%m-%dT%H:%M:%S.000Z"
    DATE_TIME_FORMAT_6_REGEX = (
        r"([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}Z)"
    )

    DATE_TIME_FORMAT_7 = "YYYYMMDD"
    DATE_TIME_FORMAT_7_REGEX = r"([0-9]{4})([0-9]{2})([0-9]{2})"

    DATE_TIME_FORMAT_8 = "yyyy-MM-dd HH:mm:ss.nnnnnnn {+|-}hh:mm"
    DATE_TIME_FORMAT_8_REGEX = r"([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{1,7}? [+-][0-9]{2}:[0-9]{2})"

    DATE_TIME_FORMAT_DEFAULT = "MM/DD/YYYY"
    DATE_TIME_FORMAT_DEFAULT_REGEX = r"([0-9]{2}/[0-9]{2}/[0-9]{4})"

    # INTEGER_REGEX = "(^-?\d+$)"
    # DECIMAL_REGEX = "(^-?\d*[.]?\d+$)"

    if qc_param == DATE_TIME_FORMAT_1:
        regex = DATE_TIME_FORMAT_1_REGEX
    elif qc_param == DATE_TIME_FORMAT_2:
        regex = DATE_TIME_FORMAT_2_REGEX
    elif qc_param == DATE_TIME_FORMAT_3:
        regex = DATE_TIME_FORMAT_3_REGEX
    elif qc_param == DATE_TIME_FORMAT_4:
        regex = DATE_TIME_FORMAT_4_REGEX
    elif qc_param == DATE_TIME_FORMAT_5:
        regex = DATE_TIME_FORMAT_5_REGEX
    elif qc_param == DATE_TIME_FORMAT_6:
        regex = DATE_TIME_FORMAT_6_REGEX
    elif qc_param == DATE_TIME_FORMAT_7:
        regex = DATE_TIME_FORMAT_7_REGEX
    elif qc_param == DATE_TIME_FORMAT_8:
        regex = DATE_TIME_FORMAT_8_REGEX
    else:
        regex = DATE_TIME_FORMAT_DEFAULT_REGEX
        return regex