from re import match as regex_pattern_matches


def validate_pattern(file_pattern: str, file_name: str, custom: bool = False) -> bool:
    """
    Validate whether a given file name matches a specified file naming pattern.

    This function supports dynamic date-based patterns (`YYYY`, `YYYYMM`, `YYYYMMDD`) and wildcards (`*`).
    If `custom=True`, it treats the pattern as a raw regular expression and matches directly.

    Supported replacements:
        - `YYYYMMDD` → `[0-9]{8}` (e.g., 20250405)
        - `YYYYMM`   → `[0-9]{6}` (e.g., 202504)
        - `YYYY`     → `[0-9]{4}` (e.g., 2025)

    Wildcard support:
        - `*` is replaced with `.*` in regex for flexible matching

    Args:
        file_pattern (str): The pattern to validate against. May contain date placeholders or be a regex.
        file_name (str): The actual file name to validate.
        custom (bool, optional): If True, treat `file_pattern` as a full regex string. Defaults to False.

    Returns:
        bool: True if the file name matches the pattern, False otherwise.

    Examples:
        >>> validate_pattern("data_YYYYMMDD.csv", "data_20250405.csv")
        True

        >>> validate_pattern("report_*.csv", "report_20250405.csv")
        True

        >>> validate_pattern(r"data_\\d{8}\\.csv", "data_20250405.csv", custom=True)
        True
    """
    if custom:
        return bool(regex_pattern_matches(file_pattern, file_name))

    regex_pattern = (
        file_pattern.replace("YYYYMMDD", "[0-9]{8}")
        .replace("YYYYMM", "[0-9]{6}")
        .replace("YYYY", "[0-9]{4}")
        .replace("*", ".*")
    )

    return bool(regex_pattern_matches(regex_pattern, file_name))
