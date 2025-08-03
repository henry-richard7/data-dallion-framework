from jsonpath_ng import parse


class JsonDataMapper:
    """
    A class to map and extract data from JSON using JSONPath expressions defined in a mapping.

    This class takes a JSON data structure and a mapping dictionary where values are JSONPath expressions.
    It extracts values from the JSON data based on these expressions and transforms the result into a list of dictionaries,
    suitable for use with DataFrame creation or database insertion.
    """

    def __init__(self, mapping: dict, json_data):
        """
        Initialize the JsonDataMapper with mapping rules and JSON data.

        Args:
            mapping (Dict[str, str]): Dictionary mapping output keys to JSONPath expressions.
            json_data (Any): JSON-like data (e.g., dict or list) to be parsed.
        """
        self.mapping = mapping
        self.json_data = json_data

    def convert_to_dict(self, data) -> list[dict]:
        """
        Convert a dictionary of lists into a list of dictionaries.

        This method aligns values by index. If a list is shorter than others, the last value is repeated.

        Args:
            data (Dict[str, List]): A dictionary where each key maps to a list of extracted values.

        Returns:
            List[Dict]: A list of dictionaries, each representing one row of aligned data.

        Examples:
            >>> data = {"name": ["Alice", "Bob"], "age": ["30", "25"]}
            >>> self.convert_to_dict(data)
            [{'name': 'Alice', 'age': 30}, {'name': 'Bob', 'age': 25}]
        """

        max_length = max(len(values) for values in data.values())
        result = []
        for i in range(max_length):
            item = {}
            for key, values in data.items():
                if i < len(values):
                    value = values[i]
                else:
                    value = values[-1]

                try:
                    item[key] = int(value)
                except ValueError:
                    item[key] = value
            result.append(item)
        return result

    def get_mapped_data(self) -> list[dict]:
        """
        Extract and transform data from JSON based on the provided mapping.

        Uses JSON Path expressions in the mapping to extract values from the JSON data.
        Converts the result into a list of dictionaries.

        Returns:
            List[Dict]: A list of dictionaries containing mapped and structured data.

        Examples:
            >>> mapping = {"name": "person.name", "age": "person.age"}
            >>> json_data = {"person": {"name": "Alice", "age": "30"}}
            >>> mapper = JsonDataMapper(mapping, json_data)
            >>> mapper.get_mapped_data()
            [{'name': 'Alice', 'age': 30}]
        """
        parsing_results = dict()
        for j_key, j_value in self.mapping.items():
            temp_list = list()
            jsonpath_expression = parse(j_value)

            for match in jsonpath_expression.find(self.json_data):
                temp_list.append(match.value)
            parsing_results[j_key] = temp_list

        parsed_data = self.convert_to_dict(parsing_results)
        return parsed_data