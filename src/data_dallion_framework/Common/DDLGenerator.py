import os
from jinja2 import Environment, FileSystemLoader
from typing import List, Optional, Dict

class DDLGenerator:
    """
    Utility class for generating DDL scripts for Delta tables using Jinja2 templates.
    """
    def __init__(self):
        """
        Initializes the DDLGenerator by loading the Jinja2 template.

        Raises:
            FileNotFoundError: If the expected template directory does not exist.
        """
        # Resolve the template directory relative to this file's location
        # src/data_dallion_framework/Common/DDLGenerator.py -> src/data_dallion_framework/templates
        current_dir = os.path.dirname(os.path.abspath(__file__))
        template_dir = os.path.join(os.path.dirname(current_dir), "templates")
        
        if not os.path.exists(template_dir):
            raise FileNotFoundError(f"Template directory not found: {template_dir}")
            
        self.jinja_env = Environment(
            loader=FileSystemLoader(template_dir),
            trim_blocks=True,
            lstrip_blocks=True
        )
        self.template = self.jinja_env.get_template("DDL_Template.jinja")

    def generate_ddl(
        self, 
        env: str, 
        table_name: str, 
        columns: List[Dict[str, str]], 
        partition_by: Optional[List[str]] = None, 
        location: Optional[str] = None, 
        is_external: bool = False, 
        tblproperties: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Renders the DDL script using the Jinja2 template.

        Args:
            env (str): The environment prefix (e.g., 'dev', 'prod').
            table_name (str): The name of the table.
            columns (List[Dict[str, str]]): List of columns, each containing 'name' and 'type'.
            partition_by (List[str], optional): List of columns to partition by.
            location (str, optional): The storage location for external tables.
            is_external (bool): Whether to create an EXTERNAL table. Defaults to False.
            tblproperties (Dict[str, str], optional): Spark table properties.

        Returns:
            str: The rendered DDL SQL script.
        """
        return self.template.render(
            env=env,
            table_name=table_name,
            columns=columns,
            partition_by=partition_by,
            location=location,
            is_external=is_external,
            tblproperties=tblproperties
        )
