import re

class QueryParser:
    def __init__(self):
        # Regular expressions to match different query types
        self.create_pattern = r"CREATE TABLE (\w+)\s+SET\s+\((\w+\s+PRIMARY KEY,?\s*|\w+(?:,?\s+|$))*(\w+\s+FOREIGN KEY REF\s+\w+\$\w+,?\s*)?\);"
        self.import_pattern = r"IMPORT TABLE (\w+) FROM (.+)\s+SET\s+\((\w+\s+PRIMARY KEY,?\s*|\w+(?:,?\s+|$))*(\w+\s+FOREIGN KEY REF\s+\w+\$\w+,?\s*)?\);"
        self.drop_pattern = r"DROP TABLE (\w+);"

        self.projection_pattern = r"FIND \{(.+?)\} IN TABLE (\w+);"
        self.filtering_pattern = r"FIND \{(.+?)\}\s*IF \((.+?)\)\s*IN TABLE (\w+)\s*(?:AND (\w+) JOIN BY (\w+))?;"
        self.join_pattern = r"JOIN (\w+) AND (\w+) BY (\w+) ON=\'(\w+)\';"
        self.grouping_pattern = r"FIND (\w+)\.(\w+)\(\) GROUP BY (\w+)\s*IN TABLE (\w+)(?: AND (\w+) JOIN BY (\w+))?;"
        self.aggregation_pattern = r"FIND (\w+)\.(\w+)\(\)(?: IF \((.+?)\))? IN TABLE (\w+)(?: AND (\w+) BY (\w+));"
        self.ordering_pattern = r"FIND\s+{([^}]+)}\s+IN\s+TABLE\s+(\w+)\s+(?:ORDER BY\s+(\w+)\s+(DESC|ASC)\s+IN\s+TABLE\s+(\w+))?(?:\s+AND\s+(\w+)\s+JOIN BY\s+(\w+))?;"

        self.inserting_pattern = r"INSERT INTO (\w+)\(([^)]+)\) VALUES\(([^)]+)\);"
        self.deleting_pattern = r"DELETE FROM\s*(\w+)\s*IF\s*(.*?);"
        self.updating_pattern = r"UPDATE (\w+)\s*SET (\w+)\s*=\s*(\w+)\s*IF\s*(.*?);"

    def parse(self, query):
        # Match the query to the appropriate pattern
        if re.match(self.create_pattern, query, re.IGNORECASE):
            match = re.match(self.create_pattern, query)
            table_name = match.group(1)
            columns_info = match.group(2)

            if match.group(3):
                columns = columns_info.split(',')
                column_details = []
                for col in columns:
                    col = col.strip()
                    column_name, *constraints = col.split()
                    column_detail = {"column_name": column_name}
                    
                    if "PRIMARY KEY" in constraints:
                        column_detail["primary_key"] = True
                    
                    if "FOREIGN KEY REF" in col:
                        fk_info = re.search(r"FOREIGN\s+KEY\s+REF\s+(\w+)\$(\w+)", col)
                        if fk_info:
                            column_detail["foreign_key"] = {"table": fk_info.group(1), "column": fk_info.group(2)}
                    
                    column_details.append(column_detail)

            return {
                "query_type": "create",
                "table_name": table_name,
                "columns": column_details
            }
        
        elif re.match(self.import_pattern, query, re.IGNORECASE):
            match = re.match(self.import_pattern, query)
            table_name = match.group(1)
            file_path = match.group(2)
            columns_info = match.group(3)

            if match.group(3):
                columns = columns_info.split(',')
                column_details = []
                for col in columns:
                    col = col.strip()
                    column_name, *constraints = col.split()
                    column_detail = {"column_name": column_name}
                    
                    if "PRIMARY KEY" in constraints:
                        column_detail["primary_key"] = True
                    
                    if "FOREIGN KEY REF" in col:
                        fk_info = re.search(r"FOREIGN\s+KEY\s+REF\s+(\w+)\$(\w+)", col)
                        if fk_info:
                            column_detail["foreign_key"] = {"table": fk_info.group(1), "column": fk_info.group(2)}
                    
                    column_details.append(column_detail)

            return {
                "query_type": "import",
                "table_name": table_name,
                "file_path": file_path,
                "columns": column_details
            }

        elif re.match(self.drop_pattern, query, re.IGNORECASE):
            match = re.match(self.drop_pattern, query)
            return  {
                "query_type": "drop",
                "table_name": match.group(1)
            }

        elif re.match(self.projection_pattern, query, re.IGNORECASE):
            match = re.match(self.projection_pattern, query)
            return {
                "query_type": "projection",
                "columns": [col.strip() for col in match.group(1).split(", ")],
                "table_name": match.group(2),
            }
        
        elif re.match(self.filtering_pattern, query, re.IGNORECASE):
            match = re.match(self.filtering_pattern, query)
            columns = match.group(1).split(", ")
            condition = match.group(2)
            table_name = match.group(3)
            if match.group(4) and match.group(5):
                join_table = match.group(4)
                join_column = match.group(5)
            else:
                join_table = None
                join_column = None
            return {
                "query_type": "filtering",
                "columns": columns,
                "condition": condition,
                "table_name": table_name,
                "join_table": join_table,
                "join_column": join_column,
            }
        
        elif re.match(self.join_pattern, query, re.IGNORECASE):
            match = re.match(self.join_pattern, query)
            return {
                "query_type": "join",
                "left_table": match.group(1),
                "right_table": match.group(2),
                "join_column": match.group(3),
                "join_type": match.group(4),
            }
        
        elif re.match(self.grouping_pattern, query, re.IGNORECASE):
            match = re.match(self.grouping_pattern, query)
            if match.group(5) and match.group(6):
                join_table = match.group(5)
                join_column = match.group(6)
            else:
                join_table = None
                join_column = None
            return {
                "query_type": "grouping",
                "column": match.group(1),
                "function_name": match.group(2),
                "group_by_column": match.group(3),
                "table_name": match.group(4),
                "join_table": join_table,
                "join_column": join_column,
            }
        
        elif re.match(self.aggregation_pattern, query, re.IGNORECASE):
            match = re.match(self.aggregation_pattern, query)
            if match.group(5) and match.group(6):
                join_table = match.group(5)
                join_column = match.group(6)
            else:
                join_table = None
                join_column = None
            return {
                "query_type": "aggregation",
                "column": match.group(1),
                "method": match.group(2),
                "condition": match.group(3),
                "table_name": match.group(4),
                "join_table": join_table,
                "join_column": join_column,
            }
        elif re.match(self.ordering_pattern, query, re.IGNORECASE):
            match = re.match(self.ordering_pattern, query)
            return {
                "query_type": "ordering",
                "columns": [col.strip() for col in match.group(1).split(", ")],
                "table_name": match.group(2),
                "order_by_column": match.group(3),
                "method": match.group(4), 
                "join_table": match.group(6),
                "join_column": match.group(7),
            }
        elif re.match(self.inserting_pattern, query, re.IGNORECASE):
            match = re.match(self.inserting_pattern, query)
            return {
                "query_type": "inserting",
                "table_name": match.group(1),
                "columns": [col.strip() for col in match.group(2).split(", ")],
                "values": [value.strip() for value in match.group(3).split(", ")],
            }
        elif re.match(self.deleting_pattern, query, re.IGNORECASE):
            match = re.match(self.deleting_pattern, query)
            return {
                "query_type": "deleting",
                "table_name": match.group(1),
                "condition": match.group(2),
            }
        elif re.match(self.updating_pattern, query, re.IGNORECASE):
            match = re.match(self.updating_pattern, query)
            return {
                "query_type": "updating",
                "table_name": match.group(1),
                "set_column": match.group(2),
                "set_value": match.group(3),
                "condition": match.group(4),
            }
        else:
            return {"error": "Invalid query format! Please restate your request!"}
