import re

class QueryParser:
    def __init__(self):
        # database 
        self.show_database_pattern = r"SHOW DATABASE;"
        self.create_database_pattern = r"CREATE DATABASE (\w+) (Relational|NoSQL);"
        self.use_database_pattern = r"USE DATABASE (\w+) (Relational|NoSQL);"
        self.drop_database_pattern = r"DROP DATABASE (\w+) (Relational|NoSQL);"

        # table
        self.show_table_pattern = r"SHOW TABLES;"
        self.create_table_pattern = r"CREATE TABLE (\w+) SET \(([^)]+)\);"
        self.import_table_pattern = r"IMPORT TABLE (\w+) FROM (.+) SET \(([^)]+)\);"
        self.drop_table_pattern = r"DROP TABLE (\w+);"

        # operator
        self.projection_pattern = r"FIND \(([^)]+)\) IN TABLE (\w+);"
        self.filtering_pattern = r"FIND \(([^)]+)\) IF (.+?) IN TABLE (\w+);"
        self.join_pattern = r"FIND \(([^)]+)\) JOIN BY (.+?) ON (inner|outer|left|right);"
        self.group_agg_pattern = r"FIND (COUNT|SUM|MEAN|MIN|MAX)\((\w+)\) GROUP BY (\w+) IN TABLE (\w+);"
        self.ordering_pattern = r"FIND \(([^)]+)\) IN TABLE (\w+) ORDER BY (\w+) (DESC|ASC);"

        # modification
        self.inserting_pattern = r"INSERT INTO (\w+)\(([^)]+)\) VALUES \(([^)]+)\);"
        self.deleting_row_pattern = r"DELETE FROM\s*(\w+)\s*IF\s*(.*?);"
        self.deleting_column_pattern = r"DELETE \(([^)]+)\) FROM (\w+);"
        self.updating_pattern = r"UPDATE (\w+) SET \(([^)]+)\) IF ([^;]+);"

    def Parser(self, query):
        if re.match(self.show_database_pattern, query, re.IGNORECASE):
            match = re.match(self.show_database_pattern, query)
            return {
                "query_type": "show_database"
            }
        
        elif re.match(self.create_database_pattern, query, re.IGNORECASE):
            match = re.match(self.create_database_pattern, query)
            return {
                "query_type": "create_database",
                "database_name": match.group(1),
                "type_of_database": match.group(2).lower()
            }
        
        elif re.match(self.use_database_pattern, query, re.IGNORECASE):
            match = re.match(self.use_database_pattern, query)
            return {
                "query_type": "use_database",
                "database_name": match.group(1),
                "type_of_database": match.group(2).lower()
            } 
        
        elif re.match(self.drop_database_pattern, query, re.IGNORECASE):
            match = re.match(self.drop_database_pattern, query)
            return {
                "query_type": "drop_database",
                "database_name": match.group(1),
                "type_of_database": match.group(2).lower()
            }               

                   
        elif re.match(self.show_table_pattern, query, re.IGNORECASE):
            match = re.match(self.show_table_pattern, query)
            return  {
                "query_type": "show_table"
            }
        
        elif re.match(self.create_table_pattern, query, re.IGNORECASE):
            match = re.match(self.create_table_pattern, query)

            column = match.group(2)

            column_pattern = r"^\s*(\w+)"
            column_definitions = column.split(',')
            column_names = []
            for definition in column_definitions:
                column_match = re.match(column_pattern, definition.strip())
                if match:
                    column_names.append(column_match.group(1))

            column_pattern = r"^\s*(\w+)"
            column_match = re.match(column_pattern, column)

            primary_key_pattern = r"(\w+)\sPRIMARY KEY"
            primary_key_match = re.match(primary_key_pattern, column)
            if primary_key_match:
                primary_key = primary_key_match.group(1)
            else: 
                primary_key = ""
            
            foreign_key_pattern = r"(\w+) FOREIGN KEY REF (\w+)\$(\w+)"
            foreign_key_match = re.findall(foreign_key_pattern, column)
            foreign_key = []
            if foreign_key_match:
                for item in foreign_key_match:
                    foreign_key.append({
                            "self_column":item[0],
                            "ref_table":item[1],
                            "ref_column":item[2]})

            return {
                "query_type": "create_table",
                "table_name": match.group(1),
                "columns": column_names,
                "primary key": primary_key,
                "foreign key": foreign_key
            }
        
        elif re.match(self.import_table_pattern, query, re.IGNORECASE):
            match = re.match(self.import_table_pattern, query)

            if match.group(3): 
                column = match.group(3)

                primary_key_pattern = r"(\w+) PRIMARY KEY"
                primary_key_match = re.match(primary_key_pattern, column)

                if primary_key_match:
                    primary_key = primary_key_match.group(1)
                else: 
                    primary_key = ""
                
                foreign_key_pattern = r"(\w+) FOREIGN KEY REF (\w+)\$(\w+)"
                foreign_key_match = re.findall(foreign_key_pattern, column)

                foreign_key = []
                if foreign_key_match:
                    for item in foreign_key_match:
                        foreign_key.append({
                                "self_column":item[0],
                                "ref_table":item[1],
                                "ref_column":item[2]})
            
            
            return {
                "query_type": "import_table",
                "table_name": match.group(1),
                "file_path": match.group(2),
                "primary key": primary_key,
                "foreign key": foreign_key
            }

        elif re.match(self.drop_table_pattern, query, re.IGNORECASE):
            match = re.match(self.drop_table_pattern, query)
            return  {
                "query_type": "drop_table",
                "table_name": match.group(1)
            }


        
        elif re.match(self.projection_pattern, query, re.IGNORECASE):
            match = re.match(self.projection_pattern, query)
            return {
                "query_type": "projection",
                "table_name": match.group(2),
                "columns": [col.strip() for col in match.group(1).split(",")],
            }
        
        elif re.match(self.filtering_pattern, query, re.IGNORECASE):
            match = re.match(self.filtering_pattern, query)

            condition = match.group(2)
            condition_pattern = r'(\w+) (=|!=|<|>|>=|<=) [\"“]([^\"”]+)[\"”]'
            condition_match = re.match(condition_pattern, condition)

            return {
                "query_type": "filtering",
                "table_name": match.group(3),
                "columns": [col.strip() for col in match.group(1).split(",")],
                "condition": {
                    "variable":condition_match.group(1),
                    "method": condition_match.group(2),
                    "value": condition_match.group(3)
                }
            }
        
        elif re.match(self.join_pattern, query, re.IGNORECASE):
            match = re.match(self.join_pattern, query)
            
            column = match.group(1)
            column_pattern = r"(\w+)\$(\w+)"
            column_match = re.findall(column_pattern, column)
            selected_columns = []
            for item in column_match:
                selected_columns.append({"table":item[0], 
                                        "column":item[1]})
        
            join = match.group(2)
            join_pattern = r"(\w+)\$(\w+)\s*=\s*(\w+)\$(\w+)"
            join_match = re.match(join_pattern, join)

            return {
                "query_type": "join",
                "selected": selected_columns,
                "left_table": join_match.group(1),
                "left_column": join_match.group(2),
                "right_table": join_match.group(3),
                "right_column": join_match.group(3),
                "join_method": match.group(3),
            }
        
        elif re.match(self.group_agg_pattern, query, re.IGNORECASE):
            match = re.match(self.group_agg_pattern, query)
            return {
                "query_type": "group_agg",
                "table_name": match.group(4),
                "agg_column": match.group(2),
                "agg_method": match.group(1),
                "group_by_column": match.group(3)
            }
        
        elif re.match(self.ordering_pattern, query, re.IGNORECASE):
            match = re.match(self.ordering_pattern, query)
            return {
                "query_type": "ordering",
                "table_name": match.group(2),
                "columns": [col.strip() for col in match.group(1).split(",")],
                "order_by_column": match.group(3),
                "method": match.group(4)
            }

        
        elif re.match(self.inserting_pattern, query, re.IGNORECASE):
            match = re.match(self.inserting_pattern, query)
            return {
                "query_type": "inserting",
                "table_name": match.group(1),
                "columns": [col.strip() for col in match.group(2).split(",")],
                "values": [value.strip() for value in match.group(3).split(",")],
            }
        
        elif re.match(self.deleting_row_pattern, query, re.IGNORECASE):
            match = re.match(self.deleting_row_pattern, query)

            condition = match.group(2)
            condition_pattern = r'(\w+) (=|!=|<|>|>=|<=) [\"“]([^\"”]+)[\"”]'
            condition_match = re.match(condition_pattern, condition)

            return {
                "query_type": "deleting_row",
                "table_name": match.group(1),
                "condition":{
                    "variable":condition_match.group(1),
                    "method": condition_match.group(2),
                    "value": condition_match.group(3)
                },
            }
        
        elif re.match(self.deleting_column_pattern, query, re.IGNORECASE):
            match = re.match(self.deleting_column_pattern, query)
            return {
                "query_type": "deleting_column",
                "table_name": match.group(2),
                "columns": [col.strip() for col in match.group(1).split(",")],
            }
        
        elif re.match(self.updating_pattern, query, re.IGNORECASE):
            match = re.match(self.updating_pattern, query)

            target_condition =  match.group(2)
            target_condition_pattern = r'(\w+) = [\"“]([^\"”]+)[\"”]'
            target_match = re.match(target_condition_pattern, target_condition)

            condition = match.group(3)
            condition_pattern = r'(\w+) (=|!=|<|>|>=|<=) [\"“]([^\"”]+)[\"”]'
            condition_match = re.match(condition_pattern, condition)

            return {
                "query_type": "updating",
                "table_name": match.group(1),
                "condition":{
                    "variable":condition_match.group(1),
                    "method": condition_match.group(2),
                    "value": condition_match.group(3)
                },
                "target_condition":{
                    "target": target_match.group(1),
                    "value": target_match.group(2),
                },    
            }
       
        else:
            return {"error": "Invalid query format! Please restate your request!"}

# Example usage
if __name__ == "__main__":
    # Database
    show_database_pattern = "SHOW DATABASE;"
    create_database_pattern = "CREATE DATABASE dsci551 Relational;"
    use_database_pattern = "USE DATABASE dsci551 Relational;"
    drop_database_pattern = "DROP DATABASE dsci551 Relational;"

    # Table
    show_table_pattern = "SHOW TABLES;"
    create_table_pattern = "CREATE TABLE cars SET (ID PRIMARY KEY, name, model FOREIGN KEY REF model$Model_name, price);"
    import_table_pattern = "IMPORT TABLE cars FROM user/data/cars.csv SET (ID PRIMARY KEY, model FOREIGN KEY REF model$model_name);"
    drop_table_pattern = "DROP TABLE cars;"

    # Operator
    projection_pattern = "FIND (ID, NAME) IN TABLE cars;"
    filtering_pattern = "FIND (ID, NAME) IF ID = “100” IN TABLE cars;"
    join_pattern = "FIND (cars$price, model$color) JOIN BY cars$model = model$model_name ON inner;"
    group_agg_pattern = "FIND COUNT(price) GROUP BY model IN TABLE cars;"
    ordering_pattern = "FIND (ID, NAME) IN TABLE cars ORDER BY ID DESC;"

    # Modification
    inserting_pattern = "INSERT INTO cars(ID,name,model,price) VALUES (“001”, “BMW”, “SUV”, “45000”);"
    deleting_row_pattern = "DELETE FROM cars IF price > “40000”;"
    deleting_column_pattern = "DELETE (name, price) FROM cars;"
    updating_pattern = "UPDATE cars SET (price = “over 100k”) IF price > “100000”;"

    print(QueryParser().Parser("show database;"))


