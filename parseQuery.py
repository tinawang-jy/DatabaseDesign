import re

class QueryParser:
    def __init__(self):
        # database 
        self.show_database_pattern = r"SHOW DATABASE;"
        self.create_database_pattern = r"CREATE DATABASE (\w+) (Relational|NoSQL);"
        self.use_database_pattern = r"USE DATABASE (\w+);"
        self.drop_database_pattern = r"DROP DATABASE (\w+);"

        # table
        self.show_table_pattern = r"SHOW TABLES;"
        self.create_table_pattern = r"CREATE TABLE (\w+) SET \(([^)]+)\);"
        self.import_table_pattern = r"IMPORT TABLE (\w+) FROM (.+) SET \(([^)]+)\);"
        self.drop_table_pattern = r"DROP TABLE (\w+);"

        # operator
        self.projection_pattern = r"FIND \{(.+?)\} IN TABLE (\w+);"
        self.filtering_pattern = r"FIND \{(.+?)\}\s*IF \((.+?)\)\s*IN TABLE (\w+)\s*(?:AND (\w+) JOIN BY (\w+))?;"
        self.join_pattern = r"JOIN (\w+) AND (\w+) BY (\w+) ON=\'(\w+)\';"
        self.group_agg_pattern = r"FIND (\w+)\.(\w+)\(\) GROUP BY (\w+)\s*IN TABLE (\w+)(?: AND (\w+) JOIN BY (\w+))?;"
        self.ordering_pattern = r"FIND\s+{([^}]+)}\s+IN\s+TABLE\s+(\w+)\s+(?:ORDER BY\s+(\w+)\s+(DESC|ASC)\s+IN\s+TABLE\s+(\w+))?(?:\s+AND\s+(\w+)\s+JOIN BY\s+(\w+))?;"

        # modification
        self.inserting_pattern = r"INSERT INTO (\w+)\(([^)]+)\) VALUES\(([^)]+)\);"
        self.deleting_row_pattern = r"DELETE FROM\s*(\w+)\s*IF\s*(.*?);"
        self.deleting_column_pattern = r""
        self.updating_pattern = r"UPDATE (\w+)\s*SET (\w+)\s*=\s*(\w+)\s*IF\s*(.*?);"

    def dbParser(self, query):
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
                "type_of_database": match.group(2)
            }
        
        elif re.match(self.use_database_pattern, query, re.IGNORECASE):
            match = re.match(self.use_database_pattern, query)
            return {
                "query_type": "use_database",
                "database_name": match.group(1)
            } 
        
        elif re.match(self.drop_database_pattern, query, re.IGNORECASE):
            match = re.match(self.use_database_pattern, query)
            return {
                "query_type": "drop_database",
                "database_name": match.group(1)
            }               

        else:
            return {"error": "Invalid query format! Please restate your request!"}
                   
    def tableParser(self, query):
        if re.match(self.show_table_pattern, query, re.IGNORECASE):
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
            
            foreign_key_pattern = r"(\w+)\sFOREIGN KEY REF (\w+)\$(\w+)"
            foreign_key_match = re.findall(foreign_key_pattern, column)

            foreign_key = []
            for item in foreign_key_match:
                foreign_key.append({
                        "self_column":item[0],
                        "ref_table":item[1],
                        "ref_column":item[2]})

            return {
                "query_type": "create_table",
                "table_name": match.group(1),
                "columns": column_names,
                "primary key": primary_key_match.gourp(1),
                "foreign key": foreign_key
            }
        
        elif re.match(self.import_table_pattern, query, re.IGNORECASE):
            match = re.match(self.import_table_pattern, query)

            if match.group(3): 
                column = match.group(3)

                primary_key_pattern = r"(\w+)\sPRIMARY KEY"
                primary_key_match = re.match(primary_key_pattern, column)
                
                foreign_key_pattern = r"(\w+)\sFOREIGN KEY REF (\w+)\$(\w+)"
                foreign_key_match = re.findall(foreign_key_pattern, column)

                foreign_key = []
                for item in foreign_key_match:
                    foreign_key.append({
                            "self_column":item[0],
                            "ref_table":item[1],
                            "ref_column":item[2]})
            else:
                primary_key_match = ""
                foreign_key = []
            
            return {
                "query_type": "import_table",
                "table_name": match.group(1),
                "file_path": match.group(2),
                "primary key": primary_key_match.group(1),
                "foreign key": foreign_key
            }

        elif re.match(self.drop_table_pattern, query, re.IGNORECASE):
            match = re.match(self.drop_table_pattern, query)
            return  {
                "query_type": "drop",
                "table_name": match.group(1)
            }

        else:
            return {"error": "Invalid query format! Please restate your request!"}
        
    def operatorParse(self, query):
        if re.match(self.projection_pattern, query, re.IGNORECASE):
            match = re.match(self.projection_pattern, query)
            return {
                "query_type": "projection",
                "table_name": match.group(2),
                "columns": [col.strip() for col in match.group(1).split(", ")],
            }
        
        elif re.match(self.filtering_pattern, query, re.IGNORECASE):
            match = re.match(self.filtering_pattern, query)
            return {
                "query_type": "filtering",
                "table_name": match.group(3),
                "columns": [col.strip() for col in match.group(1).split(", ")],
                "condition": {
                    "variable":"",
                    "method": "",
                    "value": ""
                }
            }
        
        elif re.match(self.join_pattern, query, re.IGNORECASE):
            match = re.match(self.join_pattern, query)
            
            selected_columns = []
            selected_columns.append({"table1":"", 
                                     "column":[]})

            return {
                "query_type": "join",
                "selected": selected_columns,
                "left_table": "",
                "left_column": "",
                "right_table": "",
                "right_column": "",
                "join_method": "",
            }
        
        elif re.match(self.group_agg_pattern, query, re.IGNORECASE):
            match = re.match(self.group_agg_pattern, query)
            return {
                "query_type": "group_agg",
                "table_name": match.group(4),
                "agg_column": "",
                "agg_method": match.group(2)
            }
        
        elif re.match(self.ordering_pattern, query, re.IGNORECASE):
            match = re.match(self.ordering_pattern, query)
            return {
                "query_type": "ordering",
                "table_name": match.group(2),
                "columns": [col.strip() for col in match.group(1).split(",")],
                "order_by_column": "",
                "method":""
            }
        
        else:
            return {"error": "Invalid query format! Please restate your request!"}
        
    def modificationParser(self, query):
        if re.match(self.inserting_pattern, query, re.IGNORECASE):
            match = re.match(self.inserting_pattern, query)
            return {
                "query_type": "inserting",
                "table_name": match.group(1),
                "columns": [col.strip() for col in match.group(2).split(",")],
                "values": [value.strip() for value in match.group(3).split(",")],
            }
        
        elif re.match(self.deleting_row_pattern, query, re.IGNORECASE):
            match = re.match(self.deleting_row_pattern, query)
            return {
                "query_type": "deleting_row",
                "table_name": match.group(1),
                "condition":{
                    "variable":"",
                    "method": "",
                    "value": ""
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
            return {
                "query_type": "updating",
                "table_name": match.group(1),
                "condition":{
                    "variable":"",
                    "method": "",
                    "value": ""
                },
                "target_condition":{
                    "target": "",
                    "value": "",
                },    
            }
       
        else:
            return {"error": "Invalid query format! Please restate your request!"}

# Example usage
if __name__ == "__main__":
    # Database
    show_database_pattern = "SHOW DATABASE;"
    create_database_pattern = "CREATE DATABASE dsci551 Relational;"
    use_database_pattern = "USE DATABASE dsci551;"
    drop_database_pattern = "DROP DATABASE dsci551;"

    # Table
    show_table_pattern = "SHOW TABLES;"
    create_table_pattern = "CREATE TABLE cars SET (ID PRIMARY KEY, name, model FOREIGN KEY REF model$Model_name, price);"
    import_table_pattern = "IMPORT TABLE cars FROM user/data/cars.csv SET (ID PRIMARY KEY, model FOREIGN REF model$model_name);"
    drop_table_pattern = "DROP TABLE cars;"

    # Operator
    projection_pattern = "FIND (ID, NAME) IN TABLE cars;"
    filtering_pattern = "FIND (ID, NAME) IF ID = “100” IN TABLE cars;"
    join_pattern = "JOIN cars AND model BY cars$model = model$model_name ON inner;"
    group_agg_pattern = "FIND COUNT(price) GROUP BY model IN TABLE cars;"
    ordering_pattern = "FIND (ID, NAME)IN TABLE cars ORDER BY ID {DESC,ASC};"

    # Modification
    inserting_pattern = "INSERT INTO cars(ID,name,model,price) VALUES (“001”, “BMW”, “SUV”, “45000”);"
    deleting_row_pattern = "DELETE FROM cars IF price > “40000”;"
    deleting_column_pattern = "DELETE (name, price) FROM cars;"
    updating_pattern = "UPDATE cars SET (price = “over 100k”) IF price > “100000”;"

    qp = QueryParser()

    print(qp.dbParser(show_database_pattern))
#    print(qp.dbParser(create_database_pattern))
#   print(qp.dbParser(use_database_pattern))
#    print(qp.dbParser(drop_database_pattern))

#    print(qp.tableParser(show_table_pattern))
#    print(qp.tableParser(create_table_pattern))
#    print(qp.tableParser(import_table_pattern))
#    print(qp.tableParser(drop_table_pattern))

#   print(qp.operatorParse(projection_pattern))
#    print(qp.operatorParse(filtering_pattern))
#    print(qp.operatorParse(join_pattern))
#    print(qp.operatorParse(group_agg_pattern))
#    print(qp.operatorParse(ordering_pattern))

#    print(qp.modificationParser(inserting_pattern))
#    print(qp.modificationParser(deleting_row_pattern))
#    print(qp.modificationParser(deleting_column_pattern))
#    print(qp.modificationParser(updating_pattern))
