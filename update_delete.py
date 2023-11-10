import csv
import json
import os
import re
import uuid
import shutil

def ud_parser(user_input):
    # patterns
    inserting_pattern = r"INSERT INTO (\w+)\(([^)]+)\) VALUES\(([^)]+)\);"

    #match to operation
    if re.match(inserting_pattern, user_input):
        match = re.match(inserting_pattern, user_input, re.IGNORECASE)
        crud =  {
            "query_type": "inserting",
            "table_name": match.group(1),
            "columns": [col.strip() for col in match.group(2).split(", ")],
            "values": [value.strip() for value in match.group(3).split(", ")],
        }
        return crud

def insert(dbinfo, crud):
    # Run database monitor before user input
    update_and_save_database_info(root, metadata_filepath)

    # After parsing the input commands, we should get the following info
    dbinfo
    database_directory, database_name, metadata_filepath = input() #use or create database from input commands

    data = metadata(metadata_filepath)
    subdir_info = {
                'directory_name': d,
                'num_files': 0,
                'attributes': set()
            }
            sample_format_for_data = {
                'database_name' : database_name,
                'tables' : subdir_info
            }
    table_names = []
    for db in data:
        if db["database_name"] == database_name:
            for table in db["tables"]:
                table_names.append(table["directory_name"])


    table_name = crud['table_name']
    columns = crud["columns"]
    values = crud["values"]
    if table_name not in table_names:
        print(f"Table {table_name} doesn't exist.")
        return
    else:
        for db in data:
            if db["tables"]["directory_name"] == table_name:
                if db["tables"]["attributes"] >= set(columns):
                    new = dict(zip(columns,values))
                    with open(f'{database_directory}/{table_name}/chunk_{db["tables"]["num_files"]}.json', 'w', encoding='utf-8') as f:
                        data = json.load(f)
                    data.append(new)
                    with open(f'{database_directory}/{table_name}/chunk_{db["tables"]["num_files"]}.json', 'w', encoding='utf-8') as f:
                        json.dump(data,f,indent=4)






    # assumption: operated within a database, varname = database_name
def past():
    user_input = input("Enter the command (or type 'exit' to stop): ")
    if user_input.lower() == 'exit':  # Check if the user wants to exit the loop
        print("Exiting the program.")
        return
    else:
        try:
            crud = parse_crud(user_input)
            if crud['query_type'] == 'create':
                in_path = crud['input_path']
                out_path = crud['table_name']
                if out_path not in table_names:
                    store_chunks(in_path,100,out_path)
                    print(crud)
                    print(f"CSV data from {in_path} has been stored in JSON format in the directory '{out_path}'.")
                else:
                    print(f"Table {out_path} already exists.")
                    return

            elif crud["query_type"] == "drop":
                path_to_delete = crud['table_name']
                if path_to_delete in table_names:
                    shutil.rmtree(f'database/{path_to_delete}')
                    print(f"Table {path_to_delete} has been deleted.")
                else:
                    print(f"Table {path_to_delete} doesn't exist.")
                    return

            elif crud["query_type"] == "inserting":
                # check then find a chunk not exceed 100 and insert
                table_name = crud['table_name']
                columns = crud["columns"]
                values = crud["values"]
                if table_name not in table_names:
                    print(f"Table {table_name} doesn't exist.")
                    return
                else:
                    for x in data:
                        if x["directory_name"] == table_name:

                            if re.compile(r"\b_id\b|\bid\b|.*_id\b") in columns:
                                # if set(columns) <= set(x["attributes"]): sql need to check columns, nosql is fine
                                with open() x["num_files"] as f:



                print(crud)


        except Exception as e:
            print(f"An error occurred: {e}")


    # Run database monitor after user input
    update_and_save_database_info(database_directory, json_output_file)


if __name__ == "__main__":
    user_input = input("Enter the command (or type 'exit' to stop): ")
    crud = ud_parser(user_input)

    if crud["query_type"] == "inserting":
        insert(dbinfo, crud)
