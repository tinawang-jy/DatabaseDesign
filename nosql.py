import csv
import json
import os
import re
import uuid
import shutil



def database_monitor(directory):
    dir_info = []

    # Walk through all directories and files in the given directory
    for root, dirs, files in os.walk(directory):
        for d in dirs:
            # Prepare the information for each directory
            subdir_info = {
                'directory_name': d,
                'num_files': 0,
                'attributes': set()
            }

            # Construct the full path to the sub-directory
            subdir_path = os.path.join(root, d)

            # List all files in the sub-directory
            subdir_files = os.listdir(subdir_path)

            # Count only files (not sub-directories)
            files_only = [f for f in subdir_files if os.path.isfile(os.path.join(subdir_path, f))]
            subdir_info['num_files'] = len(files_only)

            # Iterate over each file
            for file_name in files_only:
                # Check if the file is a JSON by its extension
                if file_name.lower().endswith('.json'):
                    with open(os.path.join(subdir_path, file_name), 'r', encoding='utf-8') as file:
                        try:
                            data = json.load(file)
                            # Assuming the JSON structure is a list of objects
                            if isinstance(data, list) and len(data) > 0:
                                attributes = set(data[0].keys())  # Get attribute names from the first object
                                subdir_info['attributes'] = subdir_info['attributes'] | attributes
                        except json.JSONDecodeError as e:
                            print(f"An error occurred while decoding JSON from file {file_name}: {e}")
            subdir_info['attributes'] = list(subdir_info['attributes'])
            dir_info.append(subdir_info)

    return dir_info


def save_to_json(data, filename):
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)

def update_and_save_database_info(directory, json_output_file):
    dir_info = database_monitor(directory)
    save_to_json(dir_info, json_output_file)

def metadata(metadata_filepath):
    with open(metadata_filepath,'r', encoding='utf-8') as record:
        return json.load(record)

def parse_crud(user_input):
    # crud patterns
    create_pattern = r"CREATE TABLE (\w+) FROM (.+);"
    drop_pattern = r"^DROP TABLE ([a-zA-Z0-9_]+);"

    projection_pattern = r"FIND \{(.+?)\} IN TABLE (\w+);"
    filtering_pattern = r"FIND \{(.+?)\}\s*IF \((.+?)\)\s*IN TABLE (\w+)\s*(?:AND (\w+) JOIN BY (\w+))?;"
    join_pattern = r"JOIN (\w+) AND (\w+) BY (\w+) ON=\'(\w+)\';"
    grouping_pattern = r"FIND (\w+)\.(\w+)\(\) GROUP BY (\w+)\s*IN TABLE (\w+)(?: AND (\w+) JOIN BY (\w+))?;"
    aggregation_pattern = r"FIND (\w+)\.(\w+)\(\)(?: IF \((.+?)\))? IN TABLE (\w+)(?: AND (\w+) BY (\w+));"
    ordering_pattern = r"FIND\s+{([^}]+)}\s+IN\s+TABLE\s+(\w+)\s+(?:ORDER BY\s+(\w+)\s+(DESC|ASC)\s+IN\s+TABLE\s+(\w+))?(?:\s+AND\s+(\w+)\s+JOIN BY\s+(\w+))?;"
    inserting_pattern = r"INSERT INTO (\w+)\(([^)]+)\) VALUES\(([^)]+)\);"


    deleting_pattern = r"DELETE FROM\s*(\w+)\s*IF\s*(.*?);"

    updating_pattern = r"UPDATE (\w+)\s*SET (\w+)\s*=\s*(\w+)\s*IF\s*(.*?);"

    # match input to pattern
    if re.match(create_pattern, user_input,re.IGNORECASE):
        match = re.match(create_pattern, user_input)
        crud = {
            "query_type": "create",
            "input_path": match.group(2),
            "table_name": match.group(1),
        }
        return crud

    elif re.match(drop_pattern, user_input,re.IGNORECASE):
        match = re.match(drop_pattern, user_input)
        crud = {
            "query_type": "drop",
            "table_name": match.group(1)
        }
        return crud



    # Match the query to the appropriate pattern

    elif re.match(inserting_pattern, user_input):
        match = re.match(inserting_pattern, user_input, re.IGNORECASE)
        crud =  {
            "query_type": "inserting",
            "table_name": match.group(1),
            "columns": [col.strip() for col in match.group(2).split(", ")],
            "values": [value.strip() for value in match.group(3).split(", ")],
        }
        return crud

    else:
        print("Invalid query format! Please restate your request!")
        return None


def store_chunks(csv_path, chunk_size, output_dir):
    if not os.path.exists(f'database/{output_dir}'):
        os.makedirs(f'database/{output_dir}')
    try:
        with open(csv_path, 'r') as csvfile:
            reader = csv.reader(csvfile)
            headers = next(reader)
            headers.append('_id')
            chunk = []
            chunk_number = 1
            for row in reader:
                row.append(str(uuid.uuid4()))  # Assign a unique ID to each row
                chunk.append(dict(zip(headers, row)))
                if len(chunk) == chunk_size:
                    # write the current chunk to a JSON file
                    with open(f'database/{output_dir}/chunk_{chunk_number}.json', 'w') as jsonfile:
                        json.dump(chunk, jsonfile)

                    chunk_number += 1
                    chunk = []  # Reset the chunk for the next set of rows

            # Handle the last chunk which might be less than chunk_size
            if chunk:
                with open(f'database/{output_dir}/chunk_{chunk_number}.json', 'w') as jsonfile:
                    json.dump(chunk, jsonfile)
        return {"headers":headers, "# of chunks": chunk_number, "location":output_dir}
    except Exception as e:
        print(f"An error occurred: {e}")






def user_interact():
    database_directory = 'database'
    json_output_file = 'database/database_info.json'

    # Run database monitor before user input
    update_and_save_database_info(database_directory, json_output_file)
    data = metadata('database/database_info.json') # list of dic
    table_names = []
    for dic in data:
        table_names.append(dic["directory_name"])

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

                            if re.compile() in columns:
                                # if set(columns) <= set(x["attributes"]): sql need to check columns, nosql is fine
                                with open() x["num_files"] as f:



                print(crud)


        except Exception as e:
            print(f"An error occurred: {e}")


    # Run database monitor after user input
    update_and_save_database_info(database_directory, json_output_file)


if __name__ == "__main__":
    if not os.path.exists("database"):
        os.makedirs("database")
    while True:  # Start a loop that will run until the user decides to exit
        user_interact()
        continue_input = input("Do you want to exit? (yes/no): ").lower()
        if continue_input == 'yes':
            break



