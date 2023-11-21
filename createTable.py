import csv
import json
import os
from createDB import DatabaseManager
import shutil
from config import db_path

class TableManager:
    def __init__(self, command_type=None, table_name=None,
                columns=None, primary_key=None, foreign_key=None, input_path=None):
        self.command_type = command_type
        self.table_name = table_name
        self.columns = columns
        self.primary_key = primary_key
        self.foreign_key = foreign_key
        self.input_path = input_path
        self.db_info = DatabaseManager().monitor()



    def show_table(self,db_path):
        if self.command_type == "show_table":
            for db in self.db_info:
                if db['database_path'] == db_path:
                    print(db['tables'])

    def new_table_nosql(self,db_path):
        data = {column: "" for column in self.columns}
        directory_path = f'{db_path}/{self.table_name}'
        if not os.path.exists(directory_path):
        # If it does not exist, create it
            os.makedirs(directory_path, exist_ok=True)
        # create empty file
        with open(f'{db_path}/{self.table_name}/chunk_1.json', 'w', encoding='utf-8') as f:
            json.dump([data], f, indent=4)
            # create table metadata file
        with open(f'{db_path}/{self.table_name}/metadata.json', 'w', encoding='utf-8') as f:
            obj = {
                    "table_name": self.table_name,
                    "columns": self.columns,
                    "primary key": self.primary_key,
                    "foreign key": self.foreign_key
                                    }
            json.dump(obj, f, indent=4)
        print(f"Table {self.table_name} created in JSON.")

    def new_table_relational(self,db_path):
        directory_path = f'{db_path}/{self.table_name}'
        if not os.path.exists(directory_path):
        # If it does not exist, create it
            os.makedirs(directory_path, exist_ok=True)
        # create empty file
        with open(f'{db_path}/{self.table_name}/chunk_1.csv', 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.columns)
            writer.writeheader()  # Write the headers to the CSV file
            # create table metadata file
        with open(f'{db_path}/{self.table_name}/metadata.json', 'w', encoding='utf-8') as f:
            obj = {
                    "table_name": self.table_name,
                    "columns": self.columns,
                    "primary key": self.primary_key,
                    "foreign key": self.foreign_key
                                    }
            json.dump(obj, f, indent=4)
        print(f"Table {self.table_name} created in csv.")


    def create_table(self,db_path):
        # Input: table; columns; primary key; foreign key{self_column, ref_table, ref_column}
        # Output:  “table successfully created” or print error message
        table_info = []
        if self.command_type == "create_table":
            for db in self.db_info:
                if db['database_path'] == db_path:
                    for table in db['tables']:
                        table_metadata_path = f'{table["table_path"]}/metadata.json'
                        with open(table_metadata_path, 'r', encoding='utf-8') as f:
                            table_info.append(json.load(f))
                    tb_names = []
                    for tb in table_info:
                        tb_names.append(tb["table_name"])
                    if self.table_name in tb_names:
                        print(f'Table name {self.table_name} already exist.')
                    else:
                        if db_path.endswith('_nosql'):
                            # empty db
                            if len(db['tables']) == 0:
                                # check primary key
                                if self.primary_key == []:
                                    print("Primary key needed!")
                                else:
                                    # check foreign key
                                    if self.foreign_key != []:
                                        print(f'You are creating the first table in database. No table existing to refer.')
                                    else:
                                        self.new_table_nosql(db_path)

                            # db not empty
                            else:
                                for table in db['tables']:
                                    table_metadata_path = f'{table["table_path"]}/metadata.json'
                                    with open(table_metadata_path, 'r', encoding='utf-8') as f:
                                        table_info.append(json.load(f))
                                # check primary key
                                if self.primary_key == []:
                                    print("Please designate a primary key for your table.")
                                else:
                                    # case when no foreign key
                                    if self.foreign_key == []:
                                        self.new_table_nosql(db_path)
                                    # case when exists foreign key
                                    else:
                                        key = self.foreign_key[0]
                                            # foreign key must refer to primary key
                                        for info in table_info:
                                            if info['table_name'] == key['ref_table'] and info['primary key'] == key['ref_column']:
                                                self.new_table_nosql(db_path)
                                                break
                                            else:
                                                print("Check your foreign key.")
                                                break


                        elif db_path.endswith('_relational'):
                            # empty db
                            if len(db['tables']) == 0:
                                # check primary key
                                if not self.primary_key:
                                    print("Primary key needed.")
                                else:
                                # check foreign key
                                    if self.foreign_key == []:
                                        print(f'You are creating the first table in database. No table existing to refer.')
                                    else:
                                        self.new_table_relational(db_path)


                            # db not empty
                            else:
                                # check primary key
                                if self.primary_key == []:
                                    print("Please designate a primary key for your table.")

                                else:
                                    # case when no foreign key
                                    if not self.foreign_key:
                                        self.new_table_relational(db_path)
                                    # case when exists foreign key
                                    else:
                                        for key in self.foreign_key:
                                            # foreign key must refer to primary key
                                            for info in table_info:
                                                if info['table_name'] == key['ref_table'] and info['primary key'] == key['ref_column']:
                                                    self.new_table_relational(db_path)
                                                    break
                                                else:
                                                    print("Check your foreign key.")
                                                    break


    def chunk_to_json(self,db_path):
        path = f'{db_path}/{self.table_name}'
        chunk_size = 100
        if not os.path.exists(path):
            os.makedirs(path)
        try:
            with open(self.input_path, 'r') as csvfile:
                reader = csv.reader(csvfile)
                headers = next(reader)
                #headers.append('_id')
                chunk = []
                chunk_number = 1
                for row in reader:
                    #row.append(str(uuid.uuid4()))  # Assign a unique ID to each row
                    chunk.append(dict(zip(headers, row)))
                    if len(chunk) == chunk_size:
                        # write the current chunk to a JSON file
                        with open(f'{path}/chunk_{chunk_number}.json', 'w') as jsonfile:
                            json.dump(chunk, jsonfile)

                        chunk_number += 1
                        chunk = []  # Reset the chunk for the next set of rows

                # Handle the last chunk which might be less than chunk_size
                if chunk:
                    with open(f'{path}/chunk_{chunk_number}.json', 'w') as jsonfile:
                        json.dump(chunk, jsonfile)
            #return {"headers":headers, "# of chunks": chunk_number, "location":output_dir}
            with open(f'{path}/metadata.json', 'w', encoding='utf-8') as f:
                obj = {
                        "table_name": self.table_name,
                        "columns": headers,
                        "primary key": self.primary_key,
                        "foreign key": self.foreign_key
                                        }
                json.dump(obj, f, indent=4)
            print(f"Table {self.table_name} imported into JSON.")
        except Exception as e:
            print(f"An error occurred: {e}")

    def chunk_to_csv(self,db_path):
        path = f'{db_path}/{self.table_name}'
        chunk_size = 100  # Define your desired chunk size
        if not os.path.exists(path):
            os.makedirs(path)
        try:
            with open(self.input_path, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)  # Use DictReader to read CSV into a dictionary
                chunk = []
                chunk_number = 1
                for row in reader:
                    # Assign a unique ID to each row if needed
                    # row['_id'] = str(uuid.uuid4())
                    chunk.append(row)
                    if len(chunk) == chunk_size:
                        # Write the current chunk to a CSV file
                        with open(f'{path}/chunk_{chunk_number}.csv', 'w', newline='', encoding='utf-8') as chunkfile:
                            writer = csv.DictWriter(chunkfile, fieldnames=reader.fieldnames)
                            writer.writeheader()
                            writer.writerows(chunk)

                        chunk_number += 1
                        chunk = []  # Reset the chunk for the next set of rows

                # Handle the last chunk which might be less than chunk_size
                if chunk:
                    with open(f'{path}/chunk_{chunk_number}.csv', 'w', newline='', encoding='utf-8') as chunkfile:
                        writer = csv.DictWriter(chunkfile, fieldnames=reader.fieldnames)
                        writer.writeheader()
                        writer.writerows(chunk)

            with open(f'{path}/metadata.json', 'w', encoding='utf-8') as f:
                obj = {
                        "table_name": self.table_name,
                        "columns": reader.fieldnames,
                        "primary key": self.primary_key,
                        "foreign key": self.foreign_key
                                            }
                json.dump(obj, f, indent=4)
            print(f"Table {self.table_name} imported into csv.")
        except Exception as e:
            print(f"An error occurred: {e}")

    def import_table(self,db_path):
        # Input: table; file path; primary key; foreign key{self_column”, “ref_table”, “ref_column”}
        # Output: “table successfully imported” or print error message
        table_info = []
        with open(self.input_path, 'r') as csvfile:
            reader = csv.reader(csvfile)
            headers = next(reader)
        if self.command_type == "import_table":
            for db in self.db_info:
                if db['database_path'] == db_path:
                    for table in db['tables']:
                        table_metadata_path = f'{table["table_path"]}/metadata.json'
                        with open(table_metadata_path, 'r', encoding='utf-8') as f:
                            table_info.append(json.load(f))
                    tb_names = []
                    for tb in table_info:
                        tb_names.append(tb["table_name"])
                    if self.table_name in tb_names:
                        print(f'Table name {self.table_name} already exist.')
                    else:
                        if db_path.endswith('_nosql'):
                            # empty db
                            if len(db['tables']) == 0:
                                # check primary key
                                if self.primary_key == []:
                                    print("Primary key needed!")
                                elif self.primary_key not in headers:
                                    print("Primary key not in import file.")
                                else:
                                    # check foreign key
                                    if self.foreign_key != []:
                                        print(f'You are creating the first table in database. No table existing to refer.')
                                    else:
                                        self.chunk_to_json(db_path)

                            # db not empty
                            else:
                                # check primary key
                                if not self.primary_key:
                                    print("Please designate a primary key for your table.")
                                elif self.primary_key not in headers:
                                    print("Primary key not in import file.")
                                else:
                                    # case when no foreign key
                                    if self.foreign_key == []:
                                        self.chunk_to_json(db_path)
                                    # case when exists foreign key
                                    else:
                                        for key in self.foreign_key:
                                            if key['self_column'] not in headers:
                                                print('Foreign key not exist in import table.')

                                            # Check if the foreign key refers to an existing table and primary key
                                            foreign_key_is_valid = False
                                            for info in table_info:
                                                if info['table_name'] == key['ref_table'] and info['primary key'] == key['ref_column']:
                                                    foreign_key_is_valid = True
                                                    self.chunk_to_json(db_path)  # Stop checking since we've found a valid foreign key reference

                                            if not foreign_key_is_valid:
                                                print("Check your foreign key!")


                        elif db_path.endswith('_relational'):
                            # empty db
                            if len(db['tables']) == 0:
                                # check primary key
                                if self.primary_key == []:
                                    print("Primary key needed.")
                                elif self.primary_key not in headers:
                                    print("Primary key not in import file.")
                                else:
                                # check foreign key
                                    if self.foreign_key != []:
                                        print(f'You are creating the first table in database. No table existing to refer.')
                                    else:
                                        self.chunk_to_csv(db_path)

                            # db not empty
                            else:
                                # check primary key
                                if self.primary_key is []:
                                    print("Please designate a primary key for your table.")
                                elif self.primary_key not in headers:
                                    print("Primary key not in import file.")
                                else:
                                    # case when no foreign key
                                    if not self.foreign_key:
                                        self.chunk_to_csv(db_path)
                                    # case when exists foreign key
                                    else:
                                        for key in self.foreign_key:
                                            if key['self_column'] not in headers:
                                                print('Foreign key not exist in import table.')
                                            # foreign key must refer to primary key
                                            foreign_key_is_valid = False
                                            for info in table_info:
                                                if info['table_name'] == key['ref_table'] and info['primary key'] == key['ref_column']:
                                                    foreign_key_is_valid = True
                                                    self.chunk_to_csv(db_path)  # Stop checking since we've found a valid foreign key reference

                                            if not foreign_key_is_valid:
                                                print("Check your foreign key!")

    def drop_table(self,db_path):
        if self.command_type == "drop_table":
            table_info = []
            table_valid = True
            for db in self.db_info:
                if db['database_path'] == db_path:
                    for table in db['tables']:
                        table_metadata_path = f'{table["table_path"]}/metadata.json'
                        with open(table_metadata_path, 'r', encoding='utf-8') as f:
                            table_info.append(json.load(f))
                    tb_names = []
                    for tb in table_info:
                        tb_names.append(tb["table_name"])
                    # check if this tables exist
                    if self.table_name not in tb_names:
                        print(f"Then table {self.table_name} you want to drop doesn't exist.")

                    else:
                        for tb in table_info:
                            # check if this table is referred
                            if tb["foreign key"]:
                                for fk in tb["foreign key"]:
                                    if fk["ref_table"] == self.table_name:
                                        print(f'This table is referred by another table {tb["table_name"]}')
                                        table_valid = False
                                        break

                        if table_valid:
                            path_to_delete = f'{db_path}/{self.table_name}'
                            shutil.rmtree(path_to_delete)
                            print(f'Table {self.table_name} is dropped.')



if __name__ == "__main__":

    db_path = "root/xyz_relational"
            # Input: table; file path; primary key; foreign key{self_column”, “ref_table”, “ref_column”}

    test = TableManager(command_type="import_table", table_name="basic", input_path="basic.csv", primary_key="host_id")
    #test2 = TableManager(command_type="create_table", table_name="happy", primary_key='id', columns=['id','me'])

    test.import_table()


