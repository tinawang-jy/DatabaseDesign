import csv
import json
import os
from createDB import DatabaseManager

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



    def show_table(self):
        if self.command_type == "show_table":
            for db in self.db_info:
                if db['database_path'] == db_path:
                    print(db['tables'])

    def new_table_nosql(self):
        data = {column: None for column in self.columns}
        os.makedirs(f'{db_path}/{self.table_name}')
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

    def new_table_relational(self):
        os.makedirs(f'{db_path}/{self.table_name}')
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


    def create_table(self):
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
                                if self.primary_key is None:
                                    print("Primary key needed!")
                                else:
                                    # check foreign key
                                    if self.foreign_key is not None:
                                        print(f'You are creating the first table in database. No table existing to refer.')
                                    else:
                                        self.new_table_nosql()

                            # db not empty
                            else:
                                for table in db['tables']:
                                    table_metadata_path = f'{table["table_path"]}/metadata.json'
                                    with open(table_metadata_path, 'r', encoding='utf-8') as f:
                                        table_info.append(json.load(f))
                                # check primary key
                                if self.primary_key is None:
                                    print("Please designate a primary key for your table.")
                                else:
                                    # case when no foreign key
                                    if self.foreign_key is None:
                                        self.new_table_nosql()
                                    # case when exists foreign key
                                    else:
                                        for key in self.foreign_key:
                                            # foreign key must refer to primary key
                                            for info in table_info:
                                                if info['table_name'] == key['ref_table'] and info['primary key'] == key['ref_column']:
                                                    self.new_table_nosql()
                                                else:
                                                    print("Check your foreign key!")

                        elif db_path.endswith('_relational'):
                            # empty db
                            if len(db['tables']) == 0:
                                # check primary key
                                if self.primary_key is None:
                                    print("Primary key needed.")
                                else:
                                # check foreign key
                                    if self.foreign_key is not None:
                                        print(f'You are creating the first table in database. No table existing to refer.')
                                    else:
                                        self.new_table_relational()

                            # db not empty
                            else:
                                # check primary key
                                if self.primary_key is None:
                                    print("Please designate a primary key for your table.")
                                else:
                                    # case when no foreign key
                                    if self.foreign_key is None:
                                        self.new_table_relational()
                                    # case when exists foreign key
                                    else:
                                        for key in self.foreign_key:
                                            # foreign key must refer to primary key
                                            for info in table_info:
                                                if info['table_name'] == key['ref_table'] and info['primary key'] == key['ref_column']:
                                                    self.new_table_relational()
                                                else:
                                                    print("Check your foreign key!")

    def chunk_to_json(self):
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

    def chunk_to_csv(self):
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

    def import_table(self):
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
                                if self.primary_key is None:
                                    print("Primary key needed!")
                                elif self.primary_key not in headers:
                                    print("Primary key not in import file.")
                                else:
                                    # check foreign key
                                    if self.foreign_key is not None:
                                        print(f'You are creating the first table in database. No table existing to refer.')
                                    else:
                                        self.chunk_to_json()

                            # db not empty
                            else:
                                # check primary key
                                if self.primary_key is None:
                                    print("Please designate a primary key for your table.")
                                elif self.primary_key not in headers:
                                    print("Primary key not in import file.")
                                else:
                                    # case when no foreign key
                                    if self.foreign_key is None:
                                        self.chunk_to_json()
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
                                                    self.chunk_to_json()  # Stop checking since we've found a valid foreign key reference

                                            if not foreign_key_is_valid:
                                                print("Check your foreign key!")


                        elif db_path.endswith('_relational'):
                            # empty db
                            if len(db['tables']) == 0:
                                # check primary key
                                if self.primary_key is None:
                                    print("Primary key needed.")
                                elif self.primary_key not in headers:
                                    print("Primary key not in import file.")
                                else:
                                # check foreign key
                                    if self.foreign_key is not None:
                                        print(f'You are creating the first table in database. No table existing to refer.')
                                    else:
                                        self.chunk_to_csv()

                            # db not empty
                            else:
                                # check primary key
                                if self.primary_key is None:
                                    print("Please designate a primary key for your table.")
                                elif self.primary_key not in headers:
                                    print("Primary key not in import file.")
                                else:
                                    # case when no foreign key
                                    if self.foreign_key is None:
                                        self.chunk_to_csv()
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
                                                    self.chunk_to_csv()  # Stop checking since we've found a valid foreign key reference

                                            if not foreign_key_is_valid:
                                                print("Check your foreign key!")













if __name__ == "__main__":
    db_path = "root/xyz_relational"
            # Input: table; file path; primary key; foreign key{self_column”, “ref_table”, “ref_column”}

    test = TableManager(command_type="import_table", table_name="basic", primary_key='id', input_path='basic.csv',foreign_key=[{"self_column":'id',
                                                                                                                                "ref_table":'buyer',
                                                                                                                               "ref_column":'4s_store'}])
    #test2 = TableManager(command_type="create_table", table_name="happy", primary_key='id', columns=['id','me'])

    test.import_table()


