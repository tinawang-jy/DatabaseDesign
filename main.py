import json
import os
import operator
import csv
import re
from createDB import DatabaseManager
from createTable import TableManager
from Modification import Updater
from parseQuery import QueryParser
import config
from config import db_path

def implement(command):
    global db_path
    if command['query_type'] == "create_database":
        worker = DatabaseManager(command_type="create_database", database_name=command["database_name"], database_type=command["type_of_database"])
        worker.create_database()
        return

    elif command['query_type'] == "show_database":
        DatabaseManager(command_type="show_database").show_database()
        return

    elif command['query_type'] == "use_database":
        worker = DatabaseManager(command_type="use_database", database_name=command["database_name"], database_type=command["type_of_database"])
        db_path = worker.use_database()
        return db_path

    elif command['query_type'] == "drop_database":
        worker = DatabaseManager(command_type="drop_database", database_name=command["database_name"], database_type=command["type_of_database"])
        worker.drop_database()
        return

    # table
    elif command['query_type'] == "show_table":
        worker = TableManager(command_type="show_table")
        worker.show_table(db_path)
        return

    elif command['query_type'] == "create_table":
        worker = TableManager(command_type="create_table", table_name=command["table_name"],columns=command["columns"], primary_key=command["primary key"], foreign_key=command["foreign key"], input_path=None)
        worker.create_table(db_path)
        return

    elif command['query_type'] == "import_table":
        worker = TableManager(command_type="import_table", table_name=command["table_name"], primary_key=command["primary key"], foreign_key=command["foreign key"], input_path=command["file_path"])
        worker.import_table(db_path)
        return

    elif command['query_type'] == "drop_table":
        worker = TableManager(command_type="drop_table", table_name=command["table_name"])
        worker.drop_table(db_path)
        return

    # modification
    elif command['query_type'] == "inserting":
        worker = Updater(command_type="inserting", table_name=command["table_name"],columns=command["columns"], condition=None, target_condition=None, values=command["values"],db_path=db_path)
        worker.insert(db_path)
        return

    elif command['query_type'] == "deleting_row":
        worker = Updater(command_type="deleting_row", table_name=command["table_name"], condition=command["condition"],db_path=db_path)
        worker.delete_row(db_path)
        return

    elif command['query_type'] == "deleting_column":
        worker = Updater(command_type="deleting_column", table_name=command["table_name"], columns=command["columns"],db_path=db_path)
        worker.delete_col(db_path)
        return

    elif command['query_type'] == "updating":
        worker = Updater(command_type="updating", table_name=command["table_name"],condition=command["condition"],target_condition=command["target_condition"],db_path=db_path)
        worker.update(db_path)
        return
    
    # aggregation,...



if __name__ == "__main__":
    print("Welcome to our database management system! Please first choose/create your database, use database, and then try any CRUD queries here!")
    global db_path
    while True:
        initial_query = input("Enter your command here:")
        initial_query_info = QueryParser().Parser(initial_query) # a dict
        print(initial_query_info)
        implement(initial_query_info)
        if initial_query_info['query_type'] == "use_database":
            config.set_db_path(implement(initial_query_info))
            print(db_path)

        if db_path is not None:  # Check if db_path has been set
            while True:  # Start a loop that will run until the user decides to exit
                user_input = input("Enter your CRUD command here:")
                query_info = QueryParser().Parser(user_input) # a dict
                print(query_info)
                implement(query_info)
                continue_input = input("Do you want to exit? (yes/no): ").lower()
                if continue_input == 'yes':
                    print("Bye Bye")
                    break


        continue_input = input("Sure to exit? (yes/no): ").lower()
        if continue_input == 'yes':
            print("Bye Bye")
            break
