import json
import logging
import os
import sqlite3

from tabulate import tabulate


class DataClass:
    def __init__(self, target_table: str, attrs: dict):
        self.target_table = target_table
        self.attrs = attrs or {}

    def __setitem__(self, k, v):
        self.attrs[k] = v

    def __getitem__(self, k):
        return self.attrs[k]

    def __delitem__(self, k):
        del self.attrs[k]

    def get_columns(self):
        return list(map(getattr(str, "lower"), self.attrs.keys()))

    def get_values(self):
        return tuple(self.attrs.values())

    def __str__(self):
        return f"[{self.target_table}]: \n{json.dumps(self.attrs, indent=1)}"


def atoi(s: str):
    return int(s) if s.isdigit() else s


class DBOperations:
    def __init__(self, name: str):
        self.sql_create_base = {
            "pilots": """
                CREATE TABLE IF NOT EXISTS Pilots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    first_name TEXT NOT NULL,
                    last_name TEXT NOT NULL,
                    license_number TEXT UNIQUE NOT NULL,
                    flight_hours INTEGER NOT NULL CHECK (flight_hours >= 0)
                );
            """,
            "destinations": """
                CREATE TABLE IF NOT EXISTS Destinations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    city TEXT NOT NULL,
                    country TEXT NOT NULL,
                    airport_code TEXT UNIQUE NOT NULL
                );
            """,
            "flights": """
                CREATE TABLE IF NOT EXISTS Flights (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    flight_number TEXT UNIQUE NOT NULL,
                    source_id INTEGER NOT NULL,
                    destination_id INTEGER NOT NULL,
                    departure_time TEXT NOT NULL,
                    arrival_time TEXT NOT NULL,
                    status TEXT NOT NULL,
                    pilot_id INTEGER,
                    FOREIGN KEY (source_id) REFERENCES Destinations(id),
                    FOREIGN KEY (destination_id) REFERENCES Destinations(id),
                    FOREIGN KEY (pilot_id) REFERENCES Pilots(id)
                );
            """,
        }

        self.sql_insert = "INSERT INTO {table} ({columns}) VALUES ({values});"
        self.sql_search = "SELECT * FROM {table} WHERE {condition};"
        self.sql_update = "UPDATE {table} SET {field} = ? WHERE {condition};"
        self.sql_delete = "DELETE FROM {table} WHERE {condition};"
        self.sql_drop = "DROP TABLE IF EXISTS {table};"
        self.sql_group = "SELECT COUNT(*), {columns} FROM {table} GROUP BY {columns};"

        self.current_dir = os.path.dirname(os.path.abspath(__file__))
        self.dbname = name.capitalize() + ".db"
        logging.basicConfig(
            filename=f"flight_management_{name}.log",
            level=logging.DEBUG,
            format="%(asctime)s - %(levelname)s - %(message)s",
        )
        if not os.path.exists(self.dbname):
            try:
                self.initialize_tables()
            except Exception as e:
                print("Error, operation aborted")
                logging.error(e)

    def get_connection(self):
        return sqlite3.connect(self.dbname)

    def initialize_tables(self):
        for table in self.sql_create_base.keys():
            self.create_table(table)
            self.populate_table(table)

    def create_table(self, table_name: str):
        with self.get_connection() as conn:
            try:
                cursor = conn.cursor()
                cursor.execute(self.sql_create_base[table_name.lower()])
                conn.commit()
                logging.info(f"Table {table_name} created successfully")
            except KeyError:
                logging.error(f"Cannot find DDL for table {table_name}")
            except Exception as e:
                print("Error, operation aborted")
                logging.error(e)

    def populate_table(self, table_name: str):
        try:
            file_path = os.path.join(
                self.current_dir, "src", f"{table_name.lower()}.csv"
            )
            with open(file_path, "r") as f:
                header = list(map(getattr(str, "lower"), f.readline().split(",")))
                for line in f.readlines():
                    fields = [field.strip() for field in line.strip().split(",")]
                    data = DataClass(table_name, dict(zip(header, fields)))
                    self._insert_data(data)
            logging.info(f"Successfully populated table {table_name}")
        except Exception as e:
            print("Error, operation aborted")
            logging.error(e)

    def drop_table(self, table_name: str):
        with self.get_connection() as conn:
            try:
                cursor = conn.cursor()
                query = self.sql_drop.format(table=table_name.capitalize())
                logging.debug(f"Running:\n{query}")
                cursor.execute(query)
                conn.commit()
                logging.info(f"Table {table_name} dropped successfully")
            except Exception as e:
                print("Error, operation aborted")
                logging.error(e)

    def get_table_columns(self, table_name: str):
        with self.get_connection() as conn:
            try:
                cursor = conn.cursor()
                cursor.execute(f"PRAGMA table_info({table_name});")
                return [col[1] for col in cursor.fetchall()]
            except Exception as e:
                print("Error, operation aborted")
                logging.error(f"Failed to get columns for {table_name}: {e}")
                return []

    def _insert_data(self, data: DataClass):
        with self.get_connection() as conn:
            try:
                cursor = conn.cursor()
                query = self.sql_insert.format(
                    table=data.target_table.capitalize(),
                    columns=",".join(data.get_columns()),
                    values=",".join(["?"] * len(data.get_columns())),
                )
                logging.debug(f"Running:\n{query} with {data.get_values()}")
                cursor.execute(query, data.get_values())
                conn.commit()
                logging.info("Inserted row successfully")
            except Exception as e:
                print("Error, operation aborted")
                logging.error(e)

    def insert_data(self, table_name: str):
        columns = self.get_table_columns(table_name)
        if not columns:
            print(f"Table {table_name} does not exist or has no columns.")
            return
        attrs = {}
        print(
            "Enter values for the following fields (leave blank to skip, `EXIT` to cancel):"
        )
        for col in columns:
            if col == "id":
                continue
            value = input(f"{col}: ")
            if value.strip().upper() == "EXIT":
                print("Insert cancelled.")
                return
            if value.strip() == "":
                continue
            attrs[col] = atoi(value)
        if not attrs:
            print("No data entered.")
            return
        data = DataClass(target_table=table_name, attrs=attrs)
        self._insert_data(data)
        self.search_data(
            table_name, id="(SELECT MAX(id) FROM {table})".format(table=table_name)
        )

    def search_data(self, table_name: str, show_all: bool = False, id: int = None):
        with self.get_connection() as conn:
            try:
                cursor = conn.cursor()
                if show_all:
                    query = self.sql_search.format(table=table_name, condition="True")
                elif id is not None:
                    query = self.sql_search.format(
                        table=table_name, condition=f"id = {id}"
                    )
                else:
                    columns = self.get_table_columns(table_name)
                    filter_by = dict(zip(range(1, len(columns) + 1), columns))
                    print("Choose a column to filter by:")
                    for k, v in filter_by.items():
                        print(f"{k}) {v}")
                    choose_column = input()
                    if choose_column.strip().upper() == "EXIT":
                        print("Search cancelled.")
                        return
                    filter_column = filter_by.get(int(choose_column))
                    value = input("Enter a value to filter by:\n")
                    if value.strip().upper() == "EXIT":
                        print("Search cancelled.")
                        return
                    value = atoi(value)
                    query = self.sql_search.format(
                        table=table_name, condition=f"{filter_column} = {value}"
                    )
                logging.debug(f"Running:\n{query}")
                cursor.execute(query)
                data = cursor.fetchall()
                return self.show(table_name, data)
            except Exception as e:
                print("Error, operation aborted")
                logging.error(e)

    def update_data(self, table_name: str):
        with self.get_connection() as conn:
            try:
                cursor = conn.cursor()
                columns = self.get_table_columns(table_name)
                if "id" not in columns:
                    print("No 'id' column found in this table.")
                    return
                print("Enter `EXIT` to cancel.")
                record_id = input("Enter the id of the record to update:\n")
                if record_id.strip().upper() == "EXIT":
                    print("Update cancelled.")
                    return
                record_id = atoi(record_id)
                filter_by = {i: col for i, col in enumerate(columns, 1) if col != "id"}
                print("Choose a column to update:")
                for k, v in filter_by.items():
                    print(f"{k}) {v}")
                choose_column = input()
                if choose_column.strip().upper() == "EXIT":
                    print("Update cancelled.")
                    return
                update_column = filter_by[int(choose_column)]
                new_value = input(f"Enter new value for {update_column}:\n")
                if new_value.strip().upper() == "EXIT":
                    print("Update cancelled.")
                    return
                query = self.sql_update.format(
                    table=table_name, field=update_column, condition="id = ?"
                )
                logging.debug(
                    f"Running:\n{query} with value {new_value} for id {record_id}"
                )
                cursor.execute(query, (new_value, record_id))
                conn.commit()
                print("Record updated successfully.")
                self.search_data(table_name, id=record_id)
            except Exception as e:
                print("Error, operation aborted")
                logging.error(e)

    def delete_data(self, table_name: str):
        with self.get_connection() as conn:
            try:
                cursor = conn.cursor()
                columns = self.get_table_columns(table_name)
                if "id" not in columns:
                    print("No 'id' column found in this table.")
                    return
                record_id = input("Enter the id of the record to delete:\n")
                if record_id.strip().upper() == "EXIT":
                    print("Delete cancelled.")
                    return
                record_id = atoi(record_id)
                query = self.sql_delete.format(table=table_name, condition="id = ?")
                logging.debug(f"Running:\n{query} for id {record_id}")
                cursor.execute(query, (record_id,))
                conn.commit()
                if cursor.rowcount != 0:
                    print(f"Successfully deleted from table {table_name}")
                else:
                    print("Record not found.")
            except Exception as e:
                print("Error, operation aborted")
                logging.error(e)
                print(e)

    def group_data(self, table_name: str):
        columns = self.get_table_columns(table_name)
        if not columns:
            print(f"Table '{table_name}' does not exist or has no columns.")
            return

        filter_by = {i: col for i, col in enumerate(columns, 1) if col != "id"}
        if not filter_by:
            print("No groupable columns found.")
            return

        print("Choose a column to group by (or type 'EXIT' to cancel):")
        for k, v in filter_by.items():
            print(f"{k}) {v}")

        choice = input("Enter choice: ").strip()
        if choice.upper() == "EXIT":
            print("Group by operation cancelled.")
            return

        try:
            choice = int(choice)
            group_column = filter_by.get(choice)
            if not group_column:
                raise ValueError("Invalid column selection.")
        except ValueError:
            print("Invalid selection. Please enter a valid number.")
            return

        query = self.sql_group.format(columns=group_column, table=table_name)
        with self.get_connection() as conn:
            try:
                cursor = conn.cursor()
                logging.debug(f"Running:\n{query}")
                cursor.execute(query)
                data = cursor.fetchall()
                if data:
                    print(
                        tabulate(data, headers=["count", group_column], tablefmt="grid")
                    )
                else:
                    print("No data found for grouping.")
            except Exception as e:
                print("Error, operation aborted")
                logging.error(f"Failed to group by {group_column}: {e}")

    def select_all(self, table_name: str):
        return self.search_data(table_name=table_name, show_all=True)

    def show(self, table_name: str, data: list[tuple]):
        headers = self.get_table_columns(table_name)
        print(tabulate(data, headers=headers, tablefmt="grid"))

    def teardown(self):
        if input("Are you sure?(y/n): ") == "y":
            os.remove(os.path.join(self.current_dir, self.dbname))


if __name__ == "__main__":
    test = DBOperations(name="Testing")
    test.select_all("pilots")
    test.group_data("pilots")
    test.teardown()
