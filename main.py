import json
import logging
import os
import sqlite3
from typing import Any, Dict, List, Optional, Tuple

from tabulate import tabulate


class DataClass:
    """
    A data container class for table operations.

    Attributes:
        target_table (str): The target database table name
        attrs (dict): Dictionary of attribute key-value pairs
    """

    def __init__(self, target_table: str, attrs: Dict[str, Any] = None):
        """
        Initialize DataClass with target table and attributes.

        Args:
            target_table (str): The name of the target database table
            attrs (Dict[str, Any], optional): Attributes dictionary. Defaults to None.
        """
        self.target_table = target_table
        self.attrs = attrs or {}

    def __setitem__(self, k: str, v: Any) -> None:
        """Set attribute value."""
        self.attrs[k] = v

    def __getitem__(self, k: str) -> Any:
        """Get attribute value."""
        return self.attrs[k]

    def __delitem__(self, k: str) -> None:
        """Delete attribute."""
        del self.attrs[k]

    def get_columns(self) -> List[str]:
        """
        Get list of column names in lowercase.

        Returns:
            List[str]: List of lowercase column names
        """
        return [col.lower() for col in self.attrs.keys()]

    def get_values(self) -> Tuple[Any, ...]:
        """
        Get tuple of attribute values.

        Returns:
            Tuple[Any, ...]: Tuple of attribute values
        """
        return tuple(self.attrs.values())

    def __str__(self) -> str:
        """String representation of DataClass."""
        return f"[{self.target_table}]: \n{json.dumps(self.attrs, indent=1)}"


def atoi(s: str) -> Any:
    """
    Convert string to integer if it's a digit, otherwise return as string.

    Args:
        s (str): Input string

    Returns:
        Any: Integer if string is digit, otherwise original string
    """
    return int(s) if s.isdigit() else s


class SQLQueries:
    """Container class for SQL query templates and constants."""

    def __init__(self):
        """Initialize SQL query templates."""
        self.sql_create_base = {
            "pilots": """
                CREATE TABLE IF NOT EXISTS Pilots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
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

        # Common SQL templates
        self.sql_insert = "INSERT INTO {table} ({columns}) VALUES ({values});"
        self.sql_search = "SELECT * FROM {table} WHERE {condition};"
        self.sql_update = "UPDATE {table} SET {field} = ? WHERE {condition};"
        self.sql_delete = "DELETE FROM {table} WHERE {condition};"
        self.sql_drop = "DROP TABLE IF EXISTS {table};"
        self.sql_group = "SELECT COUNT(*), {columns} FROM {table} GROUP BY {columns};"

        # Specialized queries
        self.sql_flight_count = """
            SELECT 
                a.{group_column}, 
                COUNT(fl.id) 
            FROM 
                {table_a} a 
                LEFT JOIN Flights fl 
                ON a.id = fl.{a_id} 
            WHERE {condition}
            GROUP BY a.{group_column};
        """

        self.sql_pilot_schedule = """
            SELECT
                p.name,
                f.flight_number,
                f.departure_time,
                f.arrival_time
            FROM 
                Pilots p
            LEFT JOIN Flights f
                ON p.id = f.pilot_id
            WHERE p.id = ?;
        """

    def get_table_names(self) -> List[str]:
        """
        Get list of available table names.

        Returns:
            List[str]: List of table names
        """
        return list(self.sql_create_base.keys())


class DBOperations(SQLQueries):
    """
    Database operations class handling all database interactions.

    Attributes:
        current_dir (str): Current directory path
        dbname (str): Database file name
    """

    def __init__(self, name: str):
        """
        Initialize database operations with project name.

        Args:
            name (str): Project name for database and log files
        """
        super().__init__()
        self.current_dir = os.path.dirname(os.path.abspath(__file__))
        self.dbname = f"{name.capitalize()}.db"

        # Setup logging
        self._setup_logging(name)

        # Initialize database if it doesn't exist
        if not os.path.exists(self.dbname):
            self._initialize_database()

    def _setup_logging(self, name: str) -> None:
        """
        Setup logging configuration.

        Args:
            name (str): Project name for log file
        """
        logging.basicConfig(
            filename=f"flight_management_{name}.log",
            level=logging.DEBUG,
            format="%(asctime)s - %(levelname)s - %(message)s",
        )

    def _initialize_database(self) -> None:
        """Initialize database tables and populate with initial data."""
        try:
            self.initialize_tables()
        except Exception as e:
            self._handle_error("Database initialization failed", e)

    def _handle_error(self, message: str, exception: Exception) -> None:
        """
        Unified error handling.

        Args:
            message (str): Error message
            exception (Exception): Exception object
        """
        print("Error, operation aborted")
        logging.error(f"{message}: {exception}")

    def get_connection(self) -> sqlite3.Connection:
        """
        Get database connection.

        Returns:
            sqlite3.Connection: Database connection object
        """
        return sqlite3.connect(self.dbname)

    def initialize_tables(self) -> None:
        """Initialize all tables with data."""
        for table_name in self.get_table_names():
            self.create_table(table_name)
            self.populate_table(table_name)

    def create_table(self, table_name: str) -> None:
        """
        Create a database table.

        Args:
            table_name (str): Name of the table to create
        """
        with self.get_connection() as conn:
            try:
                cursor = conn.cursor()
                cursor.execute(self.sql_create_base[table_name.lower()])
                conn.commit()
                logging.info(f"Table {table_name} created successfully")
            except KeyError:
                logging.error(f"Cannot find DDL for table {table_name}")
            except Exception as e:
                self._handle_error(f"Failed to create table {table_name}", e)

    def populate_table(self, table_name: str) -> None:
        """
        Populate table with data from CSV file.

        Args:
            table_name (str): Name of the table to populate
        """
        try:
            file_path = os.path.join(
                self.current_dir, "src", f"{table_name.lower()}.csv"
            )

            with open(file_path, "r") as f:
                header = [col.lower() for col in f.readline().strip().split(",")]

                for line in f.readlines():
                    fields = [field.strip() for field in line.strip().split(",")]
                    data = DataClass(table_name, dict(zip(header, fields)))
                    self._insert_data(data)

            logging.info(f"Successfully populated table {table_name}")
        except Exception as e:
            self._handle_error(f"Failed to populate table {table_name}", e)

    def drop_table(self, table_name: str) -> None:
        """
        Drop a database table.

        Args:
            table_name (str): Name of the table to drop
        """
        with self.get_connection() as conn:
            try:
                cursor = conn.cursor()
                query = self.sql_drop.format(table=table_name.capitalize())
                logging.debug(f"Running:\n{query}")
                cursor.execute(query)
                conn.commit()
                logging.info(f"Table {table_name} dropped successfully")
            except Exception as e:
                self._handle_error(f"Failed to drop table {table_name}", e)

    def get_table_columns(self, table_name: str) -> List[str]:
        """
        Get column names for a table.

        Args:
            table_name (str): Name of the table

        Returns:
            List[str]: List of column names
        """
        with self.get_connection() as conn:
            try:
                cursor = conn.cursor()
                cursor.execute(f"PRAGMA table_info({table_name});")
                return [col[1] for col in cursor.fetchall()]
            except Exception as e:
                self._handle_error(f"Failed to get columns for {table_name}", e)
                return []

    def _execute_query(self, query: str, params: Tuple = None) -> Optional[List[Tuple]]:
        """
        Execute a SQL query and return results.

        Args:
            query (str): SQL query string
            params (Tuple, optional): Query parameters

        Returns:
            Optional[List[Tuple]]: Query results or None if failed
        """
        with self.get_connection() as conn:
            try:
                cursor = conn.cursor()
                logging.debug(f"Running:\n{query} with params: {params}")

                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)

                return cursor.fetchall()
            except Exception as e:
                self._handle_error("Query execution failed", e)
                return None

    def _insert_data(self, data: DataClass) -> None:
        """
        Insert data into database.

        Args:
            data (DataClass): Data object to insert
        """
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
                self._handle_error("Data insertion failed", e)

    def _get_user_input(self, prompt: str, allow_exit: bool = True) -> Optional[str]:
        """
        Get user input with optional exit handling.

        Args:
            prompt (str): Input prompt
            allow_exit (bool): Whether to allow EXIT command

        Returns:
            Optional[str]: User input or None if cancelled
        """
        value = input(prompt).strip()
        if allow_exit and value.upper() == "EXIT":
            print("Operation cancelled.")
            return None
        return value

    def _get_column_choice(
        self, columns: List[str], exclude_cols: List[str] = None
    ) -> Optional[str]:
        """
        Get user choice for column selection.

        Args:
            columns (List[str]): Available columns
            exclude_cols (List[str], optional): Columns to exclude from selection

        Returns:
            Optional[str]: Selected column name or None if cancelled
        """
        exclude_cols = exclude_cols or ["id"]
        available_cols = {
            i: col for i, col in enumerate(columns, 1) if col not in exclude_cols
        }

        if not available_cols:
            print("No selectable columns found.")
            return None

        print("Choose a column:")
        for k, v in available_cols.items():
            print(f"{k}) {v}")

        choice = self._get_user_input("Enter choice: ")
        if choice is None:
            return None

        try:
            return available_cols.get(int(choice))
        except ValueError:
            print("Invalid selection. Please enter a valid number.")
            return None

    def insert_data(self, table_name: str) -> None:
        """
        Interactive data insertion for a table.

        Args:
            table_name (str): Name of the table
        """
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

            value = self._get_user_input(f"{col}: ")
            if value is None:
                return

            if value.strip():
                attrs[col] = atoi(value)

        if not attrs:
            print("No data entered.")
            return

        data = DataClass(target_table=table_name, attrs=attrs)
        self._insert_data(data)
        self.search_data(
            table_name, id="(SELECT MAX(id) FROM {table})".format(table=table_name)
        )

    def search_data(
        self, table_name: str, show_all: bool = False, id: Optional[str] = None
    ) -> None:
        """
        Search and display data from a table.

        Args:
            table_name (str): Name of the table
            show_all (bool): Whether to show all records
            id (Optional[str]): Specific ID to search for
        """
        try:
            if show_all:
                query = self.sql_search.format(table=table_name, condition="1=1")
                data = self._execute_query(query)
            elif id is not None:
                query = self.sql_search.format(table=table_name, condition=f"id = {id}")
                data = self._execute_query(query)
            else:
                columns = self.get_table_columns(table_name)
                filter_column = self._get_column_choice(columns)
                if not filter_column:
                    return

                value = self._get_user_input("Enter a value to filter by:\n")
                if value is None:
                    return

                value = atoi(value)
                query = self.sql_search.format(
                    table=table_name, condition=f"{filter_column} = {value}"
                )
                data = self._execute_query(query)

            if data is not None:
                self.show(table_name=table_name, data=data)

        except Exception as e:
            self._handle_error("Search operation failed", e)

    def update_data(self, table_name: str) -> None:
        """
        Interactive data update for a table record.

        Args:
            table_name (str): Name of the table
        """
        columns = self.get_table_columns(table_name)
        if "id" not in columns:
            print("No 'id' column found in this table.")
            return

        print("Enter `EXIT` to cancel.")
        record_id = self._get_user_input("Enter the id of the record to update:\n")
        if record_id is None:
            return

        record_id = atoi(record_id)

        update_column = self._get_column_choice(columns)
        if not update_column:
            return

        new_value = self._get_user_input(f"Enter new value for {update_column}:\n")
        if new_value is None:
            return

        with self.get_connection() as conn:
            try:
                cursor = conn.cursor()
                query = self.sql_update.format(
                    table=table_name, field=update_column, condition="id = ?"
                )
                logging.debug(
                    f"Running:\n{query} with value {new_value} for id {record_id}"
                )
                cursor.execute(query, (new_value, record_id))
                conn.commit()
                print("Record updated successfully.")
                self.search_data(table_name, id=str(record_id))
            except Exception as e:
                self._handle_error("Update operation failed", e)

    def delete_data(self, table_name: str) -> None:
        """
        Interactive data deletion for a table record.

        Args:
            table_name (str): Name of the table
        """
        columns = self.get_table_columns(table_name)
        if "id" not in columns:
            print("No 'id' column found in this table.")
            return

        record_id = self._get_user_input("Enter the id of the record to delete:\n")
        if record_id is None:
            return

        record_id = atoi(record_id)

        with self.get_connection() as conn:
            try:
                cursor = conn.cursor()
                query = self.sql_delete.format(table=table_name, condition="id = ?")
                logging.debug(f"Running:\n{query} for id {record_id}")
                cursor.execute(query, (record_id,))
                conn.commit()

                if cursor.rowcount > 0:
                    print(f"Successfully deleted from table {table_name}")
                else:
                    print("Record not found.")
            except Exception as e:
                self._handle_error("Delete operation failed", e)

    def group_data(self, table_name: str) -> None:
        """
        Group data by a column and display counts.

        Args:
            table_name (str): Name of the table
        """
        columns = self.get_table_columns(table_name)
        if not columns:
            print(f"Table '{table_name}' does not exist or has no columns.")
            return

        group_column = self._get_column_choice(columns)
        if not group_column:
            return

        query = self.sql_group.format(columns=group_column, table=table_name)
        data = self._execute_query(query)

        if data is not None:
            self.show(data=data, headers=["count", group_column])

    def flight_summary(self, group_by: str, condition: str = "1=1") -> None:
        """
        Generate flight summary grouped by specified criteria.

        Args:
            group_by (str): Grouping criteria ('Pilot', 'Source', 'Destination')
            condition (str): WHERE clause condition
        """
        group_config = {
            "Pilot": ("Pilots", "pilot_id", "name"),
            "Source": ("Destinations", "source_id", "airport_code"),
            "Destination": ("Destinations", "destination_id", "airport_code"),
        }

        if group_by not in group_config:
            print(f"Grouping by '{group_by}' is not supported.")
            return

        table_a, a_id, group_column = group_config[group_by]

        query = self.sql_flight_count.format(
            group_column=group_column, table_a=table_a, a_id=a_id, condition=condition
        )

        data = self._execute_query(query)

        if data:
            self.show(headers=[group_column, "NumFlights"], data=data)
        else:
            print("No summary data found.")

    def get_pilot_schedule(self, pilot_id: int) -> Optional[List[Tuple]]:
        """
        Get pilot schedule by pilot ID.

        Args:
            pilot_id (int): Pilot ID

        Returns:
            Optional[List[Tuple]]: Pilot schedule data or None if failed
        """
        return self._execute_query(self.sql_pilot_schedule, (pilot_id,))

    def select_all(self, table_name: str) -> None:
        """
        Select and display all records from a table.

        Args:
            table_name (str): Name of the table
        """
        self.search_data(table_name=table_name, show_all=True)

    def show(
        self,
        data: List[Tuple],
        headers: Optional[List[str]] = None,
        table_name: Optional[str] = None,
    ) -> None:
        """
        Display data in tabular format.

        Args:
            data (List[Tuple]): Data to display
            headers (Optional[List[str]]): Column headers
            table_name (Optional[str]): Table name for header lookup
        """
        if not headers:
            if table_name:
                headers = self.get_table_columns(table_name)
            else:
                logging.error("Error. No headers or table provided for lookup")
                print("Error visualizing your data")
                return

        print(tabulate(data, headers=headers, tablefmt="grid"))

    def teardown(self) -> None:
        """Remove the database file after confirmation."""
        if input("Are you sure? (y/n): ").lower() == "y":
            db_path = os.path.join(self.current_dir, self.dbname)
            if os.path.exists(db_path):
                os.remove(db_path)
                print("Database removed successfully.")
            else:
                print("Database file not found.")


class DBUI:
    """
    Database User Interface class for interactive menu system.

    Attributes:
        driver (DBOperations): Database operations driver
    """

    def __init__(self, driver: DBOperations):
        """
        Initialize UI with database operations driver.

        Args:
            driver (DBOperations): Database operations instance
        """
        self.driver = driver
        self.entities = {"1": "flights", "2": "pilots", "3": "destinations"}
        self.group_map = {"1": "Pilot", "2": "Source", "3": "Destination"}

    def main_menu(self) -> None:
        """Display and handle main menu interactions."""
        while True:
            print("\nFlight Management System Menu")
            print("1) Flights")
            print("2) Pilots")
            print("3) Destinations")
            print("4) Flight Summary")
            print("5) Custom Analysis")
            print("6) Exit")

            choice = input("Select an option: ").strip()

            if choice in self.entities:
                self.sub_menu(entity=self.entities[choice])
            elif choice == "4":
                self.flight_summary()
            elif choice == "5":
                self.custom_analysis()
            elif choice == "6":
                print("Exiting...")
                break
            else:
                print("Invalid choice. Please try again.")

    def sub_menu(self, entity: str) -> None:
        """
        Display and handle sub-menu for specific entity.

        Args:
            entity (str): Entity name (flights, pilots, destinations)
        """
        print(f"\n{entity.capitalize()} Menu")
        print(f"1) Add New {entity.capitalize()}")
        print(f"2) View {entity.capitalize()}")
        print(f"3) Search {entity.capitalize()}")
        print(f"4) Update {entity.capitalize()} Info")

        choice = input("Select an option: ").strip()

        if choice == "1":
            self.driver.insert_data(entity)
        elif choice == "2":
            self.driver.search_data(entity, show_all=True)
        elif choice == "3":
            self.driver.search_data(entity)
        elif choice == "4":
            self.driver.update_data(entity)
        else:
            print("Invalid choice.")

    def custom_analysis(self) -> None:
        """Display and handle custom analysis menu."""
        print("\nCustom Analysis Options")
        print("1) Pilot Schedule")
        print("2) Back to Main Menu")

        choice = input("Enter your choice: ").strip()

        if choice == "1":
            self.pilot_schedule()
        elif choice == "2":
            return
        else:
            print("Invalid option.")

    def flight_summary(self) -> None:
        """Display and handle flight summary menu."""
        print("\nFlight Summary")
        print("1) By Pilot")
        print("2) By Source")
        print("3) By Destination")

        choice = input("Enter choice (or 'EXIT' to cancel): ").strip()

        if choice.upper() == "EXIT":
            print("Cancelled.")
            return

        group_by = self.group_map.get(choice)
        if not group_by:
            print("Invalid selection.")
            return

        condition = (
            input("Enter optional WHERE clause (e.g. fl.status = 'on-time'): ").strip()
            or "1=1"
        )
        self.driver.flight_summary(group_by=group_by, condition=condition)

    def pilot_schedule(self) -> None:
        """Display pilot schedule based on pilot ID."""
        try:
            pilot_id = int(input("Enter the pilot's ID: ").strip())
        except ValueError:
            print("Invalid ID format. Must be an integer.")
            return

        data = self.driver.get_pilot_schedule(pilot_id)

        if data:
            self.driver.show(
                data=data,
                headers=["Pilot Name", "Flight Number", "Departure", "Arrival"],
            )
        else:
            print("No scheduled flights found for this pilot.")


def main() -> None:
    """Main entry point for the application."""
    project_name = input("Enter project name: ")
    if not project_name.strip():
        print("Project name cannot be empty.")
        return

    try:
        db_operations = DBOperations(name=project_name)
        ui = DBUI(db_operations)
        ui.main_menu()
    except Exception as e:
        print(f"Application failed to start: {e}")
        logging.error(f"Application startup failed: {e}")


if __name__ == "__main__":
    main()
