import os
import copy
import time
import sqlite3
import requests
import socket
from bs4 import BeautifulSoup
from datetime import datetime
from flask import Flask, request, jsonify
from multiprocessing import Process
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger


class DataBaseManager:
    """
    Class responsible for managing the SQLite database.
    """
    def __init__(self):
        self.rates_table_name = "currency_exchange_rates"
        self.date_table_name = "recorded_dates"
        self.database_name = "currency_exchange.db"
        self.exchange_rates = dict()
        self.date = None
        self.date_format = ["%d %B %Y", "%Y-%m-%d"]

    def create_table(self, table_name):
        """
        Create a new table in the database.

        Args:
            table_name (str): The name of the table to be created.
        """
        conn = sqlite3.connect(self.database_name)
        cursor = conn.cursor()
        drop_table_query = f"DROP TABLE IF EXISTS {table_name}"
        cursor.execute(drop_table_query)

        if table_name == self.rates_table_name:
            create_table_query = f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INTEGER PRIMARY KEY,
                    currency TEXT,
                    previous_rates REAL DEFAULT NULL,
                    latest_rates REAL DEFAULT NULL
                )
            '''
        else:
            create_table_query = f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INTEGER PRIMARY KEY,
                    type TEXT,
                    date DATE DEFAULT NULL
                )
            '''
        cursor.execute(create_table_query)
        conn.commit()

    def insert_data(self):
        """
        Insert currency exchange rates and dates into the respective tables in the database.
        """
        conn = sqlite3.connect(self.database_name)
        cursor = conn.cursor()
        if not self.table_exists():
            print(f"The table '{self.rates_table_name}' does not exist.")
            self.create_table(self.rates_table_name)
            self.create_table(self.date_table_name)
            try:
                cursor.execute(
                    f"INSERT INTO {self.date_table_name} (type, date) "
                    "VALUES ('previous_date', NULL), ('latest_date', NULL)"
                )
                conn.commit()
            except sqlite3.Error as e:
                print(f"Error in insert: {e}")
                conn.rollback()

        cursor.execute(f'SELECT date FROM {self.date_table_name} WHERE type = "latest_date" LIMIT 1')
        date_fetched = cursor.fetchone()[0]

        if date_fetched != self.date:
            for k, v in self.exchange_rates.items():
                query = f'SELECT latest_rates FROM {self.rates_table_name} WHERE currency = ?'
                result = self.execute_query(query, (k,))
                if result:
                    query = f'''
                        UPDATE {self.rates_table_name}
                        SET previous_rates = latest_rates,
                            latest_rates = ?
                        WHERE currency = ?
                    '''
                    self.execute_query(query, (v, k))
                else:
                    query = f'''
                        INSERT INTO {self.rates_table_name} (currency, previous_rates, latest_rates)
                        VALUES (?, NULL, ?)
                    '''
                    self.execute_query(query, (k, v))

            print(f"currency rates inserted successfully")
            query = f"UPDATE {self.date_table_name} SET date = ? WHERE type = 'previous_date'"
            self.execute_query(query, (date_fetched,))
            query = f"UPDATE {self.date_table_name} SET date = ? WHERE type = 'latest_date'"
            self.execute_query(query, (self.date,))
            print(f"dates inserted in data table successfully")
            self.display_table_data(self.rates_table_name)
            self.display_table_data(self.date_table_name)
        else:
            print("date is same")

    def table_exists(self):
        """
        Check if a table exists in the database.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        conn = sqlite3.connect(self.database_name)
        cursor = conn.cursor()
        try:
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{self.rates_table_name}'")
            result = cursor.fetchone()
            if result:
                return True
            else:
                return False
        except sqlite3.Error:
            return False

    def display_table_data(self, table_name):
        """
        Display the data in a specific table.

        Args:
            table_name (str): The name of the table to display.
        """
        conn = sqlite3.connect(self.database_name)
        cursor = conn.cursor()
        try:
            cursor.execute(f"SELECT * FROM {table_name}")
            data = cursor.fetchall()
            for row in data:
                print(row)
        except sqlite3.Error as e:
            print(f"Error in display {table_name}: {e}")

    def execute_query(self, query, params=()):
        """
        Execute a SQL query on the database.

        Args:
            query (str): The SQL query to execute.
            params (tuple): The parameters to be used in the query.

        Returns:
            list: The result of the query.
        """
        conn = sqlite3.connect(self.database_name)
        cursor = conn.cursor()
        cursor.execute(query, params)
        result = cursor.fetchall()
        conn.commit()
        return result

    def get_latest_rate(self, currency=None):
        """
        Get the latest exchange rate for a specific currency if currency provided, otherwise it returns latest rates for
         all currencies.

        Args:
            currency (str, optional): The currency code for which to get the rate. If None, returns rates for all
            currencies.

        Returns:
            dict: A dictionary with currency codes as keys and their latest exchange rates as values.
        """
        if currency:
            query = f'SELECT latest_rates FROM {self.rates_table_name} WHERE currency = ?'
            result = self.execute_query(query, (currency,))
            if result:
                return {currency: result[0][0]}
            else:
                return None
        else:
            query = f'SELECT currency, latest_rates FROM {self.rates_table_name}'
            data = self.execute_query(query)
            return {currency: rate for currency, rate in data}

    def get_previous_rate(self, currency=None):
        """
        Get the previous exchange rate for a specific currency if currency provided, otherwise it returns previous rates
        for all currencies..

        Args:
            currency (str, optional): The currency code for which to get the rate. If None, returns rates for all
            currencies.

        Returns:
            dict: A dictionary with currency codes as keys and their previous exchange rates as values.
        """
        if currency:
            query = f'SELECT previous_rates FROM {self.rates_table_name} WHERE currency = ?'
            result = self.execute_query(query, (currency,))
            if result:
                return {currency: result[0][0]}
            else:
                return None
        else:
            query = f'SELECT currency, previous_rates FROM {self.rates_table_name}'
            data = self.execute_query(query)
            return {currency: rate for currency, rate in data}


class CurrencyExchange(DataBaseManager):
    """
    Class for fetching currency exchange rates from a website and providing API endpoints.
    """
    def __init__(self):
        super().__init__()
        self.url = ("https://www.ecb.europa.eu/stats/policy_and_exchange_rates/"
                    "euro_reference_exchange_rates/html/index.en.html")
        self.app = Flask(__name__)

    def scrap_currency_rates(self):
        """
        Scrape the currency exchange rates from the website and send them to insert_data function
        in DataBaseManager class to store in the database.
        """
        response = requests.get(self.url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            self.date = soup.find('div', {'class': 'jumbo-box'}).find('div', {'class': 'upper'}).h3.text.strip()
            date_obj = datetime.strptime(self.date, self.date_format[0])
            self.date = date_obj.strftime(self.date_format[1])
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Date: {self.date}\n")
            currency_table = soup.find("table", class_="forextable")

            if currency_table:
                for row in currency_table.find_all("tr"):
                    columns = row.find_all("td")
                    if len(columns) >= 3:
                        currency = columns[0].text.strip()
                        rate = columns[2].text.strip()
                        self.exchange_rates.update({currency: rate})

                self.insert_data()
            else:
                print("Table with class 'forextable' not found on the page.")
        else:
            print("Failed to fetch the webpage. Status code:", response.status_code)

    def calculate_rate_change(self, current_rate, previous_rate):
        """
        Calculate the rate changes between current and previous exchange rates.

        Args:
            current_rate (dict): A dictionary with currency codes as keys and their latest exchange rates as values.
            previous_rate (dict): A dictionary with currency codes as keys and their previous exchange rates as values.

        Returns:
            dict: A dictionary with currency codes as keys and rate change information as values.
        """
        if current_rate is not None and previous_rate is not None:
            result = dict()
            for key, cur_val in current_rate.items():
                prev_val = copy.deepcopy(previous_rate.get(key))
                if prev_val:
                    result.update(
                        {key: {"latest_rate": cur_val, "previous_rate": prev_val, "rate_changes": cur_val - prev_val}})
                else:
                    result.update(
                        {key: {"latest_rate": cur_val, "previous_rate": prev_val,
                               "rate_changes": "Not have enough data"}})
            return result
        else:
            return None

    def schedule_job(self):
        """
        Schedule the job to scrape currency rates periodically and update the database.
        one job is scheduled at 16:05 Europe/Berlin Timezone and second is scheduled at 17:35 Europe/Berlin
        This is because it is mentioned in the website that they update the data around 16:00 CET (Europe/Berlin)
        but in case they couldn't update on time, so we use their closing time which is also mentioned in the website
        and that is 17:30 CET (Europe/Berlin)
        """
        self.scrap_currency_rates()
        scheduler = BackgroundScheduler()
        berlin_tz = 'Europe/Berlin'

        scheduler.add_job(self.scrap_currency_rates, trigger='cron', hour=16, minute=5, timezone=berlin_tz)
        scheduler.add_job(self.scrap_currency_rates, trigger='cron', hour=17, minute=35, timezone=berlin_tz)
        scheduler.start()

        try:
            while True:
                time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            # Shut down the scheduler when exiting the program
            scheduler.shutdown()

    def setup_endpoints(self, host='127.0.0.1', port=5000, debug=False):
        """
        Set up API endpoints using Flask.

        Args:
            host (str, optional): The host address for the Flask app. Default is '127.0.0.1'.
            port (int, optional): The port number for the Flask app. Default is 5000.
            debug (bool, optional): Enable debug mode for Flask. Default is False.
        """

        @self.app.route('/current_rates', methods=['GET'])
        def get_current_rates():
            currency = request.args.get('currency')
            result = self.get_latest_rate(currency)
            if result is not None:
                return jsonify(result), 200
            else:
                return jsonify({'error': 'No data found for the specified currency.'}), 404

        @self.app.route('/rate_changes', methods=['GET'])
        def get_rate_changes():
            currency = request.args.get('currency')
            latest_rate = self.get_latest_rate(currency)
            previous_rate = self.get_previous_rate(currency)
            rate_change = self.calculate_rate_change(latest_rate, previous_rate)
            if rate_change is not None:
                return jsonify(rate_change), 200
            else:
                return jsonify({'error': 'No data found for the specified currency.'}), 404

        try:
            self.app.run(host=host, port=port, debug=debug, ssl_context='adhoc')
        except Exception as e:
            print(f"Error in setup endpoints: {e}")

    def get_system_ip(self):
        try:
            # Get the IP address of the system
            host_name = socket.gethostname()
            ip_address = socket.gethostbyname(host_name)
            return ip_address
        except socket.error as e:
            print(f"Error occurred while getting the IP address: {e}")
            return None

    def run(self):
        """
        Run the currency exchange application.

        The function first attempts to obtain the system's IP address using the 'get_system_ip' method. If successful,
        it uses the IP address to set up the Flask app to listen on that IP. If the IP address cannot be retrieved,
        the Flask app will listen on the default IP '127.0.0.1'.

        Launches a background job to periodically scrape currency exchange rates and update the database, and starts a
        Flask app to serve API endpoints for current rates and rate changes.
        """

        ip = self.get_system_ip()
        if not ip:
            ip = "127.0.0.1"

        process_list = list()
        process_list.append(Process(name="latest_currency_rates_job", target=self.schedule_job))
        process_list.append(Process(name="flask_app", target=self.setup_endpoints, args=(ip,)))

        for process in process_list:
            process.start()

        for process in process_list:
            process.join()


if __name__ == "__main__":
    currency_exchange = CurrencyExchange()
    currency_exchange.run()
