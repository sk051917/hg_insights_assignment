import csv
import os
import random
from faker import Faker
from datetime import datetime
import pandas as pd

fake = Faker()

class DataGenerator:

    def __init__(
        self,
        num_customers=100,
        num_products=50,
        num_orders=200,
        duplicate_percent=0.05,
        invalid_percent=0.05,
    ):
        self.num_customers = num_customers
        self.num_products = num_products
        self.num_orders = num_orders
        self.duplicate_percent = duplicate_percent
        self.invalid_percent = invalid_percent

        # Root folder for generated data
        self.output_dir = "/opt/datasets"  # <-- updated root folder
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Static folder for lookups (country_codes, department_codes)
        self.static_dir = "/opt/datasets/static_data"
        os.makedirs(self.static_dir, exist_ok=True)
        print("Directories created")
        # Lookup tables
        self.country_codes = [
            {"country_code": "US", "country_name": "United States"},
            {"country_code": "FR", "country_name": "France"},
            {"country_code": "IN", "country_name": "India"},
            {"country_code": "JP", "country_name": "Japan"},
            {"country_code": "DE", "country_name": "Germany"},
            {"country_code": "GB", "country_name": "United Kingdom"},
            {"country_code": "CA", "country_name": "Canada"},
            {"country_code": "AU", "country_name": "Australia"},
            {"country_code": "BR", "country_name": "Brazil"},
            {"country_code": "CN", "country_name": "China"},
            {"country_code": "RU", "country_name": "Russia"},
            {"country_code": "MX", "country_name": "Mexico"},
            {"country_code": "IT", "country_name": "Italy"},
            {"country_code": "ES", "country_name": "Spain"},
            {"country_code": "KR", "country_name": "South Korea"},
            {"country_code": "NL", "country_name": "Netherlands"},
            {"country_code": "SE", "country_name": "Sweden"},
            {"country_code": "CH", "country_name": "Switzerland"},
            {"country_code": "ZA", "country_name": "South Africa"},
            {"country_code": "SG", "country_name": "Singapore"},
            {"country_code": "NZ", "country_name": "New Zealand"},
            {"country_code": "BE", "country_name": "Belgium"},
            {"country_code": "AR", "country_name": "Argentina"},
            {"country_code": "NO", "country_name": "Norway"},
            {"country_code": "DK", "country_name": "Denmark"},
            {"country_code": "FI", "country_name": "Finland"},
            {"country_code": "IE", "country_name": "Ireland"},
            {"country_code": "PT", "country_name": "Portugal"},
            {"country_code": "PL", "country_name": "Poland"},
            {"country_code": "TR", "country_name": "Turkey"},
            {"country_code": "GR", "country_name": "Greece"},
            {"country_code": "IL", "country_name": "Israel"},
            {"country_code": "AE", "country_name": "United Arab Emirates"},
            {"country_code": "SA", "country_name": "Saudi Arabia"},
            {"country_code": "EG", "country_name": "Egypt"},
            {"country_code": "TH", "country_name": "Thailand"},
            {"country_code": "MY", "country_name": "Malaysia"},
            {"country_code": "PH", "country_name": "Philippines"},
        ]

        self.department_codes = [
            {"department_code": "HR", "department_name": "Human Resources"},
            {"department_code": "IT", "department_name": "Information Technology"},
            {"department_code": "FIN", "department_name": "Finance"},
            {"department_code": "SALES", "department_name": "Sales"},
            {"department_code": "MKT", "department_name": "Marketing"},
            {"department_code": "SUPPORT", "department_name": "Customer Support"},
            {"department_code": "OPS", "department_name": "Operations"},
            {"department_code": "RND", "department_name": "Research & Development"},
            {"department_code": "LOG", "department_name": "Logistics"},
            {"department_code": "LEGAL", "department_name": "Legal"},
            {"department_code": "QA", "department_name": "Quality Assurance"},
            {"department_code": "PROC", "department_name": "Procurement"},
            {"department_code": "ENG", "department_name": "Engineering"},
            {"department_code": "ADM", "department_name": "Administration"},
        ]

        # Email domains
        self.email_domains = ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com"]

    def _timestamped_file(self, prefix: str, subfolder: str):

        folder_path = os.path.join(self.output_dir, subfolder)
        os.makedirs(folder_path, exist_ok=True)
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        return os.path.join(folder_path, f"{prefix}_{timestamp}.csv")

    def generate_customers(self):
        customers = []
        for i in range(self.num_customers):
            first_name = fake.first_name()
            last_name = fake.last_name()

            country = (
                random.choice(self.country_codes)
                if random.random() < 0.9
                else {"country_code": f"XX{i}", "country_name": "Unknown"}
            )
            department = (
                random.choice(self.department_codes)
                if random.random() < 0.9
                else {"department_code": f"YY{i}", "department_name": "Unknown"}
            )

            customer = {
                "customer_id": f"CUST{i+1:03d}",
                "first_name": first_name,
                "last_name": last_name,
                "email": self._generate_email(first_name, last_name),  # use new helper
                "phone_number": self._generate_phone_number(),  # new column
                "signup_date": fake.date_between(
                    start_date="-2y", end_date="today"
                ).strftime("%Y-%m-%d"),
                "country_code": country["country_code"],
                "department_code": department["department_code"],
            }
            customers.append(customer)

        # Add duplicates
        for _ in range(int(self.num_customers * self.duplicate_percent)):
            customers.append(random.choice(customers))

        # Add invalid dates
        for _ in range(int(self.num_customers * self.invalid_percent)):
            cust = random.choice(customers)
            cust["signup_date"] = "INVALID_DATE"

        # Save to CSV in customers subfolder inside data_files
        folder_path = os.path.join(self.output_dir, "customers")
        os.makedirs(folder_path, exist_ok=True)
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        filename = os.path.join(folder_path, f"customers_{timestamp}.csv")

        with open(filename, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=customers[0].keys())
            writer.writeheader()
            writer.writerows(customers)

        return filename

    def generate_products(self):
        products = []
        for i in range(self.num_products):
            product = {
                "product_id": f"PROD{i+1:03d}",
                "product_name": fake.word().title(),
                "category": random.choice(["Electronics", "Clothing", "Food", "Books"]),
                "price": round(random.uniform(10, 500), 2),
            }
            products.append(product)

        # Duplicates
        for _ in range(int(self.num_products * self.duplicate_percent)):
            products.append(random.choice(products))

        # Invalid price
        for _ in range(int(self.num_products * self.invalid_percent)):
            prod = random.choice(products)
            prod["price"] = "INVALID"

        filename = self._timestamped_file("products", "products")
        with open(filename, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=products[0].keys())
            writer.writeheader()
            writer.writerows(products)
        return filename

    def generate_orders(self, customers_file, products_file):
        # Read generated customers and products to use IDs

        customers_df = pd.read_csv(customers_file)
        products_df = pd.read_csv(products_file)

        orders = []
        for i in range(self.num_orders):
            customer = customers_df.sample(1).iloc[0]
            product = products_df.sample(1).iloc[0]
            try:
                order_date = fake.date_between(
                    start_date=customer["signup_date"], end_date="today"
                )
            except:
                order_date = fake.date_between(start_date="-2y", end_date="today")
            order = {
                "order_id": f"ORD{i+1:03d}",
                "customer_id": customer["customer_id"],
                "product_id": product["product_id"],
                "order_date": order_date.strftime("%Y-%m-%d"),
                "quantity": random.randint(1, 10),
            }
            orders.append(order)

        # Duplicates
        for _ in range(int(self.num_orders * self.duplicate_percent)):
            orders.append(random.choice(orders))

        filename = self._timestamped_file("orders", "orders")
        fieldnames = ["order_id", "customer_id", "product_id", "order_date", "quantity"]
        with open(filename, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for order in orders:
                if random.random() < self.invalid_percent:
                    corrupt_order = order.copy()
                    corrupt_order.pop(random.choice(list(corrupt_order.keys())))
                    writer.writerow(corrupt_order)
                else:
                    writer.writerow(order)
        return filename

    def generate_lookup_tables(self):
        # Country codes
        country_file = os.path.join(self.static_dir, "country_codes.csv")
        with open(country_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=self.country_codes[0].keys())
            writer.writeheader()
            writer.writerows(self.country_codes)

        # Department codes
        department_file = os.path.join(self.static_dir, "department_codes.csv")
        with open(department_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=self.department_codes[0].keys())
            writer.writeheader()
            writer.writerows(self.department_codes)

        return country_file, department_file

    def generate_all(self):
        cust_file = self.generate_customers()
        prod_file = self.generate_products()
        orders_file = self.generate_orders(cust_file, prod_file)
        country_file, dept_file = self.generate_lookup_tables()
        print("Generated files:")
        print(cust_file)
        print(prod_file)
        print(orders_file)
        print(country_file)
        print(dept_file)

    def _generate_email(self, first_name, last_name):
        domain = random.choice(self.email_domains)
        return f"{first_name.lower()}.{last_name.lower()}@{domain}"

    def _generate_phone_number(self):
        return fake.phone_number()


# -------------------
# Usage
# -------------------
if __name__ == "__main__":
    generator = DataGenerator(
        num_customers=1000,
        num_products=100,
        num_orders=20000,
        duplicate_percent=0.2,
        invalid_percent=0.05,
    )
    generator.generate_all()
