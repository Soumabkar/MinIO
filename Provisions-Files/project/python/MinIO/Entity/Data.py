import logging
from datetime import datetime, timedelta
from typing import Optional
import pandas as pd
from faker import Faker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)
fake = Faker("fr_FR")

class Customers:

    def generate_customers(self, n: int = 500) -> pd.DataFrame:
        """Génère un DataFrame de clients fictifs."""
        rows = []
        for i in range(1, n + 1):
            rows.append({
                "customer_id":  i,
                "first_name":   fake.first_name(),
                "last_name":    fake.last_name(),
                "email":        fake.email(),
                "country":      fake.country_code(representation="alpha-2"),
                "city":         fake.city(),
                "created_at":   fake.date_time_between(start_date="-2y", end_date="now"),
            })
        df = pd.DataFrame(rows)
        df["created_at"] = pd.to_datetime(df["created_at"])
        return df
    
    
class Products:

    def generate_products(self, n: int = 100) -> pd.DataFrame:
        """Génère un catalogue de produits."""
        categories = ["Electronics", "Clothing", "Food", "Books", "Sports", "Home"]
        rows = []
        for i in range(1, n + 1):
            rows.append({
                "product_id":   i,
                "name":         fake.catch_phrase(),
                "category":     fake.random_element(categories),
                "price":        round(fake.pyfloat(min_value=5, max_value=500), 2),
                "stock":        fake.random_int(min=0, max=1000),
            })
        return pd.DataFrame(rows)
    
class Orders:

    def generate_orders(self, n: int = 2000, max_customer_id: int = 500, max_product_id: int = 100, ) -> pd.DataFrame:
        """Génère des commandes avec partitionnement par année/mois."""
        statuses = ["COMPLETED", "PENDING", "CANCELLED", "REFUNDED"]
        rows = []
        base_date = datetime.now() - timedelta(days=365 * 2)
        for i in range(1, n + 1):
            order_date = fake.date_time_between(start_date=base_date, end_date="now")
            rows.append({
                "order_id":     i,
                "customer_id":  fake.random_int(min=1, max=max_customer_id),
                "product_id":   fake.random_int(min=1, max=max_product_id),
                "quantity":     fake.random_int(min=1, max=10),
                "unit_price":   round(fake.pyfloat(min_value=5, max_value=500), 2),
                "status":       fake.random_element(statuses),
                "order_date":   order_date,
                "year":         order_date.year,
                "month":        order_date.month,
            })
        df = pd.DataFrame(rows)
        df["total_amount"] = df["quantity"] * df["unit_price"]
        df["order_date"] = pd.to_datetime(df["order_date"])
        return df