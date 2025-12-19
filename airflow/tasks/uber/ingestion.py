import pandas as pd
import numpy as np
import random
import os
import io
from datetime import datetime, timedelta
from minio import Minio

# --- CẤU HÌNH ---
# Trong Docker Airflow, phải gọi service là 'minio' chứ không phải 'localhost'
MINIO_CONF = {
    "endpoint": "minio:9000",      
    "access_key": "admin",
    "secret_key": "password",
    "secure": False
}
BUCKET_NAME = "warehouse" 
OUTPUT_PREFIX = "bronze1/uber_booking" 

class UberDataGenerator:
    def __init__(self, source_file):
        self.source_file = source_file
        self.locations = []
        self.vehicle_types = []
        self.cancel_reasons_cust = []
        self.cancel_reasons_driver = []
        self.payment_methods = ['Cash', 'UPI', 'Credit Card', 'Debit Card', 'Uber Wallet']
        
        self._learn_from_sample()

    def _learn_from_sample(self):
        if os.path.exists(self.source_file):
            print(f"Reading reference data from {self.source_file}...")
            try:
                df = pd.read_csv(self.source_file)
                self.locations = df['Pickup Location'].dropna().unique().tolist()
                self.vehicle_types = df['Vehicle Type'].dropna().unique().tolist()
                self.cancel_reasons_cust = df['Reason for cancelling by Customer'].dropna().unique().tolist()
                self.cancel_reasons_driver = df['Driver Cancellation Reason'].dropna().unique().tolist()
            except Exception as e:
                print(f"Warning: Could not read sample file ({e}). Using defaults.")
        else:
            print(f"Sample file not found at: {self.source_file}")
            # Fallback defaults
            self.locations = ['District 1', 'District 7', 'Airport']
            self.vehicle_types = ['Auto', 'Bike', 'Go Sedan']

    def _get_pricing_logic(self, vehicle, distance, is_peak_hour):
        prices = {
            'Bike': {'base': 10, 'per_km': 5},
            'eBike': {'base': 12, 'per_km': 5},
            'Auto': {'base': 15, 'per_km': 8},
            'Go Mini': {'base': 20, 'per_km': 10},
            'Go Sedan': {'base': 30, 'per_km': 12},
            'UberXL': {'base': 50, 'per_km': 18},
            'Premier Sedan': {'base': 60, 'per_km': 25}
        }
        cfg = prices.get(vehicle, {'base': 20, 'per_km': 10})
        amount = cfg['base'] + (distance * cfg['per_km'])
        if is_peak_hour:
            amount *= random.uniform(1.2, 1.8)
        return int(amount)

    def generate_day(self, target_date_str, num_rows):
        print(f"Generating {num_rows} bookings for {target_date_str}...")
        target_date = datetime.strptime(target_date_str, "%Y-%m-%d")
        data = []
        
        for _ in range(num_rows):
            # Logic Thời gian
            time_segment = np.random.choice([0, 1, 2, 3, 4], p=[0.1, 0.25, 0.2, 0.3, 0.15])
            if time_segment == 1: # 7-9h
                hour = random.randint(7, 9)
                is_peak = True
            elif time_segment == 3: # 16-19h
                hour = random.randint(16, 19)
                is_peak = True
            else:
                hour = random.choice(list(set(range(0, 24)) - set([7,8,9,16,17,18,19])))
                is_peak = False
                
            booking_time = target_date + timedelta(hours=hour, minutes=random.randint(0, 59), seconds=random.randint(0, 59))
            
            vehicle = random.choice(self.vehicle_types)
            distance = round(random.uniform(1.5, 35.0), 2)
            pickup = random.choice(self.locations)
            drop = random.choice(self.locations)
            while drop == pickup: drop = random.choice(self.locations)
            
            status = np.random.choice(
                ['Completed', 'Cancelled by Customer', 'Cancelled by Driver', 'No Driver Found', 'Incomplete'],
                p=[0.65, 0.15, 0.10, 0.05, 0.05]
            )
            
            booking_val = None
            cust_rating = None
            driver_rating = None
            payment = None
            cancel_reason_cust = None
            cancel_reason_driver = None
            avg_vtat = round(random.uniform(2, 15), 1)
            avg_ctat = None
            
            if status == 'Completed':
                booking_val = self._get_pricing_logic(vehicle, distance, is_peak)
                avg_ctat = round(distance * random.uniform(2.5, 4.0), 1)
                cust_rating = round(random.uniform(3.0, 5.0), 1)
                driver_rating = round(random.uniform(3.5, 5.0), 1)
                payment = random.choice(self.payment_methods)
            elif status == 'Cancelled by Customer':
                cancel_reason_cust = random.choice(self.cancel_reasons_cust) if self.cancel_reasons_cust else "Change of plans"
            elif status == 'Cancelled by Driver':
                cancel_reason_driver = random.choice(self.cancel_reasons_driver) if self.cancel_reasons_driver else "Personal issues"

            row = {
                "Date": booking_time.strftime("%Y-%m-%d"),
                "Time": booking_time.strftime("%H:%M:%S"),
                "Booking ID": f"GEN-{random.randint(1000000, 9999999)}",
                "Booking Status": status,
                "Customer ID": f"CID{random.randint(10000, 99999)}",
                "Vehicle Type": vehicle,
                "Pickup Location": pickup,
                "Drop Location": drop,
                "Avg VTAT": avg_vtat,
                "Avg CTAT": avg_ctat,
                "Cancelled Rides by Customer": 1 if status == 'Cancelled by Customer' else None,
                "Reason for cancelling by Customer": cancel_reason_cust,
                "Cancelled Rides by Driver": 1 if status == 'Cancelled by Driver' else None,
                "Driver Cancellation Reason": cancel_reason_driver,
                "Incomplete Rides": 1 if status == 'Incomplete' else None,
                "Incomplete Rides Reason": "Car Breakdown" if status == 'Incomplete' else None,
                "Booking Value": booking_val,
                "Ride Distance": distance,
                "Driver Ratings": driver_rating,
                "Customer Rating": cust_rating,
                "Payment Method": payment
            }
            data.append(row)
            
        return pd.DataFrame(data)

def upload_to_minio(df, date_str):
    client = Minio(**MINIO_CONF)
    
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
        
    csv_bytes = df.to_csv(index=False).encode('utf-8')
    csv_buffer = io.BytesIO(csv_bytes)
    
    file_name = f"uber_booking_{date_str}.csv"
    object_name = f"{OUTPUT_PREFIX}/date={date_str}/{file_name}"
    
    print(f"Uploading to MinIO: {BUCKET_NAME}/{object_name}")
    client.put_object(
        BUCKET_NAME,
        object_name,
        data=csv_buffer,
        length=len(csv_bytes),
        content_type='application/csv'
    )
    print("Upload Success!")

# --- HÀM CHÍNH ĐỂ AIRFLOW GỌI ---
def run_generation_for_date(execution_date_str):
    """
    Hàm này sẽ được PythonOperator gọi.
    execution_date_str: Ngày cần generate (YYYY-MM-DD)
    """
    # Đường dẫn file CSV mẫu trong container Airflow
    csv_path = "/opt/airflow/tasks/uber/data/ncr_ride_bookings.csv"
    
    generator = UberDataGenerator(csv_path)
    
    # Random số lượng dòng
    rows_today = random.randint(400, 450)
    print(f">>> Generating data for: {execution_date_str} | Volume: {rows_today}")
    
    # Sinh và Upload
    df_daily = generator.generate_day(execution_date_str, num_rows=rows_today)
    upload_to_minio(df_daily, execution_date_str)