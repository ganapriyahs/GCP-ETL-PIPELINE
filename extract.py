import csv
from faker import Faker
import random
import string
from google.cloud import storage
from google.api_core.exceptions import Forbidden, GoogleAPIError

# Number of employees to generate
NUM_EMPLOYEES = 100

# Create Faker instance
fake = Faker()

# Character set for password
password_characters = string.ascii_letters + string.digits + 'm'

# Departments list
departments = [
    "HR", "Finance", "Engineering", "Sales",
    "Marketing", "IT", "Operations", "Customer Support"
]

# Track unique emails
used_emails = set()

def unique_email():
    email = fake.email()
    while email in used_emails:
        email = fake.email()
    used_emails.add(email)
    return email

def sanitize_field(value):
    """
    Flatten newlines and remove commas to prevent CSV parsing issues in Data Fusion.
    """
    return str(value).replace("\n", " ").replace(",", " ").strip()

# Generate employee CSV
csv_file = "employee_data.csv"
with open(csv_file, mode="w", newline="") as file:
    fieldnames = [
        "first_name", "last_name", "job_title", "department",
        "email", "address", "phone_number", "salary", "password"
    ]
    writer = csv.DictWriter(file, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
    writer.writeheader()

    for _ in range(NUM_EMPLOYEES):
        row = {
            "first_name": sanitize_field(fake.first_name()),
            "last_name": sanitize_field(fake.last_name()),
            "job_title": sanitize_field(fake.job()),
            "department": sanitize_field(random.choice(departments)),
            "email": sanitize_field(unique_email()),
            "address": sanitize_field(fake.address()),
            "phone_number": str(fake.random_int(min=1000000000, max=9999999999)),
            "salary": str(fake.random_int(min=30000, max=150000)),
            "password": "".join(random.choice(password_characters) for _ in range(8))
        }

        # Only write row if all fields are non-empty and match the schema length
        if len(row) == len(fieldnames) and all(row.values()):
            writer.writerow(row)

print(f"✅ Generated {NUM_EMPLOYEES} employee records in {csv_file}")

# Upload to GCS
def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)
        print(f"📤 File {source_file_name} uploaded to {destination_blob_name} in {bucket_name}.")
    except Forbidden:
        print("❌ Upload failed: Access forbidden. Check your billing/account permissions.")
    except GoogleAPIError as e:
        print(f"❌ Upload failed: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

# GCS settings
bucket_name = "bkt-employeeedata"  
source_file_name = csv_file
destination_blob_name = "employee_data.csv"

# Upload
upload_to_gcs(bucket_name, source_file_name, destination_blob_name)
