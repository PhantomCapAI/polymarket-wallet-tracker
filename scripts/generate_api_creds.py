"""Generate Polymarket API credentials from wallet private key.

Usage:
  1. Set POLYMARKET_PRIVATE_KEY and POLYMARKET_FUNDER in your .env file
  2. Run: python scripts/generate_api_creds.py
  3. Copy the output into your .env file
"""

import os
import sys

from dotenv import load_dotenv
from py_clob_client.client import ClobClient

load_dotenv()

private_key = os.getenv("POLYMARKET_PRIVATE_KEY")
funder = os.getenv("POLYMARKET_FUNDER")

if not private_key or not funder:
    print("Error: Set POLYMARKET_PRIVATE_KEY and POLYMARKET_FUNDER in your .env file")
    sys.exit(1)

client = ClobClient(
    "https://clob.polymarket.com",
    key=private_key,
    chain_id=137,
    signature_type=1,
    funder=funder,
)

api_creds = client.create_or_derive_api_creds()

print("Add these to your .env file:\n")
print(f"POLYMARKET_API_KEY={api_creds.api_key}")
print(f"POLYMARKET_SECRET={api_creds.api_secret}")
print(f"POLYMARKET_PASSPHRASE={api_creds.api_passphrase}")
