
# Script for Sparks Sports Academy message ingestion and funnel analysis.


import requests
import json
import csv
import os
from datetime import datetime

# Replace with credentials and endpoints
QISCUS_SDK_BASE_URL = "https://api.qiscus.com/api/v2.1/rest"
QISCUS_APP_ID = os.environ.get("QISCUS_APP_ID", "your_qiscus_app_id") # Example: "sdksample"
QISCUS_SDK_SECRET = os.environ.get("QISCUS_SDK_SECRET", "your_qiscus_secret_key") # Example: "2820ae9dfc5362f7f3a10381fb89afc7"

# Placeholder for Booking/Transaction API/DB access
# This section needs to be adapted based on how booking/transaction data is actually stored and accessed.
BOOKING_API_ENDPOINT = os.environ.get("BOOKING_API_ENDPOINT", "http://internal.api/bookings")
TRANSACTION_API_ENDPOINT = os.environ.get("TRANSACTION_API_ENDPOINT", "http://internal.api/transactions")
API_AUTH_TOKEN = os.environ.get("INTERNAL_API_TOKEN", "your_internal_api_token")

# Path to the keyword file (one keyword per line)
KEYWORD_FILE = "/directory/opening_keywords.txt"

# Output file
OUTPUT_CSV_FILE = "/directory/funnel_report.csv"

def load_keywords(filepath):
    """Loads keywords from a text file."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            keywords = [line.strip().lower() for line in f if line.strip()]
        print(f"Loaded {len(keywords)} keywords from {filepath}")
        return keywords
    except FileNotFoundError:
        print(f"Error: Keyword file not found at {filepath}. Using default keywords.")
        # Default keywords if file not found
        return ["booking", "daftar", "trial", "coba gratis", "info harga", "jadwal"]
    except Exception as e:
        print(f"Error loading keywords: {e}")
        return []

def make_qiscus_request(endpoint, method="GET", params=None, data=None):
    """Makes a request to the Qiscus SDK API."""
    headers = {
        "QISCUS-SDK-APP-ID": QISCUS_APP_ID,
        "QISCUS-SDK-SECRET": QISCUS_SDK_SECRET,
        "Content-Type": "application/json"
    }
    url = f"{QISCUS_SDK_BASE_URL}/{endpoint}"
    try:
        if method == "GET":
            response = requests.get(url, headers=headers, params=params, timeout=30)
        elif method == "POST":
            response = requests.post(url, headers=headers, params=params, json=data, timeout=30)
        else:
            print(f"Unsupported HTTP method: {method}")
            return None
        
        response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error making Qiscus API request to {url}: {e}")
        return None
    except json.JSONDecodeError:
        print(f"Error decoding JSON response from {url}")
        return None

def get_all_rooms():
    """Fetches all rooms (or a recent subset) from Qiscus."""
    print("Simulating fetching room list (replace with actual API call if available)")
    # Example: Fetch rooms for specific users or based on recent activity if API supports it.
    # For now, returning an empty list as a placeholder.
    relevant_room_ids = [12345, 67890] # Example IDs
    if relevant_room_ids:
        room_params = {'room_ids[]': relevant_room_ids}
        return make_qiscus_request("get_rooms_info", params=room_params)
    return {'results': {'rooms': []}} # Placeholder

def get_room_messages(room_id, last_message_id=None):
    """Fetches messages for a specific room."""
    params = {"room_id": room_id, "limit": 100} # Fetch 100 messages per call
    if last_message_id:
        params["last_message_id"] = last_message_id
    return make_qiscus_request("get_room_comments", params=params) # Adjust endpoint if needed

def extract_customer_info(room_data):
    """Extracts customer phone/email from room participants."""
    # Find the participant who is not the agent/system
    if not room_data or 'participants' not in room_data:
        return None, None # Return None for both phone and channel
        
    participants = room_data.get('participants', [])
    customer_email = None
    for p in participants:
        if 'agent' not in p.get('extras', {}).get('type', '') and '@qismo.com' not in p.get('email', ''):
            customer_email = p.get('email') # Qiscus uses email as user_id
            break
            
    # Extract channel from room metadata if available (e.g., room_options or name), hhis is highly dependent on how channels are tracked in your Qiscus setup.
    channel = room_data.get('room_options', {}).get('channel', 'Unknown') # Example
    if channel == 'Unknown' and 'name' in room_data:
        # Fallback: try to infer from room name if structured
        if 'whatsapp' in room_data['name'].lower(): channel = 'WhatsApp'
        elif 'web' in room_data['name'].lower(): channel = 'Web Chat'
        # Add more channel inference rules as needed
        
    # Assuming email can be used as a proxy for phone number lookup if needed, or that phone number might be in user 'extras'. Adjust as necessary.
    # For now, we'll use the email as the primary identifier.
    phone_number = customer_email # Placeholder: Use email as identifier
    
    return phone_number, channel

def find_opening_message(messages, keywords):
    """Finds the first customer message containing a keyword."""
    if not messages or not keywords:
        return None

    # Sort messages by timestamp (assuming ascending order)
    # Example format: "2018-03-12T08:11:31Z"
    try:
        sorted_messages = sorted(messages, key=lambda m: datetime.fromisoformat(m.get('timestamp', '').replace('Z', '+00:00')))
    except ValueError:
         print("Warning: Could not parse timestamps for sorting messages.")
         sorted_messages = messages # Process in received order if sorting fails

    for message in sorted_messages:
        # Check if message is from customer (not agent/system)
        # Assuming customer messages have a specific type or sender identifier
        sender_type = message.get('sender', {}).get('extras', {}).get('type', 'customer') # Example assumption
        if sender_type != 'agent':
            msg_text = message.get('message', '').lower()
            for keyword in keywords:
                if keyword in msg_text:
                    return message # Return the first matching message
    return None


def get_booking_data(customer_identifier):
    """Placeholder: Fetches booking data for a customer."""
    # In a real implementation, query your booking system (API or DB) using the customer_identifier (e.g., phone number, email).
    print(f"Placeholder: Querying booking data for {customer_identifier}")
    # Simulate finding a booking, return {"booking_date": "2025-05-27", "status": "confirmed"} 
    return None # Simulate no booking found

def get_transaction_data(customer_identifier):
    """Placeholder: Fetches transaction data for a customer."""
    # In a real implementation, query your transaction system (API or DB).
    print(f"Placeholder: Querying transaction data for {customer_identifier}")
    # Simulate finding a transaction, return {"transaction_date": "2025-05-28", "transaction_value": 500000}
    return None # Simulate no transaction found

def process_funnel():
    """Main function to process messages and generate funnel report."""
    keywords = load_keywords(KEYWORD_FILE)
    if not keywords:
        print("Error: No keywords loaded. Aborting.")
        return

    # Get rooms (replace with actual logic)
    rooms_response = get_all_rooms()
    if not rooms_response or 'results' not in rooms_response or 'rooms' not in rooms_response['results']:
        print("Error: Could not fetch rooms or rooms data is invalid.")
        return
        
    all_rooms = rooms_response['results']['rooms']
    print(f"Found {len(all_rooms)} rooms to process (simulated)...")

    funnel_data = []

    for room in all_rooms:
        room_id = room.get('id')
        if not room_id:
            continue

        print(f"\nProcessing Room ID: {room_id}")
        
        # 1. Get Messages for the room
        messages_response = get_room_messages(room_id)
        if not messages_response or 'results' not in messages_response or 'comments' not in messages_response['results']:
            print(f"Warning: Could not fetch messages for room {room_id}.")
            continue
            
        messages = messages_response['results']['comments']
        if not messages:
            print(f"No messages found for room {room_id}.")
            continue
            
        # 2. Find the opening message with a keyword
        opening_message = find_opening_message(messages, keywords)
        if not opening_message:
            print(f"No opening keyword message found for room {room_id}.")
            continue
            
        # 3. Extract Lead Info
        leads_date_str = opening_message.get('timestamp')
        try:
            leads_date = datetime.fromisoformat(leads_date_str.replace('Z', '+00:00')).strftime('%Y-%m-%d')
        except (ValueError, TypeError):
            print(f"Warning: Could not parse leads_date '{leads_date_str}' for room {room_id}. Skipping.")
            continue
            
        customer_identifier, channel = extract_customer_info(room)
        if not customer_identifier:
            print(f"Warning: Could not identify customer for room {room_id}. Skipping.")
            continue
            
        print(f"Lead Found: Date={leads_date}, Channel={channel}, Customer={customer_identifier}")
        
        # 4. Correlate with Booking Data (Placeholder)
        booking_info = get_booking_data(customer_identifier)
        booking_date = booking_info.get('booking_date') if booking_info else None
        
        # 5. Correlate with Transaction Data (Placeholder)
        transaction_info = get_transaction_data(customer_identifier)
        transaction_date = transaction_info.get('transaction_date') if transaction_info else None
        transaction_value = transaction_info.get('transaction_value') if transaction_info else None
        
        # 6. Append to funnel report
        funnel_data.append({
            "leads_date": leads_date,
            "channel": channel,
            "phone_number": customer_identifier,
            "booking_date": booking_date,
            "transaction_date": transaction_date,
            "transaction_value": transaction_value,
            "room_id": room_id
        })

    # 7. Write report to CSV
    if funnel_data:
        print(f"\nWriting {len(funnel_data)} records to {OUTPUT_CSV_FILE}")
        fieldnames = ["leads_date", "channel", "phone_number", "booking_date", "transaction_date", "transaction_value", "room_id"]
        try:
            with open(OUTPUT_CSV_FILE, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(funnel_data)
            print("Funnel report generated successfully.")
        except IOError as e:
            print(f"Error writing CSV file: {e}")
    else:
        print("\nNo funnel data generated.")


if __name__ == "__main__":
    print("Starting Sparks Sports Academy Funnel Processor...")
    if not os.path.exists(KEYWORD_FILE):
        print(f"Creating dummy keyword file: {KEYWORD_FILE}")
        try:
            with open(KEYWORD_FILE, 'w', encoding='utf-8') as f:
                f.write("booking\n")
                f.write("daftar\n")
                f.write("trial\n")
        except IOError as e:
            print(f"Could not create dummy keyword file: {e}")
            
    process_funnel()
    print("Processing finished.")


