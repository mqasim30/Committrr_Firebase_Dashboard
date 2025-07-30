import os
from dotenv import load_dotenv
load_dotenv()

import firebase_admin
from firebase_admin import credentials, db
import pandas as pd
import streamlit as st
import logging
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load Firebase configuration from environment variables or Streamlit secrets
firebase_cert_source = os.environ.get("FIREBASE_CERT_PATH") or st.secrets.get("FIREBASE_CERT_JSON")
firebase_db_url = os.environ.get("FIREBASE_DB_URL") or st.secrets.get("FIREBASE_DB_URL")

logging.info("Firebase DB URL: %s", firebase_db_url)

if not firebase_cert_source or not firebase_db_url:
    st.error("Firebase configuration is missing. Set FIREBASE_CERT_JSON (as dict) and FIREBASE_DB_URL in your secrets.")
    st.stop()

# Convert to a regular dict if it's not one already
if not isinstance(firebase_cert_source, dict):
    try:
        firebase_cert_source = dict(firebase_cert_source)
        logging.info("Converted firebase_cert_source to dict successfully.")
    except Exception as e:
        logging.error("Failed to convert certificate source to dict: %s", e)
        st.error("Failed to convert certificate source to dict: " + str(e))
        st.stop()

# Replace escaped newline characters with actual newlines in the private_key field
if "private_key" in firebase_cert_source:
    firebase_cert_source["private_key"] = firebase_cert_source["private_key"].replace("\\n", "\n")
    logging.info("Processed private_key newlines.")

# Initialize Firebase credentials
try:
    cred = credentials.Certificate(firebase_cert_source)
    logging.info("Certificate credential initialized successfully.")
except Exception as e:
    logging.error("Failed to initialize certificate credential: %s", e)
    st.error("Failed to initialize certificate credential: " + str(e))
    st.stop()

# Initialize Firebase Admin (only once)
try:
    try:
        firebase_admin.initialize_app(cred, {'databaseURL': firebase_db_url})
        logging.info("Firebase Admin initialized successfully.")
    except ValueError:
        logging.info("Firebase Admin already initialized. Using existing app.")
        firebase_admin.get_app()
except Exception as e:
    logging.error("Error initializing Firebase Admin: %s", e)
    st.error("Firebase initialization failed. Check your configuration.")
    st.stop()

logging.info("Firebase Admin setup complete.")

# Get database reference
def get_database():
    return db

database = get_database()

def format_timestamp(timestamp):
    """Convert timestamp to readable format with timezone adjustment"""
    if pd.notna(timestamp) and timestamp != 0:
        try:
            # Convert to datetime
            dt = datetime.fromtimestamp(timestamp/1000)
            # Add 5 hours to adjust for timezone
            dt = dt + timedelta(hours=5)
            return dt.strftime('%H:%M:%S %Y-%m-%d')
        except (ValueError, TypeError):
            return "Invalid date"
    return "Not available"

def fetch_recent_payments(limit=50):
    """Fetch recent payments sorted by createdAt timestamp using indexed query"""
    try:
        ref = database.reference("payments")
        
        # Use indexed query to efficiently get recent payments
        query = ref.order_by_child("createdAt").limit_to_last(limit)
        payments_data = query.get()
        
        if not payments_data or not isinstance(payments_data, dict):
            logging.warning("No payments data found")
            return []
        
        # Convert to list with payment IDs
        payments_list = []
        for payment_id, payment_data in payments_data.items():
            if isinstance(payment_data, dict):
                payment_record = {
                    "payment_id": payment_id,
                    **payment_data
                }
                payments_list.append(payment_record)
        
        # Sort by createdAt timestamp (most recent first) - data comes in ascending order
        sorted_payments = sorted(
            payments_list,
            key=lambda x: x.get("createdAt", 0),
            reverse=True
        )
        
        logging.info(f"Found {len(sorted_payments)} recent payments using indexed query")
        return sorted_payments
        
    except Exception as e:
        logging.error(f"Error fetching payments with indexed query: {e}")
        # Fallback to basic query if indexed query fails
        try:
            ref = database.reference("payments")
            all_payments = ref.get()
            
            if not all_payments:
                return []
            
            payments_list = []
            for payment_id, payment_data in all_payments.items():
                if isinstance(payment_data, dict):
                    payment_record = {"payment_id": payment_id, **payment_data}
                    payments_list.append(payment_record)
            
            sorted_payments = sorted(payments_list, key=lambda x: x.get("createdAt", 0), reverse=True)
            return sorted_payments[:limit]
            
        except Exception as fallback_error:
            logging.error(f"Fallback query also failed: {fallback_error}")
            return []

def fetch_valid_payments_24h():
    """Fetch valid payments from the last 24 hours using indexed queries"""
    try:
        # Calculate 24 hours ago timestamp
        now = datetime.now()
        twenty_four_hours_ago = now - timedelta(hours=24)
        cutoff_timestamp = int(twenty_four_hours_ago.timestamp() * 1000)
        
        ref = database.reference("payments")
        
        # Use indexed query to get payments from last 24 hours
        # First get payments by timestamp, then filter by status
        query = ref.order_by_child("createdAt").start_at(cutoff_timestamp)
        recent_payments = query.get()
        
        if not recent_payments:
            logging.info("No payments found in last 24 hours using indexed query")
            return []
        
        valid_payments = []
        for payment_id, payment_data in recent_payments.items():
            if isinstance(payment_data, dict):
                # Check if payment is completed
                if payment_data.get("status") == "completed":
                    payment_record = {
                        "payment_id": payment_id,
                        **payment_data
                    }
                    valid_payments.append(payment_record)
        
        # Sort by createdAt (most recent first)
        valid_payments.sort(key=lambda x: x.get("createdAt", 0), reverse=True)
        
        logging.info(f"Found {len(valid_payments)} valid payments in last 24 hours using indexed query")
        return valid_payments
        
    except Exception as e:
        logging.error(f"Error fetching 24h valid payments with indexed query: {e}")
        # Fallback to non-indexed query
        try:
            ref = database.reference("payments")
            all_payments = ref.get()
            
            if not all_payments:
                return []
            
            now = datetime.now()
            twenty_four_hours_ago = now - timedelta(hours=24)
            cutoff_timestamp = int(twenty_four_hours_ago.timestamp() * 1000)
            
            valid_payments = []
            for payment_id, payment_data in all_payments.items():
                if isinstance(payment_data, dict):
                    if (payment_data.get("status") == "completed" and 
                        payment_data.get("createdAt", 0) >= cutoff_timestamp):
                        
                        payment_record = {"payment_id": payment_id, **payment_data}
                        valid_payments.append(payment_record)
            
            valid_payments.sort(key=lambda x: x.get("createdAt", 0), reverse=True)
            return valid_payments
            
        except Exception as fallback_error:
            logging.error(f"Fallback query for 24h payments failed: {fallback_error}")
            return []

def fetch_user_payments(user_id, limit=20):
    """Fetch payments for a specific user using indexed query"""
    try:
        ref = database.reference("payments")
        
        # Use indexed query to get payments for specific user
        query = ref.order_by_child("userId").equal_to(user_id).limit_to_last(limit)
        user_payments = query.get()
        
        if not user_payments:
            logging.info(f"No payments found for user {user_id}")
            return []
        
        payments_list = []
        for payment_id, payment_data in user_payments.items():
            if isinstance(payment_data, dict):
                payment_record = {
                    "payment_id": payment_id,
                    **payment_data
                }
                payments_list.append(payment_record)
        
        # Sort by createdAt (most recent first)
        payments_list.sort(key=lambda x: x.get("createdAt", 0), reverse=True)
        
        logging.info(f"Found {len(payments_list)} payments for user {user_id}")
        return payments_list
        
    except Exception as e:
        logging.error(f"Error fetching payments for user {user_id}: {e}")
        return []

def fetch_user_profile(user_id):
    """Fetch user profile data by UID"""
    try:
        ref = database.reference(f"USER_PROFILES/{user_id}")
        user_data = ref.get()
        
        if user_data and isinstance(user_data, dict):
            logging.info(f"Found user profile for {user_id}")
            return user_data
        else:
            logging.warning(f"No user profile found for {user_id}")
            return None
            
    except Exception as e:
        logging.error(f"Error fetching user profile {user_id}: {e}")
        return None

def calculate_payment_stats(payments):
    """Calculate basic statistics from payments data"""
    if not payments:
        return {}
    
    df = pd.DataFrame(payments)
    
    stats = {}
    if 'amount' in df.columns:
        stats['total_amount'] = df['amount'].sum()
        stats['average_amount'] = df['amount'].mean()
        stats['count'] = len(df)
    
    if 'status' in df.columns:
        status_counts = df['status'].value_counts().to_dict()
        stats['status_breakdown'] = status_counts
    
    if 'currency' in df.columns:
        currency_counts = df['currency'].value_counts().to_dict()
        stats['currency_breakdown'] = currency_counts
    
    return stats

# --- STREAMLIT DASHBOARD ---
st.title("Payments Dashboard")

# Index setup warning
st.info("📊 **Performance Note:** This dashboard uses indexed queries for efficient data retrieval. Make sure you've updated your Firebase rules with the provided indexing configuration to avoid hitting Firebase limits.")

# --- USER PROFILE SEARCH SECTION ---
st.header("🔍 User Profile Search")

user_id_input = st.text_input("Enter User ID to search:", placeholder="Enter UID here...")

if user_id_input:
    with st.spinner(f"Searching for user {user_id_input}..."):
        user_profile = fetch_user_profile(user_id_input.strip())
    
    if user_profile:
        st.success(f"User profile found for {user_id_input}")
        
        # Display user profile in a clean format
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Basic Info")
            st.write(f"**Name:** {user_profile.get('UserName', 'N/A')}")
            st.write(f"**Email:** {user_profile.get('UserEmail', 'N/A')}")
            st.write(f"**Country:** {user_profile.get('UserCountry', 'N/A')}")
            st.write(f"**Platform:** {user_profile.get('Platform', 'N/A')}")
            st.write(f"**Status:** {user_profile.get('UserStatus', 'N/A')}")
        
        with col2:
            st.subheader("Activity & Stats")
            st.write(f"**Amount Won:** ${user_profile.get('AmountWon', 0)}")
            st.write(f"**Join Date:** {format_timestamp(user_profile.get('UserJoinDate', 0))}")
            st.write(f"**Last Active:** {format_timestamp(user_profile.get('UserActiveDate', 0))}")
            st.write(f"**Source:** {user_profile.get('UserSource', 'N/A')}")
            st.write(f"**IP:** {user_profile.get('UserIP', 'N/A')}")
        
        # Fetch and display user's payment history
        st.subheader("💳 Payment History")
        with st.spinner(f"Loading payment history for {user_id_input}..."):
            user_payments = fetch_user_payments(user_id_input.strip(), 20)
        
        if user_payments:
            # Calculate user payment stats
            user_payment_stats = calculate_payment_stats(user_payments)
            
            # Display user payment metrics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Payments", user_payment_stats.get('count', 0))
            with col2:
                total_spent = user_payment_stats.get('total_amount', 0)
                st.metric("Total Spent", f"${total_spent/100:.2f}")
            with col3:
                avg_payment = user_payment_stats.get('average_amount', 0)
                st.metric("Avg Payment", f"${avg_payment/100:.2f}")
            with col4:
                completed_payments = user_payment_stats.get('status_breakdown', {}).get('completed', 0)
                st.metric("Completed", completed_payments)
            
            # Display user payments table
            user_payments_df = pd.DataFrame(user_payments)
            
            if "createdAt" in user_payments_df.columns:
                user_payments_df["Formatted_Created"] = user_payments_df["createdAt"].apply(format_timestamp)
            
            if "amount" in user_payments_df.columns:
                user_payments_df["Amount_USD"] = user_payments_df["amount"].apply(lambda x: f"${x/100:.2f}" if pd.notna(x) else "$0.00")
            
            display_cols = ["payment_id", "Amount_USD", "currency", "status", "challengeId", "Formatted_Created"]
            display_cols = [col for col in display_cols if col in user_payments_df.columns]
            
            st.dataframe(user_payments_df[display_cols], use_container_width=True)
        else:
            st.info("No payment history found for this user.")
        
        # Show raw data in expandable section
        with st.expander("View Raw Profile Data"):
            st.json(user_profile)
    else:
        st.error(f"No user profile found for UID: {user_id_input}")

st.divider()

# --- RECENT PAYMENTS SECTION ---
st.header("💳 Recent Payments (All)")

with st.spinner("Loading recent payments..."):
    recent_payments = fetch_recent_payments(50)

if not recent_payments:
    st.warning("No payments found")
else:
    # Calculate and display stats
    stats = calculate_payment_stats(recent_payments)
    
    if stats:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Payments", stats.get('count', 0))
        
        with col2:
            total_amount = stats.get('total_amount', 0)
            st.metric("Total Amount", f"${total_amount/100:.2f}")  # Assuming amount is in cents
        
        with col3:
            avg_amount = stats.get('average_amount', 0)
            st.metric("Average Amount", f"${avg_amount/100:.2f}")
        
        with col4:
            completed_count = stats.get('status_breakdown', {}).get('completed', 0)
            st.metric("Completed", completed_count)
    
    # Create DataFrame for display
    payments_df = pd.DataFrame(recent_payments)
    
    # Format timestamps
    if "createdAt" in payments_df.columns:
        payments_df["Formatted_Created"] = payments_df["createdAt"].apply(format_timestamp)
    
    # Format amount to dollars
    if "amount" in payments_df.columns:
        payments_df["Amount_USD"] = payments_df["amount"].apply(lambda x: f"${x/100:.2f}" if pd.notna(x) else "$0.00")
    
    # Display columns
    display_cols = ["payment_id", "userId", "Amount_USD", "currency", "status", "challengeId", "Formatted_Created"]
    display_cols = [col for col in display_cols if col in payments_df.columns]
    
    st.dataframe(payments_df[display_cols], use_container_width=True)

st.divider()

# --- VALID PAYMENTS LAST 24 HOURS SECTION ---
st.header("✅ Valid Payments (Last 24 Hours)")

with st.spinner("Loading valid payments from last 24 hours..."):
    valid_payments_24h = fetch_valid_payments_24h()

if not valid_payments_24h:
    st.warning("No valid payments found in the last 24 hours")
else:
    # Calculate stats for valid payments
    valid_stats = calculate_payment_stats(valid_payments_24h)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Valid Payments (24h)", valid_stats.get('count', 0))
    
    with col2:
        total_valid = valid_stats.get('total_amount', 0)
        st.metric("Total Revenue (24h)", f"${total_valid/100:.2f}")
    
    with col3:
        avg_valid = valid_stats.get('average_amount', 0)
        st.metric("Average Payment (24h)", f"${avg_valid/100:.2f}")
    
    # Create DataFrame for valid payments
    valid_df = pd.DataFrame(valid_payments_24h)
    
    # Format timestamps
    if "createdAt" in valid_df.columns:
        valid_df["Formatted_Created"] = valid_df["createdAt"].apply(format_timestamp)
    
    # Format amount to dollars
    if "amount" in valid_df.columns:
        valid_df["Amount_USD"] = valid_df["amount"].apply(lambda x: f"${x/100:.2f}" if pd.notna(x) else "$0.00")
    
    # Display columns for valid payments
    display_cols = ["payment_id", "userId", "Amount_USD", "currency", "status", "challengeId", "Formatted_Created"]
    display_cols = [col for col in display_cols if col in valid_df.columns]
    
    st.dataframe(valid_df[display_cols], use_container_width=True)

st.divider()

# --- ANALYTICS SECTION ---
st.header("📈 Payment Analytics")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Filter by Status")
    if recent_payments:
        status_filter = st.selectbox(
            "Select payment status:",
            options=["All"] + list(set([p.get('status', 'unknown') for p in recent_payments])),
            index=0
        )
        
        if status_filter != "All":
            filtered_payments = [p for p in recent_payments if p.get('status') == status_filter]
            if filtered_payments:
                filtered_stats = calculate_payment_stats(filtered_payments)
                st.metric(f"{status_filter.title()} Payments", filtered_stats.get('count', 0))
                st.metric(f"Total Amount ({status_filter})", f"${filtered_stats.get('total_amount', 0)/100:.2f}")
            else:
                st.info(f"No payments found with status: {status_filter}")

with col2:
    st.subheader("Filter by Currency")
    if recent_payments:
        currency_filter = st.selectbox(
            "Select currency:",
            options=["All"] + list(set([p.get('currency', 'unknown') for p in recent_payments])),
            index=0
        )
        
        if currency_filter != "All":
            currency_payments = [p for p in recent_payments if p.get('currency') == currency_filter]
            if currency_payments:
                currency_stats = calculate_payment_stats(currency_payments)
                st.metric(f"{currency_filter.upper()} Payments", currency_stats.get('count', 0))
                st.metric(f"Total Amount ({currency_filter.upper()})", f"${currency_stats.get('total_amount', 0)/100:.2f}")
            else:
                st.info(f"No payments found with currency: {currency_filter}")

st.divider()

# --- STATUS BREAKDOWN SECTION ---
if recent_payments:
    st.header("📊 Payment Status Breakdown")
    
    status_df = pd.DataFrame(recent_payments)
    if 'status' in status_df.columns:
        status_counts = status_df['status'].value_counts()
        
        col1, col2 = st.columns([1, 1])
        
        with col1:
            st.subheader("Status Counts")
            for status, count in status_counts.items():
                st.write(f"**{status.title()}:** {count}")
        
        with col2:
            st.subheader("Status Chart")
            st.bar_chart(status_counts)

# --- REFRESH BUTTON ---
st.divider()
if st.button("🔄 Refresh Data", type="primary"):
    st.rerun()

st.caption("Dashboard uses indexed Firebase queries for efficient data retrieval. Make sure Firebase rules are updated with indexing configuration. Use refresh button to force update data.")