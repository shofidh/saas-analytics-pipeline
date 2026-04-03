import pandas as pd 
import numpy as np
import random
from datetime import datetime, timedelta
import os

# =========================
# SEED (REPRODUCIBILITY)
# =========================
# Ensure consistent dataset across runs for testing & portfolio purposes
np.random.seed(42)
random.seed(42)

# =========================
# CONFIGURATION (Business Scale Setup)
# =========================
N_ACCOUNTS = 10000             # Total unique accounts
N_SUBSCRIPTIONS_TARGET = 100000  # Approximate total subscription events
N_USAGE = 500000                # Number of feature usage records
N_TICKETS = 50000               # Number of support tickets

START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2026, 12, 31)

industries = ["Technology", "Finance", "E-commerce & Retail", "Healthcare & Life Sciences",
              "Education", "Energy & Resources", "Industrial & Logistics",
              "Consumer & Lifestyle", "Professional Services"]

countries = ["Singapore", "Brunei", "Cambodia", "Indonesia", "Laos",
             "Malaysia", "Myanmar", "Philippines"]

referrals = ["organic", "ads", "partner", "outbound", "event", "other"]
plans = ["Free", "Basic", "Pro", "Enterprise"]
features = ["dashboard", "api", "export", "automation", "integration"]
priorities = ["low", "medium", "high"]

# =========================
# HELPER FUNCTIONS
# =========================
def random_date(start, end):
    """Return a random datetime between start and end"""
    if end <= start:
        return start
    delta = end - start
    total_seconds = int(delta.total_seconds())
    random_second = random.randint(0, total_seconds)
    return start + timedelta(seconds=random_second)

def plan_price(plan, seats):
    """
    Return revenue for a given plan & number of seats.
    Free plan always 0, Enterprise has variable pricing.
    """
    if plan == "Basic":
        return seats * 10
    elif plan == "Pro":
        return seats * 25
    elif plan == "Enterprise":
        return seats * random.randint(40, 80)
    return 0

# =========================
# 1. ACCOUNT TABLE
# =========================
# Simulate SaaS customers with realistic signup behavior
accounts = []

for i in range(N_ACCOUNTS):
    account_id = f"A{i:05d}"
    signup_date = random_date(START_DATE, END_DATE)
    plan = random.choices(plans, weights=[0.3,0.3,0.25,0.15])[0]  # Funnel: most start Free/Basic
    seats = random.randint(50, 500) if plan == "Enterprise" else random.randint(1,50)
    is_trial = plan == "Free" or random.random() < 0.4

    accounts.append([
        account_id,
        f"Company_{i}",
        random.choice(industries),
        random.choice(countries),
        signup_date,
        random.choice(referrals),
        plan,
        seats,
        is_trial,
        False  # churn_flag updated later
    ])

account_df = pd.DataFrame(accounts, columns=[
    "account_id","account_name","industry","country",
    "signup_date","referral_source","plan_tier","seats",
    "is_trial","churn_flag"
])

# =========================
# 2. SUBSCRIPTION TABLE
# =========================
# Tracks all subscription events, upgrades/downgrades, churn, MRR/ARR
subs = []
sub_id_counter = 0

for _, acc in account_df.iterrows():
    if sub_id_counter >= N_SUBSCRIPTIONS_TARGET:
        break

    account_id = acc.account_id
    current_date = acc.signup_date
    last_plan = acc.plan_tier
    last_seats = acc.seats
    n_events = random.randint(5, 15)
    cycles = 0
    max_cycles = 20
    start_idx = len(subs)
    seen_plans = set()
    is_first_subscription = True

    while cycles < max_cycles:
        if current_date >= END_DATE:
            break
        cycles += 1
        sub_id = f"S{sub_id_counter:06d}"
        sub_id_counter += 1

        # Duration of this subscription period (30-180 days)
        duration_days = random.randint(30, 180)
        end_date = min(current_date + timedelta(days=duration_days), END_DATE)

        # Plan change simulation
        move = random.choice(["same","upgrade","downgrade"])
        if move == "upgrade":
            plan_idx = min(plans.index(last_plan)+1, len(plans)-1)
            plan = plans[plan_idx]
            seats = last_seats + random.randint(1,20)
        elif move == "downgrade":
            plan_idx = max(plans.index(last_plan)-1,0)
            plan = plans[plan_idx]
            seats = max(1,last_seats - random.randint(1,10))
        else:
            plan = last_plan
            seats = last_seats

        # Revenue calculation
        revenue = float(round(plan_price(plan, seats),2))
        price = round(revenue / seats,2)
            # Tentukan rate dasar per plan
        rates = {
            "Basic": 0.05,
            "Pro": 0.10,
            "Enterprise": 0.20,
            "Free": 0
        }
            # Logika: Hanya 10% data yang dapet diskon
        if random.random() < 0.10:
            discount_rate = rates[plan]
        else:
            discount_rate = 0.0

        # Hitung nilai diskon & net revenue
        discount_value = round(revenue * discount_rate, 2)
        discount_rate = {"Basic":0.05,"Pro":0.10,"Enterprise":0.20,"Free":0}[plan]
        discount_value = round(revenue * discount_rate,2)
        net_revenue = round(revenue - discount_value,2)
        mrr = net_revenue
        arr = round(mrr*12,2)

        # Trial logic: first subscription or first time using this plan
        is_trial = is_first_subscription or (plan not in seen_plans)

        # Upgrade / Downgrade flags
        if is_first_subscription:
            upgrade_flag = False
            downgrade_flag = False
        else:
            upgrade_flag = move=="upgrade"
            downgrade_flag = move=="downgrade"

        churn = random.random() < 0.1

        subs.append([
            sub_id,
            account_id,
            current_date,
            end_date,
            plan,
            seats,
            price,
            discount_value,
            mrr,
            arr,
            is_trial,
            upgrade_flag,
            downgrade_flag,
            churn,
            random.choice(["monthly","annual"]),
            False if plan=="Free" else random.random()<0.85
        ])

        seen_plans.add(plan)
        is_first_subscription = False

        # Reactivation / churn logic
        if churn:
            if random.random() < 0.3:  # chance to reactivate later
                gap_days = random.randint(30,180)
                current_date = min(end_date + timedelta(days=gap_days), END_DATE)
                last_plan = random.choice(["Basic","Pro"])
                last_seats = random.randint(1,20)
                continue
            else:
                break

        current_date = end_date
        last_plan = plan
        last_seats = seats
        if len(subs)-start_idx >= n_events:
            break

subscription_df = pd.DataFrame(subs, columns=[
    "subscription_id","account_id","start_date","end_date",
    "plan_tier","seats","price","discount_value","mrr_amount","arr_amount",
    "is_trial","upgrade_flag",
    "downgrade_flag","churn_flag","billing_frequency","auto_renew_flag"
])

# Detect reactivation events
subscription_df = subscription_df.sort_values(["account_id","start_date"])
subscription_df["next_start"] = subscription_df.groupby("account_id")["start_date"].shift(-1)
subscription_df["is_reactivation"] = (
    (subscription_df["churn_flag"]==True) &
    (subscription_df["next_start"].notna())
)

# =========================
# 3. CHURN TABLE
# =========================
# Detailed churn events with reason & refund logic
churn_rows = []
for i,row in subscription_df[subscription_df["churn_flag"]].iterrows():
    churn_date = row["end_date"]
    refund = 0
    if pd.notna(churn_date):
        duration_days = (churn_date - row["start_date"]).days
        if duration_days < 30:
            duration_months = max(0.1,duration_days/20)
            actual_revenue = row["mrr_amount"]*duration_months
            refund = round(actual_revenue*0.5,2)
    churn_rows.append([
        f"C{i:06d}",
        row["account_id"],
        churn_date,
        random.choice(["price_too_high","missing_features","bugs","competitor","no_longer_needed"]),
        refund,
        row["upgrade_flag"],
        row["downgrade_flag"],
        row["is_reactivation"],
        random.choice([None,"Too expensive","Buggy","Not useful"])
    ])

churn_df = pd.DataFrame(churn_rows, columns=[
    "churn_event_id","account_id","churn_date","reason_code",
    "refund_amount_usd","preceding_upgrade_flag",
    "preceding_downgrade_flag","is_reactivation","feedback_text"
])

# Update account churn flag
account_df.loc[account_df.account_id.isin(churn_df.account_id),"churn_flag"] = True

# =========================
# 4. FEATURE USAGE TABLE
# =========================
# Logs actual usage of platform features per subscription
usage_rows = []
subs_sample = subscription_df.sample(N_USAGE, replace=True)

for i,row in subs_sample.iterrows():
    end_date = row["end_date"] if pd.notna(row["end_date"]) else END_DATE
    usage_date = random_date(row["start_date"], end_date)
    base_usage = {"Free":5,"Basic":20,"Pro":50,"Enterprise":100}[row["plan_tier"]]
    usage_count = max(0,int(np.random.normal(base_usage, base_usage*0.3)))
    duration = usage_count * random.randint(10,60)
    usage_rows.append([
        f"U{i:06d}",
        row["subscription_id"],
        usage_date,
        random.choice(features),
        usage_count,
        duration,
        random.randint(0,3),
        random.random()<0.1
    ])

usage_df = pd.DataFrame(usage_rows, columns=[
    "usage_id","subscription_id","usage_date",
    "feature_name","usage_count","usage_duration_secs",
    "error_count","is_beta_feature"
])

# =========================
# 5. SUPPORT TICKETS TABLE
# =========================
# Simulate customer support tickets with resolution & satisfaction metrics
tickets = []

for i in range(N_TICKETS):
    acc = account_df.sample(1).iloc[0]
    submitted = random_date(acc.signup_date, END_DATE)
    priority = random.choice(priorities)
    escalation = random.random() < (0.3 if priority=="high" else 0.1)
    resolution_time = {"low":round(random.uniform(24,72),1),
                       "medium":round(random.uniform(12,48),1),
                       "high":round(random.uniform(2,24),1)}[priority]
    satisfaction = None
    if random.random()>0.4:
        satisfaction = round(max(1,5-(resolution_time/24)-(2 if escalation else 0)),2)
    tickets.append([
        f"T{i:06d}",
        acc.account_id,
        submitted,
        submitted+timedelta(hours=resolution_time),
        resolution_time,
        priority,
        random.randint(5,120),
        satisfaction,
        escalation
    ])

ticket_df = pd.DataFrame(tickets, columns=[
    "ticket_id","account_id","submitted_at","closed_at",
    "resolution_time_hours","priority","first_response_time_minutes",
    "satisfaction_score","escalation_flag"
])


# =========================
# OUTPUT
# =========================
print("account_df:", account_df.shape)
print("subscription_df:", subscription_df.shape)
print("churn_df:", churn_df.shape)
print("usage_df:", usage_df.shape)
print("ticket_df:", ticket_df.shape)

# 1. Define the base path
output_path = r"data\raw_data"

# 2. Ensure the directory exists (prevents errors)
if not os.path.exists(output_path):
    os.makedirs(output_path)

# 3. Save each DataFrame
account_df.to_csv(os.path.join(output_path, "accounts.csv"), index=False)
subscription_df.to_csv(os.path.join(output_path, "subscriptions.csv"), index=False)
churn_df.to_csv(os.path.join(output_path, "churn_events.csv"), index=False)
usage_df.to_csv(os.path.join(output_path, "feature_usage.csv"), index=False)
ticket_df.to_csv(os.path.join(output_path, "support_tickets.csv"), index=False)