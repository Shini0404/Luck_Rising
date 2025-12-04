# 🛒 Retail Operational Intelligence & Personalization Suite

A comprehensive retail analytics hackathon project implementing 4 end-to-end data pipelines for inventory management, product affinity analysis, fraud detection, and customer lifetime value prediction.

---

## 📋 Project Overview

| Task | Name | Description |
|------|------|-------------|
| **Task 1** | Inventory Harmonization Pipeline | Unified product & inventory data with validation, fuzzy matching, and reconciliation |
| **Task 2** | Shopping Basket Affinity Analyzer | Market basket analysis with Support, Confidence, and Lift metrics |
| **Task 3** | Refund & Fraud Detection Engine | Automated fraud detection using rule-based and statistical anomaly detection |
| **Task 4** | CLV & Churn Prediction | Customer Lifetime Value prediction and churn risk analysis using RFM |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    MEDALLION ARCHITECTURE                           │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   RAW (Bronze)  →  STAGING (Silver)  →  CURATED (Gold)             │
│   ─────────────    ───────────────      ──────────────             │
│   • CSV Ingestion  • Validation         • Fact Tables              │
│   • Parquet Backup • Deduplication      • Aggregations             │
│                    • Fuzzy Matching     • Final Reports            │
│                    • Enrichment                                     │
│                                                                     │
│   QUARANTINE: Invalid records saved for audit                       │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 📁 Project Structure

```
Luck_Rising/
│
├── 📂 config/                    # Configuration files (YAML)
│   ├── task1_config.yml
│   ├── task2_config.yml
│   ├── task3_config.yml
│   └── task4_config.yml
│
├── 📂 raw/                       # Raw input data (Bronze layer)
│   ├── inventory_snapshot.csv
│   ├── restock_events.csv
│   ├── products.csv
│   ├── stores.csv
│   ├── store_sales_header.csv
│   ├── store_sales_line_items.csv
│   ├── customer_details.csv
│   ├── refund_transactions.csv
│   └── customer_transactions_history.csv
│
├── 📂 etl/                       # ETL Pipeline Scripts
│   ├── task1_inventory_pipeline.py
│   ├── task1_inventory.py
│   ├── task2_basket_affinity.py
│   ├── task3_fraud_detection.py
│   └── task4_clv_churn.py
│
├── 📂 staging/                   # Intermediate processed data (Silver)
│   ├── task2/
│   ├── task3/
│   └── task4/
│
├── 📂 curated/                   # Final output tables (Gold)
│   ├── inventory_fact/
│   ├── task2/
│   │   ├── affinity_scores/
│   │   └── top_affinities/
│   ├── task3/
│   │   ├── fraud_flags/
│   │   ├── suspicious_customers/
│   │   └── clean_refunds/
│   └── task4/
│       ├── customer_clv/
│       ├── churn_predictions/
│       └── customer_segments/
│
├── 📂 quarantine/                # Invalid records for audit
│
├── 📂 logs/                      # Pipeline execution logs
│
├── 📄 Task1_ER_Diagram.txt       # ER diagrams with speaking points
├── 📄 Task2_ER_Diagram.txt
├── 📄 Task3_ER_Diagram.txt
├── 📄 Task4_ER_Diagram.txt
│
├── 📄 app.py                     # Streamlit Dashboard
├── 📄 requirements.txt           # Python dependencies
└── 📄 README.md                  # This file
```

---

## 🚀 Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Run Each Pipeline

```bash
# Task 1: Inventory Harmonization
python etl/task1_inventory_pipeline.py

# Task 2: Basket Affinity Analysis
python etl/task2_basket_affinity.py

# Task 3: Fraud Detection
python etl/task3_fraud_detection.py

# Task 4: CLV & Churn Prediction
python etl/task4_clv_churn.py
```

### 3. Run Streamlit Dashboard (Optional)
```bash
streamlit run app.py
```

---

## 📊 Task Details

### Task 1: Unified Product & Inventory Data Harmonization

**Goal:** Create a single source of truth for inventory data

**Features:**
- ✅ Config-driven data ingestion
- ✅ Validation (missing fields, negative stock, capacity exceeded)
- ✅ Fuzzy matching for product ID correction (Levenshtein distance)
- ✅ SKU validation with regex
- ✅ Inventory reconciliation formula
- ✅ Quarantine layer for invalid records

**Reconciliation Formula:**
```
effective_stock = snapshot_level + incoming_restock - damaged - expired
```

---

### Task 2: Real-Time Shopping Basket Affinity Analyzer

**Goal:** Find products commonly purchased together

**Metrics Calculated:**
| Metric | Formula | Meaning |
|--------|---------|---------|
| **Support** | P(A ∩ B) | How often products appear together |
| **Confidence** | P(B\|A) | Probability of buying B given A |
| **Lift** | Confidence / P(B) | Strength of association (>1 = positive) |

**Output:** Top 10 "Customers who buy X also buy Y" recommendations

---

### Task 3: Refund & Fraud Detection Engine

**Goal:** Automatically detect suspicious refund activities

**Detection Methods:**
1. **Rule-Based Validation:**
   - Amount exceeded original
   - Customer ID mismatch
   - Outside refund window (>30 days)
   - Payment mode mismatch

2. **Behavioral Patterns:**
   - High-frequency refunds (>3 in 30 days)
   - High-value refunds (>$300)
   - Same product refunded repeatedly

3. **Statistical Anomaly Detection:**
   - Z-Score (|Z| > 2.0 = anomaly)
   - IQR (outside 1.5×IQR bounds)

**Output:** Fraud flags table with severity scores

---

### Task 4: Customer Lifetime Value & Churn Prediction

**Goal:** Predict CLV and identify customers at risk of churning

**RFM Analysis:**
| Metric | Meaning | Score 5 | Score 1 |
|--------|---------|---------|---------|
| **R**ecency | Days since last purchase | <7 days | >60 days |
| **F**requency | Number of purchases | 8+ | 1 |
| **M**onetary | Total spend | $2000+ | <$200 |

**CLV Formula:**
```
CLV = Predicted_Annual_Revenue × RFM_Weight × Profit_Margin
```

**Churn Status:**
- ACTIVE: <30 days since purchase
- AT_RISK: 30-60 days
- CHURNING: 60-90 days
- CHURNED: >90 days

**Customer Segments:**
- 🏆 CHAMPIONS (RFM 9-15)
- 💎 LOYAL CUSTOMERS (RFM 7-8)
- ⭐ POTENTIAL LOYALISTS (RFM 5-6)
- ⚠️ AT RISK (RFM 3-4)
- ❌ LOST (RFM 0-2)

---

## 🔧 Technologies Used

| Technology | Purpose |
|------------|---------|
| **Python** | Core programming language |
| **Pandas** | Data manipulation and analysis |
| **NumPy** | Numerical computations |
| **PyYAML** | Config file parsing |
| **FuzzyWuzzy** | Fuzzy string matching |
| **PyArrow** | Parquet file support |
| **Streamlit** | Interactive dashboard |
| **Plotly** | Data visualization |

---

## 📈 Sample Results

### Task 2: Top Product Affinities
```
#1: Customers who buy Milk also buy Olive Oil (Lift: 1.47)
#2: Customers who buy Eggs also buy Greek Yogurt (Lift: 1.33)
#3: Customers who buy Eggs also buy Basmati Rice (Lift: 1.33)
```

### Task 3: Suspicious Customers Detected
```
CUST012 (Fraud Test): Score 580 - CRITICAL RISK
  Flags: AMOUNT_EXCEEDED, HIGH_FREQUENCY, PAYMENT_MISMATCH
  
CUST011 (Suspicious User): Score 430 - CRITICAL RISK
  Flags: AMOUNT_EXCEEDED, PAYMENT_MISMATCH
```

### Task 4: Customer Segments
```
Champions:           7 (58.3%)
Loyal Customers:     1 (8.3%)
Potential Loyalists: 1 (8.3%)
At Risk:             3 (25.0%)
```

---

## 👥 Team Work Division

| Person | Role | Tasks |
|--------|------|-------|
| **Person 1** | Data Engineer | Config, Data Loading, RAW Layer |
| **Person 2** | Validation Developer | Validation, Fuzzy Matching, Deduplication |
| **Person 3** | Logic Developer | Reconciliation, CLV, Churn, Curated Output |

---

## 📄 ER Diagrams

Detailed ER diagrams with speaking points are available in:
- `Task1_ER_Diagram.txt`
- `Task2_ER_Diagram.txt`
- `Task3_ER_Diagram.txt`
- `Task4_ER_Diagram.txt`

---

## 🎯 Business Value

1. **Inventory Optimization:** Single source of truth reduces stock discrepancies
2. **Cross-Selling:** Product affinity insights increase basket size
3. **Fraud Prevention:** Automated detection reduces losses
4. **Customer Retention:** Proactive churn prevention saves high-value customers
5. **Marketing ROI:** Segment-based targeting improves campaign effectiveness

---

## 📝 License

This project was created for hackathon purposes.

---

## 🤝 Contributors

- Hackathon Team - Retail Operational Intelligence Suite

---

**Built with ❤️ for Retail Analytics**
