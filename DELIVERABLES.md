# Deliverables

## 1. Define at Least 3 Data Quality Metrics for Your Data Product

For your specific data product, define and calculate at least **3 Data Quality metrics** that help users understand whether the data can be trusted.

### Examples of Metrics

- **Completeness** — percentage of non-null values in key columns.
- **Uniqueness** — percentage of unique IDs or records.
- **Row count** — number of records after each pipeline run.
- **Freshness** — how recently the data was updated.
- **Validity** — percentage of values matching expected rules, for example `price > 0`, date not in the future.
- **Consistency** — whether values match expected categories or reference lists.

### Each Metric Should Include

1. Metric name
2. Metric definition
3. Current value
4. Expected threshold (if applicable)
5. Update cadence (e.g., after each pipeline run, daily, weekly, manually)

### Example

| Metric | Definition | Current Value | Threshold | Update Cadence |
|--------|------------|---------------|-----------|----------------|
| Completeness of `customer_id` | % of rows where `customer_id` is not null | 99.2% | > 98% | Every pipeline run |
| Unique order IDs | % of unique `order_id` values | 100% | 100% | Every pipeline run |
| Data freshness | Time since last successful update | 1 day | < 2 days | Daily |

---

## 2. Add Data Product Specification / Data Product Contract to Git

Create a **Data Product Specification**, also called a **Data Product Contract**, and store it in your Git repository.

The contract should describe how another person can understand and consume your product.

### It Should Include at Least

- Product name
- Product owner
- Product purpose
- Data source(s)
- Schema: column names, data types, descriptions
- Refresh frequency
- Access method
- Quality metrics
- Known limitations
- Example usage (e.g., SQL query or Python snippet)

> **Recommended file name:** `data_product_contract.yaml`

The goal is that another student can open your repository and understand what your data product contains and how to use it.

---

## 3. Share the Product on MS Teams Data Marketplace

Publish your data product in the **MS Teams Data Marketplace** folder.

### Your Published Product Should Include

- Filled **2-page DOCX Data Product Card**
- Link to the dataset or access instructions
- Link to your Git repository or contract file
- Short description of what the product is useful for

The PDF should be understandable **without additional explanation** from you.

### Minimum Expected Content

1. What the product is
2. What problem it solves
3. Where the data comes from
4. How to access it
5. What the schema looks like
6. What the key quality metrics are
7. Known limitations
8. Contact / owner

### Success Criteria

A good submission means that another student should be able to:

- [ ] Find your product in MS Teams
- [ ] Understand what it is about
- [ ] Access the data
- [ ] Load it into a tool
- [ ] Create a simple visualization
- [ ] Give feedback without asking you additional questions

---

## Additional Info

> If your repository is **Private**, make sure you can share the product in a different way. Make sure this info is enclosed in your Data Product Card (on how to get the access).