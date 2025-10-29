# dbt Project - Testing and Transforming OLIST ecommerce dataset

I defined schema as the name of folders in the model
used generic test, defined custom generic test (not_negative) as well as singular test for validating the coordinates
defined materialization in the global level

The key in dbt project is to realize and use the source and ref functions correctly, always keep in mind the LAG, the order of data transformation and desired tests etc. 

Link: [Dataset on Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)


SINGULAR TESTS:
i have designed a series of logical test to ensure data integrity and i found some cases that led to test failure. Potential reasons can be different interest rates for installment payments. Although there were a couple of other cases that could not be justified with that reseaon, there seems to be some data issues that unfortunately have not been looked at in detail by many people who used this dataset.  
🧩 payment_test_1
Logic
For orders that are delivered, shipped, or invoiced,
aggregated payments should roughly equal total item price + freight.
✅ Purpose: Ensures financial completeness and accuracy.
✅ Tolerance (0.05): Smart — avoids false positives from floating-point or rounding differences.
✅ Joins: You used INNER JOIN for both aggregated items and payments → ensures only orders present in both tables are checked.
✅ Logic: 100% valid.
Interpretation of your results
You got about 258 mismatches (out of thousands of orders).
That doesn’t mean your join or model is wrong — it means these 258 orders have some real inconsistency:
•	Over- or under-payment,
•	Refunds or adjustments,
•	Possible interest in multi-installment payments.
If you manually inspect 3–5 random rows, you’ll probably see clear patterns (like small deltas or a few large negative gaps).
✅ Verdict: Test logic is correct. Keep it as a warning test.
________________________________________
🧩 payment_test_2
Logic
Identify fulfilled orders that have no payment record.
✅ Purpose: Detects missing financial records (data loss or inconsistency).
✅ LEFT JOIN used → correct, because you want to include all orders and see where payments are missing.
✅ Filter on status ensures only paid-expected orders are checked.
Interpretation
You got 1 result — meaning there’s one delivered/shipped/invoiced order with no payment record.
That’s perfect — it’s exactly what you wanted to detect.
✅ Verdict: Correct and valuable test. Keep it.
________________________________________
🧩 payment_test_3
Logic
Identify fulfilled orders with no line-item details.
✅ Purpose: Prevents data loss or corruption in items table.
✅ LEFT JOIN and same status filtering — exactly the right approach.
✅ Result (3 rows) → means 3 orders are marked fulfilled but have no item details.
✅ Verdict: Test is logically solid.
You could refine it later if you find those 3 are genuine anomalies or part of dataset quirk, but the logic itself is flawless.
________________________________________
🧩 payment_test_4
Logic
Detect failed (canceled/unavailable) orders that still have a payment record.
✅ Purpose: Checks for unrefunded or mis-classified transactions.
✅ INNER JOIN → correct, because you only want failed orders that do have payments.
✅ Status filter is precise.
Interpretation
You got 1234 warnings → meaning there are many canceled/unavailable orders that still show payments.
That’s not “your fault” — it’s business-logic inconsistency in the Olist dataset, which is known to have this.
✅ Verdict: Test is perfectly valid. The large count just tells you about data issues, not code issues.
