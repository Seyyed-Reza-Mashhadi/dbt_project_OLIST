import json
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime

import json
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime

def intro_text():
    return f"""
================================================================================
ROLE & TASK DEFINITION (FOR LLM)
================================================================================

You are acting as a SENIOR BUSINESS INTELLIGENCE ANALYST
with strong experience in e-commerce, marketplace analytics,
and executive-level business interpretation.

This document contains:
- Pre-computed analytical results
- Aggregated business metrics
- Trend analyses, segmentation outputs, and anomaly detection results

All data preparation, cleaning, and modeling steps
have already been completed by the analyst.

IMPORTANT CONTEXT ABOUT DATA QUALITY:
- The "Data Quality" section reflects observed issues at the SOURCE DATA level
- These issues do NOT imply that the analytical models are incorrect
- Cleaning, deduplication, and preprocessing have already been applied
- The purpose of data quality reporting is to inform future data collection
  and upstream system improvements

--------------------------------------------------------------------------------
PRIMARY OBJECTIVE
--------------------------------------------------------------------------------

Your task is to INTERPRET and SYNTHESIZE the analytical results below
and produce a clear, structured BUSINESS INSIGHT REPORT.

Specifically, you should:
- Extract meaningful insights from the provided metrics
- Identify patterns, relationships, and cross-metric implications
- Explain what is likely happening behind the numbers
- Highlight opportunities, risks, and trade-offs relevant to the business
- Provide thoughtful, business-oriented recommendations

You are encouraged to:
- Combine insights across multiple sections (e.g. product + delivery + reviews)
- Restate key metrics WHEN they help support an insight
- Draw careful, logical interpretations grounded strictly in the provided data

--------------------------------------------------------------------------------
IMPORTANT CONSTRAINTS
--------------------------------------------------------------------------------

- Do NOT hallucinate or assume data that is not present
- Do NOT introduce external benchmarks or industry averages
- Do NOT critique the technical implementation unless explicitly implied by results
- If evidence is insufficient to support a conclusion, state this clearly

--------------------------------------------------------------------------------
REQUIRED OUTPUT RULES (MANDATORY)
--------------------------------------------------------------------------------
1. Introduction
    - very brief about report scope, goals, etc.
2. Data Quality Analysis
    - Basic stats on total dataset/tables
    - Raw Data problems (mention which tables are problematic, breifly) 
    - Highlighting that data is cleaned, deduplicated, etc. and that the models/results are based on cleansed data
3. Data-drieven Insights
    The following insights should be discovered and explained but you can arrange and organize
    the subheaders or subsections (i.e., 3.1, 3.2, etc.) in a different order. Even if it makes sense, 
    you can combine subsections, remove them, etc. (based on your assessments).
    Here are the contents on section 3:
    - Executive Summary (Top-level KPIs, insights & strategic implications)
    - Financial Performance (Revenue & Growth) 
        - Trends, seasonality, spikes (monthly, weekly), e.g., what was the month with the best revenue
        - Anomalies in revenue or sales
    - Customer Insights
        - Cohort analysis (retention rates, and revenue performance of cohorts, etc.)
        - RFM segments insights
        - Links to revenue & product preference, Customer lifetime value implications, etc.
    - Product & Category Performance
        - Top products, bottom performers
        - Concentration & portfolio risks
        - Cross-links to revenue & demand trends
    - Operational & Delivery Insights
        - Fulfillment performance, delays, cancellations
        - Correlation to reviews and product categories (if any)      
    - Anomalies
        - Consolidated view of unusual patterns across metrics (relationship between potential events, etc.)
        - Assessing all anomaly detection reports and providing relevant insights, business impact, etc.
    - Seller Insights 
        - which region / province / city had the most revenue / customers, etc.
    - Regional insights 
        - which region / province / city had the most revenue / customers, etc.
    - Other Data Driven Insights
        - Patterns revealed by combining multiple dimensions, any other relavant insights that can give further details about the business, etc.
    (Structure section 3 and the order of contents in a proper manner)

4. Conclusions & Actionable Recommendations
   - Overview of main insights and conclusions
   - Proposed actionable recommendations for the improvement of business tasks, operations, etc. 
   - Prioritizing them if possible (i.e., Short-term vs. long-term actions)

GENERAL RULES:
- Base all insights STRICTLY on the provided content.
- Be analytical, structured, and business-focused
- Write in a clear, professional report style. 
- An explicit and relatively detailed report should be created. 
- Regarding the structure:
    - Keep the 1 to 4 sections, creation of further subsections is OK but not compulsory. 
    - If possible, add proper icons for sections, etc. to make the report visually nice and tidy.
    - Use proper paragraphs and sentences instead of listing multiple bullet points.
- MORE IMPORTANTLY, DOUBLE CHECK YOUR REPORT TO ENSURE IT FITS THE STATED REQUIREMENTS AND THERE IS NO INCONSISTENCY 
    - for example, you should not say sth in introduction that is not done
================================================================================
END OF INSTRUCTIONS
================================================================================
================================================================================
START OF DATASET INTRODUCTION AND ANALYTICAL RESULTS
================================================================================
"""

def ending_text():
    return f"""

"""


class BusinessContextBuilder:
    """
    Builds comprehensive business context from multiple JSON report files
    for LLM analysis and insight generation.
    """
    
    def __init__(self, reports_dir: str = "."):
        """
        Initialize the context builder.
        
        Args:
            reports_dir: The directory where the *output* file will be saved.
                         (e.g., 'D:/My_Projects/OLIST/python/output')
        """
        self.reports_dir = Path(reports_dir)
        self.reports = {}
        self.anomaly_reports = {}
        self.qc_reports = {}
        
        # Define subdirectories for input files
        self.analysis_subdir = "Analysis"
        self.anomaly_subdir = "Anomaly_Detection"
        self.qc_subdir = "QC_Reports"
        
    def load_report(self, filename: str, subdir: str = None) -> Dict[str, Any]:
        """
        Load a single JSON report file from the specified subdirectory.
        
        Args:
            filename: Name of the JSON file
            subdir: Subdirectory name (defaults to analysis_subdir)
        """
        subdir = subdir or self.analysis_subdir
        filepath = self.reports_dir / subdir / filename
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"Warning: {filepath} not found. Skipping.")
            return {}
        except json.JSONDecodeError as e:
            print(f"Error parsing {filepath}: {e}")
            return {}
    
    def load_all_reports(self):
        """Load all standard report files and anomaly detection reports."""
        # Load standard analysis reports
        report_files = [
            'overall_business_metrics_report.json',
            'monthly_time_series_report.json',
            'category_performance_report.json',
            'product_performance_report.json',
            'region_performance_report.json',
            'seller_performance_report.json',
            'delivery_performance_report.json',
            'cohort_analysis_report.json',
            'rfm_analysis_report.json'
        ]
        
        for filename in report_files:
            report_name = filename.replace('_report.json', '')
            self.reports[report_name] = self.load_report(filename)
        
        # Load anomaly detection reports
        anomaly_files = [
            'delivery_duration.json',
            'order_cancellations.json',
            'sales.json',
            'successful_orders.json'
        ]
        
        for filename in anomaly_files:
            report_name = filename.replace('.json', '')
            self.anomaly_reports[report_name] = self.load_report(filename, self.anomaly_subdir)

        # Load data quality basic reports from QC_Reports folder
        qc_files = [
            'ORDER_ITEMS.json',
            'PRODUCTS.json',
            'ORDER_REVIEWS.json',
            'CUSTOMERS.json',
            'SELLERS.json',
            'ORDER_PAYMENTS.json',
            'GEOLOCATION.json',
            'ORDERS.json'
        ]
        
        for filename in qc_files:
            report_name = filename.replace('.json', '')
            self.qc_reports[report_name] = self.load_report(filename, self.qc_subdir)
    
    def format_currency(self, value: float) -> str:
        """Format currency values."""
        return f"${value:,.2f}"
    
    def format_percentage(self, value: float) -> str:
        """Format percentage values."""
        return f"{value:.2f}%"
    
    def build_executive_summary(self) -> str:
        """Build executive summary section."""
        overall = self.reports.get('overall_business_metrics', {})
        metrics = overall.get('business_metrics', {})
        kpis = overall.get('key_performance_indicators', {})
        
        summary = "=" * 80 + "\n"
        summary += "EXECUTIVE SUMMARY\n"
        summary += "=" * 80 + "\n\n"
        
        
        if metrics:
            summary += "Business Overview:\n"
            summary += f"- Total Revenue (including freight, etc.): {self.format_currency(metrics.get('total_revenue', 0))}\n"
            summary += f"- Total Orders: {metrics.get('total_orders', 0):,}\n"
            summary += f"- Total Customers: {metrics.get('total_customers', 0):,}\n"
            summary += f"- Total Sellers: {metrics.get('total_sellers', 0):,}\n"
            summary += f"- Total Items Ordered: {metrics.get('total_items_ordered', 0):,}\n\n"
        
        if kpis:
            summary += "Key Performance Indicators:\n"
            summary += f"- Average Order Value: {self.format_currency(kpis.get('avg_order_value', 0))}\n"
            summary += f"- Average Basket Size: {kpis.get('avg_basket_size', 0):.2f} items\n"
            summary += f"- Revenue per Customer: {self.format_currency(kpis.get('revenue_per_customer', 0))}\n"
            summary += f"- Orders per Customer: {kpis.get('orders_per_customer', 0):.2f}\n"
            summary += f"- Revenue per Seller: {self.format_currency(kpis.get('revenue_per_seller', 0))}\n"
            summary += f"- Orders per Seller: {kpis.get('orders_per_seller', 0):.2f}\n\n"
        
        return summary
    
    def build_time_series_section(self) -> str: # monthly time series 
        """Build monthly trends section."""
        time_series = self.reports.get('monthly_time_series', {})
        summary = time_series.get('summary', {})
        monthly_data = time_series.get('monthly_data', [])
        
        section = "=" * 80 + "\n"
        section += "MONTHLY TRENDS & GROWTH\n"
        section += "=" * 80 + "\n\n"
        
        if summary:
            section += "Time Period Overview:\n"
            section += f"- Analysis Period: {summary.get('period_start')} to {summary.get('period_end')}\n"
            section += f"- Total Months: {summary.get('total_months')}\n"
            section += f"- Average Monthly Revenue: {self.format_currency(summary.get('avg_monthly_revenue', 0))}\n"
            section += f"- Average Monthly Orders: {summary.get('avg_monthly_orders', 0):,}\n\n"
        
        if monthly_data and len(monthly_data) >= 12:
            last_12_months = monthly_data[-12:]
            
            total_revenue_12m = sum(m.get('total_revenue', 0) for m in last_12_months)
            total_orders_12m = sum(m.get('total_orders', 0) for m in last_12_months)
            avg_revenue_12m = total_revenue_12m / 12
            avg_orders_12m = total_orders_12m / 12
            
            months_with_mom = [m for m in last_12_months if m.get('revenue_mom_pct') is not None]
            highest_mom = max(months_with_mom, key=lambda x: x.get('revenue_mom_pct', -999)) if months_with_mom else None
            lowest_mom = min(months_with_mom, key=lambda x: x.get('revenue_mom_pct', 999)) if months_with_mom else None
            
            section += "Last 12 Months Summary:\n"
            section += f"- Average Monthly Revenue: {self.format_currency(avg_revenue_12m)}\n"
            section += f"- Average Monthly Orders: {avg_orders_12m:,.0f}\n"
            
            if highest_mom:
                section += f"- Highest Revenue MoM Growth: {highest_mom.get('month')} ({self.format_percentage(highest_mom.get('revenue_mom_pct', 0))})\n"
            if lowest_mom:
                section += f"- Lowest Revenue MoM Growth: {lowest_mom.get('month')} ({self.format_percentage(lowest_mom.get('revenue_mom_pct', 0))})\n"
            section += "\n"
            
            section += "Last 12 Months Monthly Performance:\n"
            for month_data in last_12_months:
                mom_revenue = month_data.get('revenue_mom_pct')
                mom_orders = month_data.get('orders_mom_pct')
                section += f"\n{month_data.get('month')}:\n"
                section += f"  - Revenue: {self.format_currency(month_data.get('total_revenue', 0))}"
                if mom_revenue is not None:
                    section += f" ({self.format_percentage(mom_revenue)} MoM)\n"
                else:
                    section += "\n"
                section += f"  - Orders: {month_data.get('total_orders', 0):,}"
                if mom_orders is not None:
                    section += f" ({self.format_percentage(mom_orders)} MoM)\n"
                else:
                    section += "\n"
            section += "\n"
        elif monthly_data:
            section += f"Monthly Performance (Last {len(monthly_data)} months):\n"
            for month_data in monthly_data:
                mom_revenue = month_data.get('revenue_mom_pct')
                mom_orders = month_data.get('orders_mom_pct')
                section += f"\n{month_data.get('month')}:\n"
                section += f"  - Revenue: {self.format_currency(month_data.get('total_revenue', 0))}"
                if mom_revenue is not None:
                    section += f" ({self.format_percentage(mom_revenue)} MoM)\n"
                else:
                    section += "\n"
                section += f"  - Orders: {month_data.get('total_orders', 0):,}"
                if mom_orders is not None:
                    section += f" ({self.format_percentage(mom_orders)} MoM)\n"
                else:
                    section += "\n"
            section += "\n"
        
        return section
    
    def build_category_section(self) -> str:
        """Build category performance section."""
        category = self.reports.get('category_performance', {})
        summary = category.get('summary', {})
        top_by_revenue = category.get('top_performers', {}).get('by_revenue', {}).get('data', [])
        bottom = category.get('bottom_performers', {}).get('data', [])
        concentration = category.get('concentration_analysis', {})
        
        section = "=" * 80 + "\n"
        section += "CATEGORY PERFORMANCE\n"
        section += "=" * 80 + "\n\n"
        
        if summary:
            section += "Category Overview:\n"
            section += f"- Total Categories: {summary.get('total_categories')}\n"
            section += f"- Total Revenue: {self.format_currency(summary.get('total_revenue', 0))}\n"
            section += f"- Total Items Sold: {summary.get('total_items_sold', 0):,}\n\n"
        
        if concentration:
            section += "Market Concentration:\n"
            section += f"- Categories needed for 80% revenue: {concentration.get('categories_for_80pct_revenue')}\n"
            section += f"- Top 5 categories revenue share: {self.format_percentage(concentration.get('top_5_revenue_share_pct', 0))}\n\n"
        
        if top_by_revenue:
            section += "Top 10 Categories by Revenue:\n"
            for i, cat in enumerate(top_by_revenue[:10], 1):
                section += f"{i}. {cat.get('product_category_name')}: "
                section += f"{self.format_currency(cat.get('total_revenue', 0))} "
                section += f"({self.format_percentage(cat.get('revenue_percentage', 0))} of total, "
                section += f"{cat.get('total_items_sold', 0):,} items)\n"
            section += "\n"
        
        if bottom:
            section += "Bottom 5 Categories (Underperformers):\n"
            for i, cat in enumerate(bottom[:5], 1):
                section += f"{i}. {cat.get('product_category_name')}: "
                section += f"{self.format_currency(cat.get('total_revenue', 0))} "
                section += f"({cat.get('total_items_sold', 0)} items)\n"
            section += "\n"
        
        return section
    
    def build_regional_section(self) -> str:
        """Build regional performance section."""
        region = self.reports.get('region_performance', {})
        summary = region.get('summary', {})
        top_by_spending = region.get('top_performers', {}).get('by_total_spending', {}).get('data', [])
        top_by_avg = region.get('top_performers', {}).get('by_avg_spending_per_customer', {}).get('data', [])
        concentration = region.get('concentration_analysis', {})
        
        section = "=" * 80 + "\n"
        section += "REGIONAL PERFORMANCE\n"
        section += "=" * 80 + "\n\n"
        
        if summary:
            section += "Regional Overview:\n"
            section += f"- Total Provinces: {summary.get('total_provinces')}\n"
            section += f"- Total Customers: {summary.get('total_customers', 0):,}\n"
            section += f"- Total Spending: {self.format_currency(summary.get('total_spending', 0))}\n"
            section += f"- Avg Spending per Customer: {self.format_currency(summary.get('avg_spending_per_customer', 0))}\n\n"
        
        if concentration:
            section += "Geographic Concentration:\n"
            section += f"- Provinces needed for 80% revenue: {concentration.get('provinces_for_80pct_spending')}\n"
            section += f"- Top 5 provinces revenue share: {self.format_percentage(concentration.get('top_5_spending_share_pct', 0))}\n\n"
        
        if top_by_spending:
            section += "Top 10 Provinces by Total Spending:\n"
            for i, prov in enumerate(top_by_spending[:10], 1):
                section += f"{i}. {prov.get('province')}: "
                section += f"{self.format_currency(prov.get('total_spending', 0))} "
                section += f"({self.format_percentage(prov.get('spending_percentage', 0))} of total, "
                section += f"{prov.get('total_customers', 0):,} customers)\n"
            section += "\n"
        
        if top_by_avg:
            section += "Top 5 Provinces by Avg Customer Value:\n"
            for i, prov in enumerate(top_by_avg[:5], 1):
                section += f"{i}. {prov.get('province')}: "
                section += f"{self.format_currency(prov.get('avg_spending_per_customer', 0))} per customer "
                section += f"({prov.get('total_customers', 0):,} customers)\n"
            section += "\n"
        
        return section
    
    def build_seller_section(self) -> str:
        """Build seller performance section."""
        seller = self.reports.get('seller_performance', {})
        summary = seller.get('summary', {})
        top_revenue = seller.get('top_performers', {}).get('by_revenue', {}).get('data', [])
        top_reviews = seller.get('top_performers', {}).get('by_review_score', {}).get('data', [])
        bottom_reviews = seller.get('bottom_performers', {}).get('data', [])
        concentration = seller.get('concentration_analysis', {})
        
        section = "=" * 80 + "\n"
        section += "SELLER PERFORMANCE\n"
        section += "=" * 80 + "\n\n"
        
        if summary:
            section += "Seller Overview:\n"
            section += f"- Total Sellers: {summary.get('total_sellers', 0):,}\n"
            section += f"- Total Revenue: {self.format_currency(summary.get('total_revenue', 0))}\n"
            section += f"- Total Orders: {summary.get('total_orders', 0):,}\n"
            section += f"- Average Review Score: {summary.get('avg_review_score', 0):.2f}/5.0\n"
            section += f"- Average Delivery Days: {summary.get('avg_delivery_days', 0):.1f}\n\n"
        
        if concentration:
            section += "Seller Concentration:\n"
            section += f"- Sellers needed for 80% revenue: {concentration.get('sellers_for_80pct_revenue')}\n"
            section += f"- Top 10 sellers revenue share: {self.format_percentage(concentration.get('top_10_revenue_share_pct', 0))}\n\n"
        
        if top_revenue:
            section += "Top 5 Sellers by Revenue:\n"
            for i, s in enumerate(top_revenue[:5], 1):
                section += f"{i}. Seller {s.get('seller_id')[:8]}...: "
                section += f"{self.format_currency(s.get('total_revenue', 0))} "
                section += f"({s.get('total_orders', 0):,} orders, "
                section += f"{s.get('avg_review_score', 0):.2f}/5.0 rating)\n"
            section += "\n"
        
        if bottom_reviews:
            section += "Bottom 5 Sellers by Review Score (min 10 orders):\n"
            for i, s in enumerate(bottom_reviews[:5], 1):
                section += f"{i}. Seller {s.get('seller_id')[:8]}...: "
                section += f"{s.get('avg_review_score', 0):.2f}/5.0 rating "
                section += f"({s.get('total_orders', 0)} orders, "
                section += f"{s.get('avg_delivery_days', 0):.1f} days delivery)\n"
            section += "\n"
        
        return section
    
    def build_delivery_section(self) -> str:
        """Build delivery performance section."""
        delivery = self.reports.get('delivery_performance', {})
        summary = delivery.get('summary', {})
        time_dist = delivery.get('delivery_time_distribution', {}).get('data', [])
        delay = delivery.get('delay_analysis', {})
        
        section = "=" * 80 + "\n"
        section += "DELIVERY PERFORMANCE\n"
        section += "=" * 80 + "\n\n"
        
        if summary:
            section += "Delivery Overview:\n"
            section += f"- Total Orders: {summary.get('total_orders', 0):,}\n"
            section += f"- On-Time Rate: {self.format_percentage(summary.get('on_time_rate_pct', 0))}\n"
            section += f"- Late Deliveries: {summary.get('late_deliveries', 0):,} ({self.format_percentage(summary.get('late_rate_pct', 0))})\n"
            section += f"- Avg Actual Delivery Days: {summary.get('avg_actual_delivery_days', 0):.2f}\n"
            section += f"- Avg Fulfillment Days: {summary.get('avg_fulfillment_days', 0):.2f}\n"
            section += f"- Avg Delay vs Estimate: {summary.get('avg_delay_vs_estimate', 0):.2f} days\n\n"
        
        if time_dist:
            section += "Delivery Time Distribution:\n"
            for bracket in time_dist:
                section += f"- {bracket.get('bracket')}: {bracket.get('order_count', 0):,} orders "
                section += f"({self.format_percentage(bracket.get('percentage', 0))})\n"
            section += "\n"
        
        if delay:
            section += "Late Delivery Analysis:\n"
            section += f"- Late Orders: {delay.get('late_orders_count', 0):,}\n"
            section += f"- Avg Delay (late orders): {delay.get('avg_delay_late_orders', 0):.2f} days\n"
            section += f"- Max Delay: {delay.get('max_delay', 0)} days\n\n"
        
        return section
    
    def build_customer_section(self) -> str:
        """Build customer analysis section."""
        cohort = self.reports.get('cohort_analysis', {})
        rfm = self.reports.get('rfm_analysis', {})
        
        section = "=" * 80 + "\n"
        section += "CUSTOMER ANALYSIS\n"
        section += "=" * 80 + "\n\n"
        
        rfm_summary = rfm.get('summary', {})
        rfm_segments = rfm.get('rfm_segment_distribution', {}).get('data', [])
        rfm_stats = rfm.get('rfm_statistics', {})
        
        if rfm_summary or rfm_stats:
            section += "RFM Analysis Overview (12-month period: 2017-11-01 to 2018-11-01):\n"
            
            if rfm_stats:
                section += "\nRecency (days since last purchase):\n"
                section += f"  - Min: {rfm_stats.get('recency', {}).get('min', 0):.0f} days\n"
                section += f"  - Median: {rfm_stats.get('recency', {}).get('median', 0):.0f} days\n"
                section += f"  - Max: {rfm_stats.get('recency', {}).get('max', 0):.0f} days\n"
                
                section += "\nFrequency (number of orders):\n"
                section += f"  - Min: {rfm_stats.get('frequency', {}).get('min', 0):.0f} orders\n"
                section += f"  - Median: {rfm_stats.get('frequency', {}).get('median', 0):.0f} orders\n"
                section += f"  - Max: {rfm_stats.get('frequency', {}).get('max', 0):.0f} orders\n"
                
                section += "\nMonetary Value (total spending):\n"
                section += f"  - Min: {self.format_currency(rfm_stats.get('monetary', {}).get('min', 0))}\n"
                section += f"  - Median: {self.format_currency(rfm_stats.get('monetary', {}).get('median', 0))}\n"
                section += f"  - Max: {self.format_currency(rfm_stats.get('monetary', {}).get('max', 0))}\n"
                section += "\n"
        
        if rfm_segments:
            section += "Customer Segmentation (RFM):\n"
            for seg in rfm_segments:
                section += f"\n{seg.get('segment')}:\n"
                section += f"  - Customers: {seg.get('customer_count', 0):,} "
                section += f"({self.format_percentage(seg.get('percentage_of_customers', 0))})\n"
                section += f"  - Revenue: {self.format_currency(seg.get('total_revenue', 0))} "
                section += f"({self.format_percentage(seg.get('percentage_of_revenue', 0))})\n"
                section += f"  - Orders: {seg.get('total_orders', 0):,}\n"
            section += "\n"
        
        cohort_summary = cohort.get('overall_summary', {})
        cohort_data = cohort.get('average_by_period_index', {}).get('data', [])
        
        if cohort_summary:
            section += "Cohort Analysis Overview:\n"
            section += f"- Analysis Period: {cohort_summary.get('analysis_period_start')} to {cohort_summary.get('analysis_period_end')}\n"
            section += f"- Total Cohorts: {cohort_summary.get('total_cohorts')}\n\n"
        
        if cohort_data:
            section += "Average Retention and Revenue by Period (months since first purchase):\n"
            for period in cohort_data:
                section += f"Month {period.get('period_index')}: "
                section += f"{self.format_percentage(period.get('retention_rate', 0))} retention, "
                section += f"{self.format_currency(period.get('avg_revenue_per_cohort', 0))} avg revenue, "
                section += f"({period.get('total_active_customers', 0):,} active customers)\n"
            section += "\n"
        
        return section
    
    def build_product_section(self) -> str:
        """Build product performance section."""
        product = self.reports.get('product_performance', {})
        summary = product.get('summary', {})
        top_revenue = product.get('top_performers', {}).get('by_revenue', {}).get('data', [])
        correlation = product.get('correlation_analysis', {})
        
        section = "=" * 80 + "\n"
        section += "PRODUCT PERFORMANCE\n"
        section += "=" * 80 + "\n\n"
        
        if summary:
            section += "Product Overview:\n"
            section += f"- Total Products: {summary.get('total_products', 0):,}\n"
            section += f"- Total Revenue: {self.format_currency(summary.get('total_revenue', 0))}\n"
            section += f"- Total Items Sold: {summary.get('total_items_sold', 0):,}\n"
            section += f"- Avg Review Score: {summary.get('avg_review_score', 0):.2f}/5.0\n"
            section += f"- Avg Delivery Days: {summary.get('avg_delivery_days', 0):.1f}\n\n"
        
        if top_revenue:
            section += "Top 10 Products by Revenue:\n"
            for i, prod in enumerate(top_revenue[:10], 1):
                section += f"{i}. Product {prod.get('product_id')[:8]}... ({prod.get('product_category_name')}): "
                section += f"{self.format_currency(prod.get('total_revenue', 0))} "
                section += f"({prod.get('total_items_sold', 0):,} items, "
                section += f"{prod.get('avg_review_score', 0):.2f}/5.0)\n"
            section += "\n"
        
        if correlation:
            corr_matrix = correlation.get('correlation_matrix', {})
            section += "Product Correlation Insights:\n"
            section += f"- Items Sold vs Review Score: {corr_matrix.get('items_sold_vs_review_score', 0):.3f}\n"
            section += f"- Items Sold vs Delivery Days: {corr_matrix.get('items_sold_vs_delivery_days', 0):.3f}\n"
            section += f"- Review Score vs Delivery Days: {corr_matrix.get('review_score_vs_delivery_days', 0):.3f}\n\n"
        
        return section
    
    def build_anomaly_section(self) -> str:
        """Build anomaly detection section."""
        section = "=" * 80 + "\n"
        section += "ANOMALY DETECTION ANALYSIS\n"
        section += "=" * 80 + "\n\n"
        
        if not self.anomaly_reports:
            section += "No anomaly detection reports available.\n\n"
            return section
        
        # Sales Anomalies
        sales = self.anomaly_reports.get('sales', {})
        if sales:
            section += "Sales Anomalies:\n"
            pipeline = sales.get('pipeline_run_details', {})
            section += f"- Method: {pipeline.get('method', 'N/A')}\n"
            section += f"- Analysis Mode: {pipeline.get('analysis_mode', 'N/A')}\n\n"
            
            for check in sales.get('anomaly_checks', []):
                freq = check.get('frequency', 'N/A')
                total = check.get('total_points', 0)
                anomaly_count = check.get('anomaly_count', 0)
                anomaly_pct = (anomaly_count / total * 100) if total > 0 else 0
                
                section += f"{freq} Analysis:\n"
                section += f"  - Total Data Points: {total:,}\n"
                section += f"  - Anomalies Detected: {anomaly_count} ({anomaly_pct:.2f}%)\n"
                section += f"  - Detection Limits: {check.get('limit_description', 'N/A')}\n"
                
                anomalies = check.get('anomalies', [])
                if anomalies:
                    section += f"  - Top Anomalies:\n"
                    for i, anom in enumerate(anomalies[:5], 1):
                        val = anom.get('value', 0)
                        section += f"    {i}. {anom.get('index_id')}: {self.format_currency(val)} ({anom.get('type')})\n"
            section += "\n" + "-" * 80 + "\n"
        
        # Order Cancellation Anomalies
        cancellations = self.anomaly_reports.get('order_cancellations', {})
        if cancellations:
            section += "Order Cancellation Anomalies:\n"
            pipeline = cancellations.get('pipeline_run_details', {})
            section += f"- Method: {pipeline.get('method', 'N/A')}\n"
            section += f"- Analysis Mode: {pipeline.get('analysis_mode', 'N/A')}\n\n"
            
            for check in cancellations.get('anomaly_checks', []):
                freq = check.get('frequency', 'N/A')
                total = check.get('total_points', 0)
                anomaly_count = check.get('anomaly_count', 0)
                anomaly_pct = (anomaly_count / total * 100) if total > 0 else 0
                
                section += f"{freq} Analysis:\n"
                section += f"  - Total Data Points: {total:,}\n"
                section += f"  - Anomalies Detected: {anomaly_count} ({anomaly_pct:.2f}%)\n"
                section += f"  - Detection Limits: {check.get('limit_description', 'N/A')}\n"
                
                anomalies = check.get('anomalies', [])
                if anomalies:
                    section += f"  - Top Anomalies:\n"
                    for i, anom in enumerate(anomalies[:5], 1):
                        section += f"    {i}. {anom.get('index_id')}: {anom.get('value')} cancellations ({anom.get('type')})\n"
            section += "\n" + "-" * 80 + "\n"
        
        # Successful Orders Anomalies
        orders = self.anomaly_reports.get('successful_orders', {})
        if orders:
            section += "Successful Orders Anomalies:\n"
            pipeline = orders.get('pipeline_run_details', {})
            section += f"- Method: {pipeline.get('method', 'N/A')}\n"
            section += f"- Analysis Mode: {pipeline.get('analysis_mode', 'N/A')}\n\n"
            
            for check in orders.get('anomaly_checks', []):
                freq = check.get('frequency', 'N/A')
                total = check.get('total_points', 0)
                anomaly_count = check.get('anomaly_count', 0)
                anomaly_pct = (anomaly_count / total * 100) if total > 0 else 0
                
                section += f"{freq} Analysis:\n"
                section += f"  - Total Data Points: {total:,}\n"
                section += f"  - Anomalies Detected: {anomaly_count} ({anomaly_pct:.2f}%)\n"
                section += f"  - Detection Limits: {check.get('limit_description', 'N/A')}\n"
                
                anomalies = check.get('anomalies', [])
                if anomalies:
                    section += f"  - Top Anomalies:\n"
                    for i, anom in enumerate(anomalies[:5], 1):
                        section += f"    {i}. {anom.get('index_id')}: {anom.get('value')} orders ({anom.get('type')})\n"
            section += "\n" + "-" * 80 + "\n"
        
        # Delivery Duration Anomalies
        delivery = self.anomaly_reports.get('delivery_duration', {})
        if delivery:
            section += "Delivery Duration Anomalies:\n"
            pipeline = delivery.get('pipeline_run_details', {})
            section += f"- Method: {pipeline.get('method', 'N/A')}\n"
            section += f"- Analysis Mode: {pipeline.get('analysis_mode', 'N/A')}\n\n"
            
            for check in delivery.get('anomaly_checks', []):
                freq = check.get('frequency', 'N/A')
                total = check.get('total_points', 0)
                anomaly_count = check.get('anomaly_count', 0)
                anomaly_pct = (anomaly_count / total * 100) if total > 0 else 0
                
                section += f"Summary:\n"
                section += f"  - Total Data Points: {total:,}\n"
                section += f"  - Anomalies Detected: {anomaly_count} ({anomaly_pct:.2f}%)\n"
                section += f"  - Detection Limits: {check.get('limit_description', 'N/A')}\n"
                
                anomalies = check.get('anomalies', [])
                if anomalies:
                    section += f"  - Notable Anomalies:\n"
                    for i, anom in enumerate(anomalies[:5], 1):
                        section += f"    {i}. {anom.get('index_id')}: {anom.get('value'):.2f} days ({anom.get('type')})\n"
                section += "\n"
        
        return section
    
    def build_data_quality_section(self) -> str:
        """Build data quality section from QC reports."""
        if not self.qc_reports:
            return ""
        
        section = "=" * 80 + "\n"
        section += "DATA QUALITY ASSESSMENT\n"
        section += "=" * 80 + "\n\n"
        
        section += "OVERALL DATASET SUMMARY:\n"
        section += "-" * 40 + "\n"
        
        # Calculate overall statistics
        total_rows_all = 0
        total_columns_all = 0
        total_duplicated_rows = 0
        
        for table_name, report in self.qc_reports.items():
            if report:
                total_rows_all += report.get('total_rows', 0)
                total_columns_all += report.get('total_columns', 0)
                total_duplicated_rows += report.get('total_duplicated_rows', 0)
        
        section += f"- Total Datasets Analyzed: {len(self.qc_reports)}\n"
        section += f"- Combined Total Rows: {total_rows_all:,}\n"
        section += f"- Combined Total Columns: {total_columns_all}\n"
        section += f"- Total Duplicated Rows: {total_duplicated_rows:,}\n\n"
        
        # Detailed table-by-table analysis
        section += "TABLE-LEVEL DATA QUALITY METRICS:\n"
        section += "-" * 40 + "\n\n"
        
        for table_name, report in sorted(self.qc_reports.items()):
            if not report:
                continue
                
            df_name = report.get('df_name', table_name)
            total_rows = report.get('total_rows', 0)
            total_columns = report.get('total_columns', 0)
            total_duplicated = report.get('total_duplicated_rows', 0)
            
            section += f"- {df_name}:\n"
            section += f"   • Rows: {total_rows:,}\n"
            section += f"   • Columns: {total_columns}\n"
            section += f"   • Duplicated Rows: {total_duplicated:,} "
            
            if total_rows > 0:
                dup_percentage = (total_duplicated / total_rows) * 100
                section += f"({dup_percentage:.2f}%)\n"
            else:
                section += "\n"
            
            # Column-level quality metrics
            column_qc = report.get('column_qc', {})
            if column_qc:
                # Count null columns
                columns_with_nulls = 0
                total_null_count = 0
                
                for col_name, col_info in column_qc.items():
                    null_count = col_info.get('null_count', 0)
                    if null_count > 0:
                        columns_with_nulls += 1
                        total_null_count += null_count
                
                if columns_with_nulls > 0:
                    section += f"   • Columns with Nulls: {columns_with_nulls}/{total_columns}\n"
                    section += f"   • Total Null Values: {total_null_count:,}\n"
                
                # Find unique value counts
                unique_counts = []
                for col_name, col_info in column_qc.items():
                    unique_count = col_info.get('unique_count')
                    if unique_count is not None:
                        unique_counts.append((col_name, unique_count))
                
                if unique_counts:
                    # Sort by unique count (ascending)
                    unique_counts.sort(key=lambda x: x[1])
                    
                    # Show low cardinality columns (less than 10 unique values)
                    low_cardinality = [(name, count) for name, count in unique_counts if count < 10]
                    if low_cardinality:
                        section += f"   • Low Cardinality Columns (<10 unique values): {len(low_cardinality)}\n"
                        for name, count in low_cardinality[:3]:  # Show first 3
                            section += f"     - {name}: {count} unique values\n"
                
                # Show data type distribution
                dtype_counts = {}
                for col_name, col_info in column_qc.items():
                    dtype = col_info.get('dtype', 'unknown')
                    dtype_counts[dtype] = dtype_counts.get(dtype, 0) + 1
                
                section += f"   • Data Types:\n"
                for dtype, count in sorted(dtype_counts.items()):
                    section += f"     - {dtype}: {count} columns\n"
            
            section += "\n"
        
        # Data quality issues summary
        section += "DATA QUALITY ISSUES SUMMARY:\n"
        section += "-" * 40 + "\n\n"
        
        issues_found = []
        
        for table_name, report in self.qc_reports.items():
            if not report:
                continue
                
            column_qc = report.get('column_qc', {})
            if not column_qc:
                continue
            
            for col_name, col_info in column_qc.items():
                null_count = col_info.get('null_count', 0)
                null_percent = col_info.get('null_percent', 0.0)
                
                if null_percent > 5.0:  # Flag columns with >5% nulls
                    issues_found.append({
                        'table': table_name,
                        'column': col_name,
                        'null_percent': null_percent,
                        'null_count': null_count
                    })
        
        if issues_found:
            section += f"-  High Null Percentage (>5%) Columns Found: {len(issues_found)}\n\n"
            # Sort by null percentage descending
            issues_found.sort(key=lambda x: x['null_percent'], reverse=True)
            
            for issue in issues_found[:5]:  # Show top 5
                section += f"• {issue['table']}.{issue['column']}: "
                section += f"{issue['null_percent']:.3f}% null ({issue['null_count']:,} null values)\n"
        else:
            section += "- No major data quality issues detected (all columns have <5% null values)\n"
        
        section += "\n"
        
        # Data completeness summary
        section += "DATA COMPLETENESS SUMMARY:\n"
        section += "-" * 40 + "\n\n"
        
        total_columns_all = 0
        complete_columns = 0
        
        for table_name, report in self.qc_reports.items():
            if not report:
                continue
                
            column_qc = report.get('column_qc', {})
            total_columns = report.get('total_columns', 0)
            total_columns_all += total_columns
            
            for col_name, col_info in column_qc.items():
                null_count = col_info.get('null_count', 0)
                if null_count == 0:
                    complete_columns += 1
        
        if total_columns_all > 0:
            completeness_percentage = (complete_columns / total_columns_all) * 100
            section += f"- Total Columns Across All Tables: {total_columns_all}\n"
            section += f"- Fully Complete Columns (0% nulls): {complete_columns}\n"
            section += f"- Data Completeness Score: {completeness_percentage:.2f}%\n"
        else:
            section += "- No column information available\n"
        
        section += "\n"
        return section
    
    def build_full_context(self) -> str:
        """Build the complete business context."""
        context = "\n"
        context += intro_text()
        context += "\n"
        context += "=" * 80 + "\n"
        context += "COMPREHENSIVE BUSINESS INTELLIGENCE REPORT\n"
        context += f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        context += "NOTE: The following report represents analyst-generated outputs and metrics."
        context += "\n      It has NOT been independently validated and is subject to review.\n"

        # Main Analytics Reports
        context += self.build_executive_summary()
        # Data Quality section
        context += self.build_data_quality_section()
        context += "NOTE THAT DATA QC REPORT REFLECTS PROBLEMS IN DATA SOURCES. THE DATA IS CLEANED (DEDUPLICATED,ETC.) FOR ANALYTICS."
        # Other Analytics
        context += self.build_time_series_section()
        context += self.build_product_section()
        context += self.build_category_section()
        context += self.build_regional_section()
        context += self.build_delivery_section()
        context += self.build_customer_section()
        context += self.build_seller_section()
        
        # Anomaly Detection
        context += self.build_anomaly_section()

        context += "=" * 80 + "\n"
        context += "END OF REPORT\n"
        context += "=" * 80
        context += ending_text()
    
        
        return context
    
    def save_context(self, output_file: str = "business_context.txt"):
        """Save the built context to a file."""
        context = self.build_full_context()
        output_path = self.reports_dir / output_file
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(context)
        
        print(f"Business context saved to: {output_path}")
        print(f"Total length: {len(context):,} characters")
        return output_path
    

def main():
    """Main execution function."""
    # 🎯 User-specified path: D:/My_Projects/OLIST/python/output
    reports_directory = "D:/My_Projects/OLIST/python/output"
    
    # Initialize the builder with the base directory
    builder = BusinessContextBuilder(reports_dir=reports_directory)
    
    # Load all reports
    print(f"Loading reports from: {reports_directory}/{builder.analysis_subdir}/...")
    print(f"Loading QC reports from: {reports_directory}/{builder.qc_subdir}/...")
    builder.load_all_reports()
    
    # Build and save context
    print(f"Building and saving business context to {reports_directory}...")
    # This will save to D:/My_Projects/OLIST/python/output/business_context.txt
    output_path = builder.save_context("business_context.txt")
    
    # Also print to console for preview
    print("\n" + "="*80)
    print("PREVIEW (first 2000 characters):")
    print("="*80)
    context = builder.build_full_context()
    print(context[:2000])
    print("\n... (truncated)")
    
    return output_path


# Quick usage function
def quick_build(reports_dir="."):
    """Quick function to build context from a directory."""
    builder = BusinessContextBuilder(reports_dir=reports_dir)
    builder.load_all_reports()
    output_path = builder.save_context("business_context.txt")
    return builder, output_path


if __name__ == "__main__":
    main()