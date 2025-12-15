import json
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime


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
        # This path (self.reports_dir) will be used for saving the final TXT file.
        self.reports_dir = Path(reports_dir)
        self.reports = {}
        
        # 💡 NEW: Define a specific subdirectory where the *input JSON files* reside.
        # This is relative to the reports_dir you passed in (D:/.../output)
        self.analysis_subdir = "Analysis" 
        
    def load_report(self, filename: str) -> Dict[str, Any]:
        """
        Load a single JSON report file from the defined 'Analysis' subdirectory.
        """
        # 💡 UPDATED: Construct the path to include the 'Analysis' subdirectory.
        filepath = self.reports_dir / self.analysis_subdir / filename
        
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
        """Load all standard report files."""
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
            # Calls the updated load_report which looks in the subfolder
            self.reports[report_name] = self.load_report(filename)
    
    def format_currency(self, value: float) -> str:
        """Format currency values."""
        return f"${value:,.2f}"
    
    def format_percentage(self, value: float) -> str:
        """Format percentage values."""
        return f"{value:.2f}%"
    
    # ... (All build_section methods remain the same) ...
    # Due to length, I'm omitting the section build methods here, 
    # but they are unchanged from your original working code.
    
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
    
    def build_time_series_section(self) -> str:
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
            # Get last 12 months
            last_12_months = monthly_data[-12:]
            
            # Calculate averages for last 12 months
            total_revenue_12m = sum(m.get('total_revenue', 0) for m in last_12_months)
            total_orders_12m = sum(m.get('total_orders', 0) for m in last_12_months)
            avg_revenue_12m = total_revenue_12m / 12
            avg_orders_12m = total_orders_12m / 12
            
            # Find highest and lowest MoM revenue growth (excluding first month)
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
                section += f"  - Revenue: {self.format_currency(month_data.get('total_revenue', 0))}"
                if mom_revenue is not None:
                    section += f" ({self.format_percentage(mom_revenue)} MoM)\n"
                else:
                    section += "\n"
                section += f"  - Orders: {month_data.get('total_orders', 0):,}"
                if mom_orders is not None:
                    section += f" ({self.format_percentage(mom_orders)} MoM)\n"
                else:
                    section += "\n"
            section += "\n"
        elif monthly_data:
            # If less than 12 months, show what we have
            section += f"Monthly Performance (Last {len(monthly_data)} months):\n"
            for month_data in monthly_data:
                mom_revenue = month_data.get('revenue_mom_pct')
                mom_orders = month_data.get('orders_mom_pct')
                section += f"\n{month_data.get('month')}:\n"
                section += f"  - Revenue: {self.format_currency(month_data.get('total_revenue', 0))}"
                if mom_revenue is not None:
                    section += f" ({self.format_percentage(mom_revenue)} MoM)\n"
                else:
                    section += "\n"
                section += f"  - Orders: {month_data.get('total_orders', 0):,}"
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
        
        # RFM Analysis
        rfm_summary = rfm.get('summary', {})
        rfm_segments = rfm.get('rfm_segment_distribution', {}).get('data', [])
        rfm_stats = rfm.get('rfm_statistics', {})
        
        if rfm_summary or rfm_stats:
            section += "RFM Analysis Overview (12-month period: 2017-11-01 to 2018-11-01):\n"
            
            # Add RFM statistics if available
            if rfm_stats:
                section += "\nRecency (days since last purchase):\n"
                section += f"  - Min: {rfm_stats.get('recency', {}).get('min', 0):.0f} days\n"
                section += f"  - Median: {rfm_stats.get('recency', {}).get('median', 0):.0f} days\n"
                section += f"  - Max: {rfm_stats.get('recency', {}).get('max', 0):.0f} days\n"
                
                section += "\nFrequency (number of orders):\n"
                section += f"  - Min: {rfm_stats.get('frequency', {}).get('min', 0):.0f} orders\n"
                section += f"  - Median: {rfm_stats.get('frequency', {}).get('median', 0):.0f} orders\n"
                section += f"  - Max: {rfm_stats.get('frequency', {}).get('max', 0):.0f} orders\n"
                
                section += "\nMonetary Value (total spending):\n"
                section += f"  - Min: {self.format_currency(rfm_stats.get('monetary', {}).get('min', 0))}\n"
                section += f"  - Median: {self.format_currency(rfm_stats.get('monetary', {}).get('median', 0))}\n"
                section += f"  - Max: {self.format_currency(rfm_stats.get('monetary', {}).get('max', 0))}\n"
                section += "\n"
        
        if rfm_segments:
            section += "Customer Segmentation (RFM):\n"
            for seg in rfm_segments:
                section += f"\n{seg.get('segment')}:\n"
                section += f"  - Customers: {seg.get('customer_count', 0):,} "
                section += f"({self.format_percentage(seg.get('percentage_of_customers', 0))})\n"
                section += f"  - Revenue: {self.format_currency(seg.get('total_revenue', 0))} "
                section += f"({self.format_percentage(seg.get('percentage_of_revenue', 0))})\n"
                section += f"  - Orders: {seg.get('total_orders', 0):,}\n"
            section += "\n"
        
        # Cohort Analysis
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
    
    # ... (End of build_section methods) ...
    
    def build_full_context(self) -> str:
        """Build the complete business context."""
        context = "\n"
        context += "█" * 80 + "\n"
        context += "COMPREHENSIVE BUSINESS INTELLIGENCE REPORT\n"
        context += f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        context += "█" * 80 + "\n\n"
        
        context += self.build_executive_summary()
        context += self.build_time_series_section()
        context += self.build_category_section()
        context += self.build_regional_section()
        context += self.build_seller_section()
        context += self.build_delivery_section()
        context += self.build_customer_section()
        context += self.build_product_section()
        
        context += "=" * 80 + "\n"
        context += "END OF REPORT\n"
        context += "=" * 80 + "\n"
        
        return context
    
    def save_context(self, output_file: str = "business_context.txt"):
        """Save the built context to a file in the reports_dir."""
        context = self.build_full_context()
        # 💡 This now saves directly into the path passed to __init__
        output_path = self.reports_dir / output_file
        
        # Ensure the output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
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