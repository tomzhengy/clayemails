#!/usr/bin/env python3
"""
Analyze email addresses in the output CSV file to provide detailed statistics.
"""

import pandas as pd
import sys
from collections import Counter

def analyze_email_domains(input_file='output.csv'):
    """
    Analyze email addresses and provide domain statistics.
    
    Args:
        input_file: Path to CSV file with email data
    """
    try:
        # Read the CSV file
        print(f"📖 Reading file: {input_file}")
        df = pd.read_csv(input_file)
        total_rows = len(df)
        print(f"   Total rows: {total_rows}")
        
        # Email columns
        work_email_col = 'Work Email (Clado)'
        personal_email_col = 'Personal Email (Clado)'
        
        # Collect all email addresses
        all_emails = []
        work_emails = []
        personal_emails = []
        
        for _, row in df.iterrows():
            # Process work emails
            if pd.notna(row.get(work_email_col)) and row.get(work_email_col):
                emails = str(row[work_email_col]).split(';')
                for email in emails:
                    email = email.strip()
                    if email and '@' in email:
                        work_emails.append(email)
                        all_emails.append(email)
            
            # Process personal emails
            if pd.notna(row.get(personal_email_col)) and row.get(personal_email_col):
                emails = str(row[personal_email_col]).split(';')
                for email in emails:
                    email = email.strip()
                    if email and '@' in email:
                        personal_emails.append(email)
                        all_emails.append(email)
        
        # Extract domains
        work_domains = [email.split('@')[1].lower() for email in work_emails if '@' in email]
        personal_domains = [email.split('@')[1].lower() for email in personal_emails if '@' in email]
        all_domains = [email.split('@')[1].lower() for email in all_emails if '@' in email]
        
        # Count domains
        work_domain_counts = Counter(work_domains)
        personal_domain_counts = Counter(personal_domains)
        all_domain_counts = Counter(all_domains)
        
        # Print statistics
        print(f"\n{'='*60}")
        print(f"📊 EMAIL ANALYSIS REPORT")
        print(f"{'='*60}")
        
        print(f"\n📧 Overall Statistics:")
        print(f"   Total unique email addresses: {len(set(all_emails))}")
        print(f"   Total work emails: {len(work_emails)}")
        print(f"   Total personal emails: {len(personal_emails)}")
        print(f"   Unique domains: {len(all_domain_counts)}")
        
        # People statistics
        has_work = df[work_email_col].notna() & (df[work_email_col] != '')
        has_personal = df[personal_email_col].notna() & (df[personal_email_col] != '')
        has_both = has_work & has_personal
        has_any = has_work | has_personal
        
        print(f"\n👥 People Statistics:")
        print(f"   People with work email: {has_work.sum()} ({has_work.sum()/total_rows*100:.1f}%)")
        print(f"   People with personal email: {has_personal.sum()} ({has_personal.sum()/total_rows*100:.1f}%)")
        print(f"   People with both types: {has_both.sum()} ({has_both.sum()/total_rows*100:.1f}%)")
        print(f"   People with any email: {has_any.sum()} ({has_any.sum()/total_rows*100:.1f}%)")
        print(f"   People without email: {(~has_any).sum()} ({(~has_any).sum()/total_rows*100:.1f}%)")
        
        # Top domains
        print(f"\n🏢 Top 20 Email Domains (All):")
        for domain, count in all_domain_counts.most_common(20):
            percentage = count / len(all_emails) * 100
            print(f"   {domain:30} {count:5} emails ({percentage:.1f}%)")
        
        if work_domains:
            print(f"\n💼 Top 10 Work Email Domains:")
            for domain, count in work_domain_counts.most_common(10):
                percentage = count / len(work_emails) * 100
                print(f"   {domain:30} {count:5} emails ({percentage:.1f}%)")
        
        if personal_domains:
            print(f"\n📧 Top 10 Personal Email Domains:")
            for domain, count in personal_domain_counts.most_common(10):
                percentage = count / len(personal_emails) * 100
                print(f"   {domain:30} {count:5} emails ({percentage:.1f}%)")
        
        # Academic domains
        academic_domains = ['.edu', '.ac.', 'university', 'college', 'institute']
        academic_emails = [email for email in all_emails if any(edu in email.lower() for edu in academic_domains)]
        
        if academic_emails:
            print(f"\n🎓 Academic Emails:")
            print(f"   Total academic emails: {len(academic_emails)} ({len(academic_emails)/len(all_emails)*100:.1f}% of all emails)")
            
            # Count academic domains
            academic_domain_list = [email.split('@')[1].lower() for email in academic_emails if '@' in email]
            academic_domain_counts = Counter(academic_domain_list)
            print(f"   Top 10 academic domains:")
            for domain, count in academic_domain_counts.most_common(10):
                print(f"      {domain:30} {count:5} emails")
        
        # Corporate domains (non-academic)
        corporate_emails = [email for email in all_emails if not any(edu in email.lower() for edu in academic_domains)]
        if corporate_emails:
            print(f"\n🏢 Corporate Emails:")
            print(f"   Total corporate emails: {len(corporate_emails)} ({len(corporate_emails)/len(all_emails)*100:.1f}% of all emails)")
        
        print(f"\n{'='*60}")
        
    except FileNotFoundError:
        print(f"❌ Error: File '{input_file}' not found")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Main function."""
    # Default input file
    input_file = 'output.csv'
    
    # Check if a filename was provided as command line argument
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    
    # Run the analysis
    analyze_email_domains(input_file)

if __name__ == "__main__":
    main()
