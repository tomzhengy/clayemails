#!/usr/bin/env python3
"""
Script to filter Turing Data Sheet to ensure each person has only one email.
Prioritizes personal emails (gmail.com, yahoo.com, hotmail.com, outlook.com, etc.)
If no personal email exists, picks one corporate email.
"""

import csv
import sys
from typing import List, Optional, Dict

# Define personal email domains (common personal email providers)
PERSONAL_EMAIL_DOMAINS = {
    'gmail.com',
    'yahoo.com',
    'hotmail.com',
    'outlook.com',
    'aol.com',
    'icloud.com',
    'me.com',
    'mac.com',
    'live.com',
    'msn.com',
    'yahoo.co.uk',
    'yahoo.fr',
    'yahoo.de',
    'yahoo.co.in',
    'yahoo.ca',
    'googlemail.com',
    'protonmail.com',
    'proton.me',
    'ymail.com',
    'rocketmail.com',
    'mail.com',
    'zoho.com',
    'fastmail.com',
    'gmx.com',
    'gmx.net',
    'inbox.com',
    'comcast.net',
    'verizon.net',
    'att.net',
    'sbcglobal.net',
    'bellsouth.net',
    'cox.net',
    'earthlink.net',
    'charter.net',
    'shaw.ca',
    'rogers.com',
    'sympatico.ca',
    'qq.com',
    '163.com',
    '126.com',
    'sina.com',
    'naver.com',
    'hanmail.net',
    'daum.net'
}

def is_personal_email(email: str) -> bool:
    """Check if an email is from a personal email provider."""
    if not email or '@' not in email:
        return False
    
    domain = email.lower().split('@')[-1].strip()
    return domain in PERSONAL_EMAIL_DOMAINS

def extract_emails_from_row(row: Dict[str, str]) -> List[str]:
    """Extract all valid emails from a row."""
    emails = []
    
    # Check all potential email columns
    email_columns = [
        'Email 1', 'Email 2', 'Email 3', 'Email 4',
        'Work Email (Clado)', 'Personal Email (Clado)'
    ]
    
    for col in email_columns:
        if col in row:
            email = row[col].strip()
            # Validate email has @ symbol and is not empty
            if email and '@' in email and email not in emails:
                emails.append(email)
    
    return emails

def select_best_email(emails: List[str]) -> Optional[str]:
    """
    Select the best email from a list of emails.
    Prioritizes personal emails, then returns the first corporate email if no personal found.
    """
    if not emails:
        return None
    
    # First, try to find a personal email
    for email in emails:
        if is_personal_email(email):
            return email
    
    # If no personal email found, return the first corporate email
    return emails[0]

def process_csv(input_file: str, output_file: str):
    """Process the CSV file and create a new one with one email per person."""
    
    print(f"Reading from: {input_file}")
    print(f"Writing to: {output_file}")
    
    with open(input_file, 'r', encoding='utf-8', errors='ignore') as infile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames
        
        if not fieldnames:
            print("Error: Could not read CSV headers")
            return
        
        # Remove the multiple email columns and add a single 'Email' column
        new_fieldnames = []
        email_columns_to_remove = [
            'Email 1', 'Email 2', 'Email 3', 'Email 4',
            'Work Email (Clado)', 'Personal Email (Clado)'
        ]
        
        for field in fieldnames:
            if field not in email_columns_to_remove:
                new_fieldnames.append(field)
        
        # Add the single Email column after Last Name
        try:
            last_name_index = new_fieldnames.index('Last Name')
            new_fieldnames.insert(last_name_index + 1, 'Email')
        except ValueError:
            # If Last Name not found, add Email at the beginning
            new_fieldnames.insert(0, 'Email')
        
        # Process rows
        processed_rows = []
        stats = {
            'total_rows': 0,
            'rows_with_email': 0,
            'rows_without_email': 0,
            'personal_emails': 0,
            'corporate_emails': 0
        }
        
        for row in reader:
            stats['total_rows'] += 1
            
            # Extract all emails from the row
            emails = extract_emails_from_row(row)
            
            # Select the best email
            selected_email = select_best_email(emails)
            
            # Create new row with single email
            new_row = {}
            for field in new_fieldnames:
                if field == 'Email':
                    new_row[field] = selected_email if selected_email else ''
                elif field in row:
                    new_row[field] = row[field]
                else:
                    new_row[field] = ''
            
            # Update statistics
            if selected_email:
                stats['rows_with_email'] += 1
                if is_personal_email(selected_email):
                    stats['personal_emails'] += 1
                else:
                    stats['corporate_emails'] += 1
            else:
                stats['rows_without_email'] += 1
            
            processed_rows.append(new_row)
        
        # Write the output file
        with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=new_fieldnames)
            writer.writeheader()
            writer.writerows(processed_rows)
        
        # Print statistics
        print("\n" + "="*50)
        print("PROCESSING COMPLETE")
        print("="*50)
        print(f"Total rows processed: {stats['total_rows']}")
        print(f"Rows with email: {stats['rows_with_email']}")
        print(f"Rows without email: {stats['rows_without_email']}")
        print(f"Personal emails selected: {stats['personal_emails']}")
        print(f"Corporate emails selected: {stats['corporate_emails']}")
        print("="*50)

def main():
    """Main function."""
    input_file = 'Turing Data Sheet (2).csv'
    output_file = 'Turing_Data_Sheet_One_Email.csv'
    
    try:
        process_csv(input_file, output_file)
        print(f"\nOutput saved to: {output_file}")
    except FileNotFoundError:
        print(f"Error: Input file '{input_file}' not found.")
        sys.exit(1)
    except Exception as e:
        print(f"Error processing file: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
