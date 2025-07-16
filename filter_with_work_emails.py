import pandas as pd
import sys

def filter_csv_with_work_emails(input_file, output_file=None):
    """
    Filter a CSV file to keep only rows with work emails.
    
    Args:
        input_file: Path to input CSV file
        output_file: Path to output CSV file (if None, adds '_with_work_emails' to input filename)
    """
    try:
        # Read the CSV file
        print(f"Reading file: {input_file}")
        df = pd.read_csv(input_file)
        
        # Check if Work Email column exists
        if 'Work Email (Clado)' not in df.columns:
            print("Error: 'Work Email (Clado)' column not found in CSV")
            print(f"Available columns: {list(df.columns)}")
            return None
        
        # Get initial count
        initial_count = len(df)
        
        # Filter rows where Work Email is not empty
        # Consider both NaN and empty string as empty
        df_filtered = df[df['Work Email (Clado)'].notna() & (df['Work Email (Clado)'] != '')]
        
        # Get filtered count
        filtered_count = len(df_filtered)
        removed_count = initial_count - filtered_count
        
        # Determine output filename
        if output_file is None:
            base_name = input_file.rsplit('_enriched', 1)[0]
            output_file = f"{base_name}_with_work_emails.csv"
        
        # Save the filtered dataframe
        df_filtered.to_csv(output_file, index=False)
        
        print(f"\nFiltered file saved to: {output_file}")
        print(f"Statistics:")
        print(f"  Initial rows: {initial_count}")
        print(f"  Rows with work emails: {filtered_count}")
        print(f"  Rows removed (no work email): {removed_count}")
        print(f"  Retention rate: {filtered_count/initial_count*100:.1f}%")
        
        return df_filtered
        
    except Exception as e:
        print(f"Error: {e}")
        return None

def main():
    """Main function to filter CSV files."""
    # Check if filenames were provided as command line arguments
    if len(sys.argv) > 1:
        # Process each file provided
        for input_file in sys.argv[1:]:
            print(f"\n{'='*60}")
            print(f"Processing: {input_file}")
            print('='*60)
            filter_csv_with_work_emails(input_file)
    else:
        # Default: process P0 and P05 enriched files
        files_to_process = ['P0_enriched.csv', 'P05_enriched.csv']
        
        for input_file in files_to_process:
            print(f"\n{'='*60}")
            print(f"Processing: {input_file}")
            print('='*60)
            filter_csv_with_work_emails(input_file)

if __name__ == "__main__":
    main()