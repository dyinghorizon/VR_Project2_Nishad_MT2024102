import os
import glob
import json

def combine_listing_files(metadata_dir, output_file):
    """
    Combine all listings_* files into a single file
    
    Args:
        metadata_dir: Directory containing listing files
        output_file: Path to output combined file
    
    Returns:
        int: Number of products processed
    """
    # Find all listing files (note: they don't have .json extension in the screenshot)
    pattern = os.path.join(metadata_dir, "listings_*")
    listing_files = glob.glob(pattern)
    print(f"Found {len(listing_files)} listing files")
    
    # Process each file
    processed = 0
    with open(output_file, 'w', encoding='utf-8') as outfile:
        for i, file_path in enumerate(listing_files):
            print(f"Processing file {i+1}/{len(listing_files)}: {os.path.basename(file_path)}")
            file_count = 0
            
            try:
                with open(file_path, 'r', encoding='utf-8') as infile:
                    for line in infile:
                        try:
                            # Verify it's valid JSON before writing
                            json.loads(line.strip())
                            outfile.write(line)
                            processed += 1
                            file_count += 1
                        except json.JSONDecodeError:
                            print(f"Skipping invalid JSON line in {file_path}")
                
                print(f"  Added {file_count} products from {os.path.basename(file_path)}")
            except Exception as e:
                print(f"Error processing {file_path}: {e}")
    
    print(f"Successfully combined {processed} products into {output_file}")
    return processed

if __name__ == "__main__":
    # Updated path from the screenshot
    metadata_dir = "D:\\VR-Project\\abo-listings\\listings\\metadata\\listings"
    
    # Output file
    output_file = "combined_listings.json"
    
    # Combine listings
    combine_listing_files(metadata_dir, output_file)