import json
import pandas as pd
import os
import shutil
import random
from collections import Counter
import math

def create_diverse_image_batches():
    # Paths (from your screenshots)
    COMBINED_LISTINGS_PATH = "D:\\VR-Project\\combined_listings.json"
    IMAGES_CSV_PATH = "D:\\VR-Project\\abo-images-small\\images\\metadata\\images.csv"
    IMAGES_BASE_DIR = "D:\\VR-Project\\abo-images-small\\images\\small"
    
    # Output directories
    OUTPUT_BASE_DIR = "D:\\VR-Project\\dataset-batches"
    BATCH_DIRS = [
        os.path.join(OUTPUT_BASE_DIR, "batch1"),
        os.path.join(OUTPUT_BASE_DIR, "batch2"),
        os.path.join(OUTPUT_BASE_DIR, "batch3"),
        os.path.join(OUTPUT_BASE_DIR, "batch4")
    ]
    
    # Create output directories
    os.makedirs(OUTPUT_BASE_DIR, exist_ok=True)
    for batch_dir in BATCH_DIRS:
        os.makedirs(batch_dir, exist_ok=True)
        print(f"Created directory: {batch_dir}")
    
    # Load images.csv for image paths
    print("Loading images.csv...")
    images_df = pd.read_csv(IMAGES_CSV_PATH)
    image_path_map = dict(zip(images_df['image_id'], images_df['path']))
    print(f"Loaded {len(image_path_map)} image paths")
    
    # Load combined listings
    print("Loading combined listings...")
    products = []
    with open(COMBINED_LISTINGS_PATH, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                product = json.loads(line.strip())
                # Only include products with main_image_id
                if 'main_image_id' in product and product['main_image_id'] and product['main_image_id'] in image_path_map:
                    products.append(product)
            except json.JSONDecodeError:
                continue
    
    print(f"Loaded {len(products)} valid products with images")
    
    # Get product type distribution
    product_types = []
    product_by_type = {}
    
    for product in products:
        product_type = "Unknown"
        if 'product_type' in product and product['product_type']:
            product_type = product['product_type'][0]['value']
        
        product_types.append(product_type)
        
        if product_type not in product_by_type:
            product_by_type[product_type] = []
        
        product_by_type[product_type].append(product)
    
    type_counts = Counter(product_types)
    print(f"Found {len(type_counts)} unique product types")
    
    # Calculate how many items to sample from each product type
    total_samples = 20000
    total_products = len(products)
    
    # Strategy for sampling:
    # 1. For very common types: Cap at max_per_type
    # 2. For medium types: Take proportional samples
    # 3. For rare types: Take all available up to min_per_type
    
    selected_products = []
    max_per_type = 200  # Cap for common types
    min_per_type = 5    # Minimum for rare types
    
    # Sort product types by frequency
    sorted_types = sorted(product_by_type.keys(), key=lambda x: len(product_by_type[x]), reverse=True)
    
    # First, take minimum samples from all types to ensure diversity
    for product_type in sorted_types:
        products_of_type = product_by_type[product_type]
        take_count = min(min_per_type, len(products_of_type))
        
        if take_count > 0:
            selected_products.extend(random.sample(products_of_type, take_count))
            # Remove the selected products
            for p in selected_products[-take_count:]:
                if p in products_of_type:
                    products_of_type.remove(p)
    
    print(f"Selected {len(selected_products)} products as minimum samples")
    
    # Calculate remaining samples needed
    remaining_samples = total_samples - len(selected_products)
    
    # Calculate proportional allocation for remaining samples
    total_remaining_products = sum(len(product_by_type[t]) for t in sorted_types)
    
    # Take proportional samples from each type, respecting max_per_type
    for product_type in sorted_types:
        if not product_by_type[product_type]:  # Skip if no products left
            continue
            
        # Calculate proportional allocation
        proportion = len(product_by_type[product_type]) / total_remaining_products
        allocation = int(remaining_samples * proportion)
        
        # Cap at max_per_type (including already taken minimum samples)
        already_taken = min_per_type
        if already_taken > len(product_by_type[product_type]) + min_per_type:
            already_taken = len(product_by_type[product_type]) + min_per_type
            
        max_additional = max_per_type - already_taken
        if max_additional < 0:
            max_additional = 0
        
        take_count = min(allocation, max_additional, len(product_by_type[product_type]))
        
        if take_count > 0:
            selected_products.extend(random.sample(product_by_type[product_type], take_count))
    
    print(f"Selected {len(selected_products)} products total after proportional allocation")
    
    # If we haven't reached our target, take more from types that still have products
    if len(selected_products) < total_samples:
        remaining_needed = total_samples - len(selected_products)
        print(f"Need {remaining_needed} more products to reach target")
        
        all_remaining = []
        for product_type in sorted_types:
            all_remaining.extend(product_by_type[product_type])
        
        if len(all_remaining) >= remaining_needed:
            additional_samples = random.sample(all_remaining, remaining_needed)
            selected_products.extend(additional_samples)
    
    # Shuffle and trim to exactly 20,000
    random.shuffle(selected_products)
    selected_products = selected_products[:total_samples]
    
    print(f"Final selection: {len(selected_products)} products")
    
    # Verify diversity
    final_types = []
    for product in selected_products:
        product_type = "Unknown"
        if 'product_type' in product and product['product_type']:
            product_type = product['product_type'][0]['value']
        final_types.append(product_type)
    
    final_type_counts = Counter(final_types)
    print(f"Selected products span {len(final_type_counts)} product types")
    print("Top 10 product types in selection:")
    for product_type, count in final_type_counts.most_common(10):
        print(f"  {product_type}: {count}")
    
    # Split into 4 balanced batches
    batch_size = total_samples // 4
    batches = [[] for _ in range(4)]
    
    # Distribute products to ensure type diversity in each batch
    product_types_list = list(final_type_counts.keys())
    
    for product_type in product_types_list:
        # Get all products of this type
        type_products = [p for p in selected_products if 
                         'product_type' in p and 
                         p['product_type'] and 
                         p['product_type'][0]['value'] == product_type]
        
        # Distribute evenly across batches
        products_per_batch = len(type_products) // 4
        remainder = len(type_products) % 4
        
        start_idx = 0
        for i in range(4):
            take_count = products_per_batch + (1 if i < remainder else 0)
            end_idx = start_idx + take_count
            batches[i].extend(type_products[start_idx:end_idx])
            start_idx = end_idx
    
    # Verify batch sizes and adjust if needed
    for i in range(4):
        print(f"Batch {i+1} size: {len(batches[i])}")
    
    # Copy images to batch directories
    for batch_idx, batch in enumerate(batches):
        batch_dir = BATCH_DIRS[batch_idx]
        print(f"Copying images to {batch_dir}...")
        
        for product in batch:
            image_id = product['main_image_id']
            
            # Get image path
            if image_id in image_path_map:
                src_path = os.path.join(IMAGES_BASE_DIR, image_path_map[image_id])
                
                # Extract file extension
                _, ext = os.path.splitext(src_path)
                if not ext:  # If no extension, default to .jpg
                    ext = ".jpg"
                
                # Create destination path
                dst_path = os.path.join(batch_dir, f"{image_id}{ext}")
                
                # Copy the file
                try:
                    shutil.copy2(src_path, dst_path)
                except Exception as e:
                    print(f"Error copying {image_id}: {str(e)}")
        
        # Count files in batch directory
        file_count = len(os.listdir(batch_dir))
        print(f"Batch {batch_idx+1} contains {file_count} images")
    
    print("Done creating diverse image batches!")

if __name__ == "__main__":
    create_diverse_image_batches()