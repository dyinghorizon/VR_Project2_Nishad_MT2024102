import json
import pandas as pd
import os
import shutil
import random
from collections import Counter

def create_new_batches():
    # Paths
    COMBINED_LISTINGS_PATH = "D:\\VR-Project\\combined_listings.json"
    IMAGES_CSV_PATH = "D:\\VR-Project\\abo-images-small\\images\\metadata\\images.csv"
    IMAGES_BASE_DIR = "D:\\VR-Project\\abo-images-small\\images\\small"
    
    # Base directory for all batches
    OUTPUT_BASE_DIR = "D:\\VR-Project\\dataset-batches"
    
    # Existing batch metadata files
    EXISTING_METADATA_FILES = [
        os.path.join(OUTPUT_BASE_DIR, "batch1_metadata.json"),
        os.path.join(OUTPUT_BASE_DIR, "batch2_metadata.json"),
        os.path.join(OUTPUT_BASE_DIR, "batch3_metadata.json"),
        os.path.join(OUTPUT_BASE_DIR, "batch4_metadata.json"),
    ]
    
    # New batch directories
    NEW_BATCH_DIRS = [
        os.path.join(OUTPUT_BASE_DIR, "batch5"),
        os.path.join(OUTPUT_BASE_DIR, "batch6"),
        os.path.join(OUTPUT_BASE_DIR, "batch7"),
        os.path.join(OUTPUT_BASE_DIR, "batch8")
    ]
    
    # Create output directories for new batches
    for batch_dir in NEW_BATCH_DIRS:
        os.makedirs(batch_dir, exist_ok=True)
        print(f"Created directory: {batch_dir}")
    
    # Load images.csv for image paths
    print("Loading images.csv...")
    images_df = pd.read_csv(IMAGES_CSV_PATH)
    image_path_map = dict(zip(images_df['image_id'], images_df['path']))
    print(f"Loaded {len(image_path_map)} image paths")
    
    # Collect all image IDs that are already in existing batches
    print("Collecting existing image IDs from batches 1-4...")
    existing_image_ids = set()
    for metadata_file in EXISTING_METADATA_FILES:
        if os.path.exists(metadata_file):
            with open(metadata_file, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
                for item in metadata:
                    if 'image_id' in item:
                        existing_image_ids.add(item['image_id'])
    
    print(f"Found {len(existing_image_ids)} existing image IDs in batches 1-4")
    
    # Load combined listings
    print("Loading combined listings...")
    products = []
    with open(COMBINED_LISTINGS_PATH, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                product = json.loads(line.strip())
                # Only include products with main_image_id that aren't already in existing batches
                if ('main_image_id' in product and 
                    product['main_image_id'] and 
                    product['main_image_id'] in image_path_map and
                    product['main_image_id'] not in existing_image_ids):
                    products.append(product)
            except json.JSONDecodeError:
                continue
    
    print(f"Loaded {len(products)} valid products with images (excluding ones in existing batches)")
    
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
    print(f"Found {len(type_counts)} unique product types in remaining products")
    
    # Calculate how many items to sample from each product type
    total_samples = 20000  # Same as original batches
    total_products = len(products)
    
    # Strategy for sampling similar to original script
    selected_products = []
    max_per_type = 200  # Cap for common types
    min_per_type = 5    # Minimum for rare types
    
    # Sort product types by frequency
    sorted_types = sorted(product_by_type.keys(), key=lambda x: len(product_by_type[x]), reverse=True)
    
    # First, take minimum samples from all types to ensure diversity
    for product_type in sorted_types:
        products_of_type = product_by_type[product_type].copy()  # Create a copy to modify
        take_count = min(min_per_type, len(products_of_type))
        
        if take_count > 0:
            sampled = random.sample(products_of_type, take_count)
            selected_products.extend(sampled)
            
            # Remove the selected products from product_by_type
            for p in sampled:
                product_by_type[product_type].remove(p)
    
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
        proportion = len(product_by_type[product_type]) / total_remaining_products if total_remaining_products > 0 else 0
        allocation = int(remaining_samples * proportion)
        
        # Cap at max_per_type (including already taken minimum samples)
        already_taken = sum(1 for p in selected_products 
                           if 'product_type' in p and p['product_type'] 
                           and p['product_type'][0]['value'] == product_type)
        
        max_additional = max_per_type - already_taken
        if max_additional < 0:
            max_additional = 0
        
        take_count = min(allocation, max_additional, len(product_by_type[product_type]))
        
        if take_count > 0:
            sampled = random.sample(product_by_type[product_type], take_count)
            selected_products.extend(sampled)
            
            # Remove the selected products
            for p in sampled:
                product_by_type[product_type].remove(p)
    
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
        else:
            # If we don't have enough remaining products, take all we have
            selected_products.extend(all_remaining)
    
    # Shuffle the selected products
    random.shuffle(selected_products)
    selected_products = selected_products[:min(total_samples, len(selected_products))]
    
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
    
    # Verify batch sizes
    for i in range(4):
        print(f"Batch {i+5} size: {len(batches[i])}")
    
    # Copy images to batch directories and create metadata
    for batch_idx, batch in enumerate(batches):
        batch_dir = NEW_BATCH_DIRS[batch_idx]
        batch_num = batch_idx + 5  # Batch numbers 5-8
        
        print(f"Copying images to {batch_dir}...")
        
        # Prepare metadata for this batch
        batch_metadata = []
        
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
                    
                    # Create metadata entry
                    metadata_entry = {
                        'item_id': product.get('item_id', ''),
                        'image_id': image_id,
                        'image_path': image_path_map.get(image_id, '')
                    }
                    
                    # Add brand info
                    if 'brand' in product and product['brand']:
                        brand_entry = product['brand'][0]
                        metadata_entry['brand'] = brand_entry.get('value', '')
                        if 'language_tag' in brand_entry:
                            metadata_entry['brand_language'] = brand_entry['language_tag']
                    
                    # Add item name
                    if 'item_name' in product and product['item_name']:
                        name_entry = product['item_name'][0]
                        metadata_entry['item_name'] = name_entry.get('value', '')
                        if 'language_tag' in name_entry:
                            metadata_entry['name_language'] = name_entry['language_tag']
                    
                    # Add color
                    if 'color' in product and product['color']:
                        color_entry = product['color'][0]
                        metadata_entry['color'] = color_entry.get('value', '')
                    
                    # Add product type
                    if 'product_type' in product and product['product_type']:
                        metadata_entry['product_type'] = product['product_type'][0].get('value', '')
                    
                    # Add bullet points (features)
                    if 'bullet_point' in product and product['bullet_point']:
                        bullet_points = []
                        for bp in product['bullet_point']:
                            if 'value' in bp:
                                bullet_points.append(bp['value'])
                        metadata_entry['features'] = bullet_points
                    
                    # Add style
                    if 'style' in product and product['style']:
                        metadata_entry['style'] = product['style'][0].get('value', '')
                    
                    # Add material
                    if 'material' in product and product['material']:
                        metadata_entry['material'] = product['material'][0].get('value', '')
                    
                    # Add weight
                    if 'item_weight' in product and product['item_weight']:
                        weight_entry = product['item_weight'][0]
                        if 'value' in weight_entry:
                            metadata_entry['weight'] = weight_entry['value']
                            if 'unit' in weight_entry:
                                metadata_entry['weight_unit'] = weight_entry['unit']
                    
                    # Add dimensions
                    if 'item_dimensions' in product and product['item_dimensions']:
                        dims = {}
                        for dim_type in ['height', 'width', 'length']:
                            if dim_type in product['item_dimensions']:
                                dim_entry = product['item_dimensions'][dim_type]
                                if 'value' in dim_entry:
                                    dims[dim_type] = {
                                        'value': dim_entry['value'],
                                        'unit': dim_entry.get('unit', '')
                                    }
                        if dims:
                            metadata_entry['dimensions'] = dims
                    
                    # Add local file path relative to batch directory
                    metadata_entry['local_path'] = f"{image_id}{ext}"
                    
                    batch_metadata.append(metadata_entry)
                    
                except Exception as e:
                    print(f"Error copying {image_id}: {str(e)}")
        
        # Save metadata to JSON file
        output_path = os.path.join(OUTPUT_BASE_DIR, f"batch{batch_num}_metadata.json")
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(batch_metadata, f, indent=2, ensure_ascii=False)
        
        # Count files in batch directory
        file_count = len(os.listdir(batch_dir))
        print(f"Batch {batch_num} contains {file_count} images")
        print(f"Created metadata file for batch {batch_num} with {len(batch_metadata)} entries")
    
    print("Done creating new batches 5-8!")

if __name__ == "__main__":
    create_new_batches()