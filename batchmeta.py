import json
import pandas as pd
import os
import glob

def create_batch_metadata_files():
    # Paths
    COMBINED_LISTINGS_PATH = "D:\\VR-Project\\combined_listings.json"
    IMAGES_CSV_PATH = "D:\\VR-Project\\abo-images-small\\images\\metadata\\images.csv"
    BATCHES_BASE_DIR = "D:\\VR-Project\\dataset-batches"
    
    # Batch directories
    batch_dirs = [
        os.path.join(BATCHES_BASE_DIR, "batch1"),
        os.path.join(BATCHES_BASE_DIR, "batch2"),
        os.path.join(BATCHES_BASE_DIR, "batch3"),
        os.path.join(BATCHES_BASE_DIR, "batch4")
    ]
    
    # Load images.csv for paths
    print("Loading images.csv...")
    images_df = pd.read_csv(IMAGES_CSV_PATH)
    image_path_map = dict(zip(images_df['image_id'], images_df['path']))
    print(f"Loaded {len(image_path_map)} image paths")
    
    # Create product lookup by image_id
    print("Loading and indexing combined listings...")
    product_by_image = {}
    with open(COMBINED_LISTINGS_PATH, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                product = json.loads(line.strip())
                if 'main_image_id' in product and product['main_image_id']:
                    product_by_image[product['main_image_id']] = product
            except json.JSONDecodeError:
                continue
    
    print(f"Indexed {len(product_by_image)} products by image_id")
    
    # Process each batch
    for batch_idx, batch_dir in enumerate(batch_dirs, 1):
        print(f"Processing batch {batch_idx}...")
        
        # Get all image files in this batch
        image_files = glob.glob(os.path.join(batch_dir, "*.*"))
        
        batch_metadata = []
        for image_file in image_files:
            # Extract image_id from filename
            image_id = os.path.splitext(os.path.basename(image_file))[0]
            
            # Find product for this image
            if image_id in product_by_image:
                product = product_by_image[image_id]
                
                # Extract only essential fields for QA generation
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
                metadata_entry['local_path'] = os.path.basename(image_file)
                
                batch_metadata.append(metadata_entry)
            else:
                print(f"Warning: No product data found for image {image_id}")
        
        # Save metadata to JSON file
        output_path = os.path.join(BATCHES_BASE_DIR, f"batch{batch_idx}_metadata.json")
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(batch_metadata, f, indent=2, ensure_ascii=False)
        
        print(f"Created metadata file for batch {batch_idx} with {len(batch_metadata)} entries at {output_path}")

if __name__ == "__main__":
    create_batch_metadata_files()