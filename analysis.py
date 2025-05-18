import json
import pandas as pd
import matplotlib.pyplot as plt
from collections import Counter
import os

def analyze_listings(listings_file):
    """
    Analyze combined listings file and extract key statistics
    
    Args:
        listings_file: Path to combined listings JSON file
    """
    print(f"Analyzing listings file: {listings_file}")
    
    # Load and parse all products
    products = []
    with open(listings_file, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                product = json.loads(line.strip())
                products.append(product)
            except json.JSONDecodeError:
                continue
    
    total_products = len(products)
    print(f"\nTotal products: {total_products}")
    
    # 1. Product Type Distribution
    product_types = []
    for product in products:
        if 'product_type' in product and product['product_type']:
            product_types.append(product['product_type'][0]['value'])
    
    type_counts = Counter(product_types)
    print(f"\nProduct Type Distribution (Top 10):")
    for product_type, count in type_counts.most_common(10):
        print(f"  {product_type}: {count} ({count/total_products:.1%})")
    
    # 2. Brand Analysis
    brands = []
    for product in products:
        if 'brand' in product and product['brand']:
            brand_value = product['brand'][0].get('value', 'Unknown')
            brands.append(brand_value)
    
    unique_brands = len(set(brands))
    top_brands = Counter(brands).most_common(10)
    
    print(f"\nBrand Analysis:")
    print(f"  Total unique brands: {unique_brands}")
    print(f"  Top 10 brands:")
    for brand, count in top_brands:
        print(f"    {brand}: {count} ({count/total_products:.1%})")
    
    # 3. Country/Marketplace Analysis
    countries = []
    marketplaces = []
    
    for product in products:
        if 'country' in product:
            countries.append(product['country'])
        if 'marketplace' in product:
            marketplaces.append(product['marketplace'])
    
    country_counts = Counter(countries)
    marketplace_counts = Counter(marketplaces)
    
    print(f"\nCountry Distribution:")
    for country, count in country_counts.most_common():
        print(f"  {country}: {count} ({count/total_products:.1%})")
    
    print(f"\nMarketplace Distribution:")
    for marketplace, count in marketplace_counts.most_common():
        print(f"  {marketplace}: {count} ({count/total_products:.1%})")
    
    # 4. Data Completeness
    key_fields = ['brand', 'item_name', 'color', 'product_type', 'main_image_id']
    field_presence = {field: 0 for field in key_fields}
    
    for product in products:
        for field in key_fields:
            if field in product and product[field]:
                field_presence[field] += 1
    
    print(f"\nData Completeness:")
    for field, count in field_presence.items():
        print(f"  {field}: {count} ({count/total_products:.1%})")
    
    # 5. Language Analysis
    languages = []
    for product in products:
        # Check brand language
        if 'brand' in product and product['brand']:
            for brand_entry in product['brand']:
                if 'language_tag' in brand_entry:
                    languages.append(brand_entry['language_tag'])
        
        # Check item_name language
        if 'item_name' in product and product['item_name']:
            for name_entry in product['item_name']:
                if 'language_tag' in name_entry:
                    languages.append(name_entry['language_tag'])
    
    language_counts = Counter(languages)
    print(f"\nLanguage Distribution (Top 10):")
    for language, count in language_counts.most_common(10):
        print(f"  {language}: {count}")
    
    # 6. Image Analysis
    has_main_image = sum(1 for p in products if 'main_image_id' in p and p['main_image_id'])
    has_other_images = sum(1 for p in products if 'other_image_id' in p and p['other_image_id'])
    
    images_per_product = []
    for product in products:
        count = 0
        if 'main_image_id' in product and product['main_image_id']:
            count += 1
        if 'other_image_id' in product and product['other_image_id']:
            count += len(product['other_image_id'])
        images_per_product.append(count)
    
    avg_images = sum(images_per_product) / total_products
    
    print(f"\nImage Analysis:")
    print(f"  Products with main image: {has_main_image} ({has_main_image/total_products:.1%})")
    print(f"  Products with additional images: {has_other_images} ({has_other_images/total_products:.1%})")
    print(f"  Average images per product: {avg_images:.2f}")
    
    # 7. Create a simple visualization
    # Create output directory if it doesn't exist
    os.makedirs('analysis_results', exist_ok=True)
    
    # Save product type distribution to CSV
    type_df = pd.DataFrame(type_counts.most_common(), columns=['Product Type', 'Count'])
    type_df.to_csv('analysis_results/product_types.csv', index=False)
    
    # Plot product type distribution (top 10)
    plt.figure(figsize=(12, 6))
    top_types = pd.DataFrame(type_counts.most_common(10))
    plt.bar(top_types[0], top_types[1])
    plt.title('Top 10 Product Types')
    plt.xlabel('Product Type')
    plt.ylabel('Count')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig('analysis_results/product_types.png')
    
    # Plot brand distribution (top 10)
    plt.figure(figsize=(12, 6))
    top_brand_df = pd.DataFrame(top_brands, columns=['Brand', 'Count'])
    plt.bar(top_brand_df['Brand'], top_brand_df['Count'])
    plt.title('Top 10 Brands')
    plt.xlabel('Brand')
    plt.ylabel('Count')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig('analysis_results/brands.png')
    
    # Plot country distribution
    plt.figure(figsize=(12, 6))
    country_df = pd.DataFrame(country_counts.most_common(), columns=['Country', 'Count'])
    plt.bar(country_df['Country'], country_df['Count'])
    plt.title('Country Distribution')
    plt.xlabel('Country')
    plt.ylabel('Count')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig('analysis_results/countries.png')
    
    print(f"\nAnalysis results saved to 'analysis_results' directory")

if __name__ == "__main__":
    # Path to the combined listings file
    listings_file = "combined_listings.json"
    
    # Analyze the listings
    analyze_listings(listings_file)