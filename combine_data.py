import csv
import boto3

s3 = boto3.client('s3')
bucket_name = 'newanjibucket'
output_file = 'clean_combined_spotify_data.csv'

with open(output_file, mode='w', newline='', encoding='utf-8') as out_file:
    writer = csv.writer(out_file)
    writer.writerow(['Source', 'Index', 'ID', 'Name', 'URL'])

    # Albums
    with open('albums.csv', newline='', encoding='utf-8') as albums:
        reader = csv.DictReader(albums)
        for row in reader:
            if row.get('URL', '').strip() and row.get('Index', '').strip():
                writer.writerow([
                    'Album',
                    row['Index'],
                    row['Album_ID'],
                    row['Album_Name'],
                    row['URL']
                ])

    # Artists
    with open('artists.csv', newline='', encoding='utf-8') as artists:
        reader = csv.DictReader(artists)
        for row in reader:
            if row.get('URL', '').strip() and row.get('Album_ID', '').strip():
                writer.writerow([
                    'Artist',
                    row['Album_ID'],
                    row['Artist_ID'],
                    row['Name'],
                    row['URL']
                ])

    # Songs
    with open('songs.csv', newline='', encoding='utf-8') as songs:
        reader = csv.DictReader(songs)
        for row in reader:
            if row.get('URL', '').strip() and row.get('Album_ID', '').strip():
                writer.writerow([
                    'Song',
                    row['Album_ID'],
                    row['Track_ID'],
                    row['Title'],
                    row['URL']
                ])

# Upload to S3
with open(output_file, 'rb') as data:
    s3.upload_fileobj(data, bucket_name, f'transformed_data/{output_file}')
    print(f"✅ Uploaded clean file to s3://{bucket_name}/transformed_data/")

import boto3

# S3 setup
s3 = boto3.client('s3')
bucket_name = 'newanjibucket'  # Replace with your actual bucket name
file_name = 'final_clean_spotify_data.csv'
s3_key = f'transformed_data/{file_name}'  # Path inside S3

# Upload the file
with open(file_name, 'rb') as data:
    s3.upload_fileobj(data, bucket_name, s3_key)
    print(f"✅ Uploaded {file_name} to s3://{bucket_name}/{s3_key}")

