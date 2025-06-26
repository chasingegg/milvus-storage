#!/usr/bin/env python3
"""
S3 Test Data Setup Script
Generates and uploads test objects of various sizes for AWS S3 async performance testing.
"""

import boto3
import os
import sys
import time
import argparse
from botocore.exceptions import ClientError, NoCredentialsError
from typing import List, Tuple

class S3TestDataSetup:
    def __init__(self, bucket_name: str, region: str = 'us-west-2'):
        """Initialize S3 client and configuration"""
        self.bucket_name = bucket_name
        self.region = region
        
        try:
            self.s3_client = boto3.client('s3', region_name=region)
            print(f"✓ AWS S3 client initialized for region: {region}")
        except NoCredentialsError:
            print("❌ AWS credentials not found. Please configure your credentials.")
            print("   You can use: aws configure, environment variables, or IAM roles")
            sys.exit(1)
        except Exception as e:
            print(f"❌ Failed to initialize S3 client: {e}")
            sys.exit(1)
    
    def create_bucket_if_not_exists(self) -> bool:
        """Create S3 bucket if it doesn't exist"""
        try:
            # Check if bucket exists
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            print(f"✓ Bucket '{self.bucket_name}' already exists")
            return True
            
        except ClientError as e:
            error_code = int(e.response['Error']['Code'])
            
            if error_code == 404:
                # Bucket doesn't exist, create it
                try:
                    if self.region == 'us-east-1':
                        # us-east-1 doesn't need LocationConstraint
                        self.s3_client.create_bucket(Bucket=self.bucket_name)
                    else:
                        self.s3_client.create_bucket(
                            Bucket=self.bucket_name,
                            CreateBucketConfiguration={'LocationConstraint': self.region}
                        )
                    
                    print(f"✓ Created bucket '{self.bucket_name}' in region '{self.region}'")
                    
                    # Wait a moment for bucket to be ready
                    time.sleep(2)
                    return True
                    
                except ClientError as create_error:
                    print(f"❌ Failed to create bucket: {create_error}")
                    return False
            else:
                print(f"❌ Error accessing bucket: {e}")
                return False
    
    def generate_test_file(self, size_mb: int, filename: str) -> str:
        """Generate a test file with random data"""
        print(f"📝 Generating {size_mb}MB test file: {filename}")
        
        # Create file with pattern data (more realistic than purely random)
        chunk_size = 1024 * 1024  # 1MB chunks
        pattern = b'AWS-SDK-CPP-PERFORMANCE-TEST-DATA-' * 32  # ~1KB pattern
        
        try:
            with open(filename, 'wb') as f:
                for i in range(size_mb):
                    # Write 1MB of patterned data with some variation
                    chunk_data = (pattern * (chunk_size // len(pattern) + 1))[:chunk_size]
                    # Add some variation to make it more realistic
                    chunk_data = chunk_data.replace(b'TEST', f'T{i:03d}'.encode())
                    f.write(chunk_data)
            
            file_size = os.path.getsize(filename)
            print(f"✓ Generated {filename} ({file_size / (1024*1024):.1f} MB)")
            return filename
            
        except Exception as e:
            print(f"❌ Failed to generate {filename}: {e}")
            return None
    
    def upload_file_to_s3(self, local_file: str, s3_key: str) -> bool:
        """Upload file to S3 with progress tracking"""
        try:
            file_size = os.path.getsize(local_file)
            print(f"📤 Uploading {local_file} -> s3://{self.bucket_name}/{s3_key} ({file_size / (1024*1024):.1f} MB)")
            
            # Use multipart upload for larger files
            if file_size > 100 * 1024 * 1024:  # 100MB
                print("   Using multipart upload for large file...")
            
            start_time = time.time()
            
            # Upload with progress callback
            def progress_callback(bytes_transferred):
                percentage = (bytes_transferred / file_size) * 100
                speed_mbps = bytes_transferred / (1024 * 1024) / max(time.time() - start_time, 0.1)
                print(f"   Progress: {percentage:.1f}% ({speed_mbps:.1f} MB/s)", end='\r')
            
            self.s3_client.upload_file(
                local_file, 
                self.bucket_name, 
                s3_key,
                Callback=progress_callback
            )
            
            end_time = time.time()
            upload_time = end_time - start_time
            speed_mbps = (file_size / (1024 * 1024)) / max(upload_time, 0.1)
            
            print(f"\n✓ Upload completed in {upload_time:.1f}s (avg {speed_mbps:.1f} MB/s)")
            return True
            
        except Exception as e:
            print(f"\n❌ Failed to upload {local_file}: {e}")
            return False
    
    def verify_upload(self, s3_key: str, expected_size: int) -> bool:
        """Verify uploaded object exists and has correct size"""
        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            actual_size = response['ContentLength']
            
            if actual_size == expected_size:
                print(f"✓ Verified {s3_key} ({actual_size} bytes)")
                return True
            else:
                print(f"❌ Size mismatch for {s3_key}: expected {expected_size}, got {actual_size}")
                return False
                
        except Exception as e:
            print(f"❌ Failed to verify {s3_key}: {e}")
            return False
    
    def cleanup_local_files(self, files: List[str]):
        """Clean up local test files"""
        print("\n🧹 Cleaning up local files...")
        for filename in files:
            try:
                if os.path.exists(filename):
                    os.remove(filename)
                    print(f"✓ Removed {filename}")
            except Exception as e:
                print(f"❌ Failed to remove {filename}: {e}")
    
    def setup_test_data(self, test_configs: List[Tuple[str, int, str]]) -> bool:
        """Main method to setup all test data"""
        print(f"\n🚀 Setting up S3 test data in bucket: {self.bucket_name}")
        print("=" * 60)
        
        # Create bucket if needed
        if not self.create_bucket_if_not_exists():
            return False
        
        local_files = []
        success_count = 0
        
        try:
            for s3_key, size_mb, description in test_configs:
                print(f"\n📋 Setting up {description}")
                print("-" * 40)
                
                # Generate local file
                local_filename = f"temp_{s3_key}"
                if not self.generate_test_file(size_mb, local_filename):
                    continue
                
                local_files.append(local_filename)
                expected_size = size_mb * 1024 * 1024
                
                # Upload to S3
                if self.upload_file_to_s3(local_filename, s3_key):
                    # Verify upload
                    if self.verify_upload(s3_key, expected_size):
                        success_count += 1
        
        finally:
            print("hello")
            # Always cleanup local files
            self.cleanup_local_files(local_files)
        
        print(f"\n📊 Setup Summary:")
        print("=" * 30)
        print(f"Total test files: {len(test_configs)}")
        print(f"Successfully uploaded: {success_count}")
        print(f"Failed: {len(test_configs) - success_count}")
        
        if success_count == len(test_configs):
            print("✅ All test data uploaded successfully!")
            print(f"\n🎯 Ready to run performance tests with bucket: {self.bucket_name}")
            return True
        else:
            print("❌ Some uploads failed. Check the errors above.")
            return False

def main():
    parser = argparse.ArgumentParser(
        description="Upload test data to S3 for AWS SDK C++ async performance testing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python setup_test_data.py --bucket my-perf-test-bucket
  python setup_test_data.py --bucket my-test-bucket --region eu-west-1
  python setup_test_data.py --bucket my-test-bucket --custom-sizes 5,25,500
        """
    )
    
    parser.add_argument(
        '--bucket', '-b',
        required=True,
        help='S3 bucket name for test data'
    )
    
    parser.add_argument(
        '--region', '-r',
        default='us-west-2',
        help='AWS region (default: us-west-2)'
    )
    
    parser.add_argument(
        '--custom-sizes',
        help='Custom file sizes in MB, comma-separated (e.g., "1,10,100")'
    )

    parser.add_argument(
        '--count',
        default=10,
        type=int,
        help="how many files"
    )
    
    parser.add_argument(
        '--skip-cleanup',
        action='store_true',
        help='Keep local test files after upload'
    )
    
    args = parser.parse_args()
    
    # Define test configurations (s3_key, size_mb, description)
    if args.custom_sizes:
        try:
            sizes = [int(x.strip()) for x in args.custom_sizes.split(',')]
            test_configs = []
            size_names = {1: 'small', 10: 'medium', 100: 'large'}
            
            for i, size in enumerate(sizes):
                if size <= 1:
                    name = "small"
                elif size > 1 and size <= 10:
                    name = "medium"
                else:
                    name = "large"
                # name = size_names.get(size, f'custom{i+1}')
                for j in range(args.count):
                    test_configs.append((
                        f"test-{name}-object.dat_{j}",
                        size,
                        f"{name.title()} Object ({size}MB)"
                    ))
        except ValueError:
            print("❌ Invalid custom sizes format. Use comma-separated integers (e.g., '1,10,100')")
            sys.exit(1)
    else:
        # Default test configurations matching the C++ performance test
        test_configs = [
            ("test-small-object.dat", 1, "Small Object (1MB)"),
            ("test-medium-object.dat", 10, "Medium Object (10MB)"),
            ("test-large-object.dat", 100, "Large Object (100MB)")
        ]
    
    # Initialize and run setup
    setup = S3TestDataSetup(args.bucket, args.region)
    success = setup.setup_test_data(test_configs)
    
    if success:
        print(f"\n🔧 Next steps:")
        print(f"1. Update your C++ performance test to use bucket: '{args.bucket}'")
        print(f"2. Compile and run: ./async_read_performance_test")
        print(f"3. Analyze results with: python analyze_results.py")
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main() 