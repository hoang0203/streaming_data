-- Tạo Database để chứa các bảng phân tích
CREATE DATABASE IF NOT EXISTS mock_sales;
USE mock_sales;

-- Tạo bảng "nhìn" thẳng vào MinIO
CREATE TABLE IF NOT EXISTS orders
(
    id Int32,
    order_date DateTime('UTC'),
    customer_name String,
    product_name String,
    price Float64,              
    quantity Int32,
    status String
)
ENGINE = DeltaLake(
    'http://minio:9000/lakehouse/orders/', 
    'minioadmin', 
    'minioadminpassword'
);