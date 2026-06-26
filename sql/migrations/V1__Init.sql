CREATE TABLE public.orders (
    id SERIAL PRIMARY KEY,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    customer_name VARCHAR(100),
    product_name VARCHAR(100),
    price NUMERIC(10, 2),
    quantity INT,
    status VARCHAR(20) DEFAULT 'PENDING'
);

-- Bật tính năng REPLICA IDENTITY FULL để Debezium bắt được cả giá trị CŨ và MỚI khi UPDATE
ALTER TABLE public.orders REPLICA IDENTITY FULL;