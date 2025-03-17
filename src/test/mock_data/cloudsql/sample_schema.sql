-- ===========================================================================
-- E-Commerce Sample Database Schema for Testing Cloud SQL Connector
-- 
-- This schema defines a typical e-commerce data model with customers, products,
-- orders, and inventory tracking to support testing of Cloud SQL extraction,
-- schema validation, and referential integrity checks.
-- ===========================================================================

BEGIN;

-- ---------------------------------------------------------------------------
-- Customers Table
-- Stores customer information including contact details and account metadata
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL UNIQUE,
    phone_number VARCHAR(20),
    address_line1 VARCHAR(100),
    address_line2 VARCHAR(100),
    city VARCHAR(50),
    state VARCHAR(50),
    postal_code VARCHAR(20),
    country VARCHAR(50),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'active'
);

-- Indexes for customers table
CREATE UNIQUE INDEX IF NOT EXISTS idx_customers_email ON customers(email);
CREATE INDEX IF NOT EXISTS idx_customers_created_at ON customers(created_at);

COMMENT ON TABLE customers IS 'Stores customer information including contact details and account metadata';
COMMENT ON COLUMN customers.customer_id IS 'Unique identifier for each customer';
COMMENT ON COLUMN customers.first_name IS 'Customer''s first name';
COMMENT ON COLUMN customers.last_name IS 'Customer''s last name';
COMMENT ON COLUMN customers.email IS 'Customer''s email address, used as unique identifier';
COMMENT ON COLUMN customers.phone_number IS 'Customer''s contact phone number';
COMMENT ON COLUMN customers.address_line1 IS 'First line of customer''s address';
COMMENT ON COLUMN customers.address_line2 IS 'Second line of customer''s address (optional)';
COMMENT ON COLUMN customers.city IS 'Customer''s city';
COMMENT ON COLUMN customers.state IS 'Customer''s state or province';
COMMENT ON COLUMN customers.postal_code IS 'Customer''s postal or zip code';
COMMENT ON COLUMN customers.country IS 'Customer''s country';
COMMENT ON COLUMN customers.created_at IS 'Timestamp when customer record was created';
COMMENT ON COLUMN customers.updated_at IS 'Timestamp when customer record was last updated';
COMMENT ON COLUMN customers.status IS 'Customer account status (active, inactive, suspended)';

-- ---------------------------------------------------------------------------
-- Products Table
-- Stores product catalog information including pricing and inventory data
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    description TEXT,
    category VARCHAR(50) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    cost DECIMAL(10,2),
    sku VARCHAR(50) UNIQUE,
    stock_quantity INTEGER DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

-- Indexes for products table
CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);
CREATE UNIQUE INDEX IF NOT EXISTS idx_products_sku ON products(sku);
CREATE INDEX IF NOT EXISTS idx_products_price ON products(price);

COMMENT ON TABLE products IS 'Stores product catalog information including pricing and inventory data';
COMMENT ON COLUMN products.product_id IS 'Unique identifier for each product';
COMMENT ON COLUMN products.product_name IS 'Name of the product';
COMMENT ON COLUMN products.description IS 'Detailed description of the product';
COMMENT ON COLUMN products.category IS 'Product category (electronics, clothing, etc.)';
COMMENT ON COLUMN products.price IS 'Current price of the product';
COMMENT ON COLUMN products.cost IS 'Cost to acquire or produce the product';
COMMENT ON COLUMN products.sku IS 'Stock keeping unit - unique product identifier';
COMMENT ON COLUMN products.stock_quantity IS 'Current quantity in stock';
COMMENT ON COLUMN products.created_at IS 'Timestamp when product was added to catalog';
COMMENT ON COLUMN products.updated_at IS 'Timestamp when product was last updated';
COMMENT ON COLUMN products.is_active IS 'Whether the product is active in the catalog';

-- ---------------------------------------------------------------------------
-- Orders Table
-- Stores order header information including customer, status, and totals
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    order_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    total_amount DECIMAL(12,2) NOT NULL,
    tax_amount DECIMAL(10,2) DEFAULT 0,
    shipping_amount DECIMAL(10,2) DEFAULT 0,
    shipping_address TEXT,
    billing_address TEXT,
    payment_method VARCHAR(50),
    notes TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE RESTRICT ON UPDATE CASCADE
);

-- Indexes for orders table
CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(order_date);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);

COMMENT ON TABLE orders IS 'Stores order header information including customer, status, and totals';
COMMENT ON COLUMN orders.order_id IS 'Unique identifier for each order';
COMMENT ON COLUMN orders.customer_id IS 'Reference to the customer who placed the order';
COMMENT ON COLUMN orders.order_date IS 'Date and time when the order was placed';
COMMENT ON COLUMN orders.status IS 'Order status (pending, processing, shipped, delivered, cancelled)';
COMMENT ON COLUMN orders.total_amount IS 'Total order amount including tax and shipping';
COMMENT ON COLUMN orders.tax_amount IS 'Tax amount applied to the order';
COMMENT ON COLUMN orders.shipping_amount IS 'Shipping cost for the order';
COMMENT ON COLUMN orders.shipping_address IS 'Shipping address for the order';
COMMENT ON COLUMN orders.billing_address IS 'Billing address for the order';
COMMENT ON COLUMN orders.payment_method IS 'Payment method used for the order';
COMMENT ON COLUMN orders.notes IS 'Additional notes or comments about the order';
COMMENT ON COLUMN orders.updated_at IS 'Timestamp when order was last updated';

-- ---------------------------------------------------------------------------
-- Order Items Table
-- Stores individual line items for each order
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS order_items (
    item_id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL DEFAULT 1,
    unit_price DECIMAL(10,2) NOT NULL,
    discount_amount DECIMAL(10,2) DEFAULT 0,
    total_price DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE RESTRICT ON UPDATE CASCADE
);

-- Indexes for order_items table
CREATE INDEX IF NOT EXISTS idx_order_items_order ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product ON order_items(product_id);

COMMENT ON TABLE order_items IS 'Stores individual line items for each order';
COMMENT ON COLUMN order_items.item_id IS 'Unique identifier for each order item';
COMMENT ON COLUMN order_items.order_id IS 'Reference to the parent order';
COMMENT ON COLUMN order_items.product_id IS 'Reference to the product ordered';
COMMENT ON COLUMN order_items.quantity IS 'Quantity of the product ordered';
COMMENT ON COLUMN order_items.unit_price IS 'Price per unit at time of order';
COMMENT ON COLUMN order_items.discount_amount IS 'Discount amount applied to this item';
COMMENT ON COLUMN order_items.total_price IS 'Total price for this line item (quantity * unit_price - discount)';
COMMENT ON COLUMN order_items.created_at IS 'Timestamp when item was added to order';

-- ---------------------------------------------------------------------------
-- Inventory Changes Table
-- Tracks changes to product inventory for testing incremental extraction and CDC patterns
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS inventory_changes (
    change_id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL,
    change_type VARCHAR(20) NOT NULL,
    quantity INTEGER NOT NULL,
    previous_quantity INTEGER NOT NULL,
    new_quantity INTEGER NOT NULL,
    changed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    reference_id INTEGER,
    notes TEXT,
    FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE ON UPDATE CASCADE
);

-- Indexes for inventory_changes table
CREATE INDEX IF NOT EXISTS idx_inventory_product ON inventory_changes(product_id);
CREATE INDEX IF NOT EXISTS idx_inventory_changed_at ON inventory_changes(changed_at);
CREATE INDEX IF NOT EXISTS idx_inventory_change_type ON inventory_changes(change_type);

COMMENT ON TABLE inventory_changes IS 'Tracks changes to product inventory for testing incremental extraction and CDC patterns';
COMMENT ON COLUMN inventory_changes.change_id IS 'Unique identifier for each inventory change record';
COMMENT ON COLUMN inventory_changes.product_id IS 'Reference to the product whose inventory changed';
COMMENT ON COLUMN inventory_changes.change_type IS 'Type of inventory change (restock, sale, adjustment, return, loss)';
COMMENT ON COLUMN inventory_changes.quantity IS 'Quantity changed (positive for additions, negative for reductions)';
COMMENT ON COLUMN inventory_changes.previous_quantity IS 'Inventory quantity before the change';
COMMENT ON COLUMN inventory_changes.new_quantity IS 'Inventory quantity after the change';
COMMENT ON COLUMN inventory_changes.changed_at IS 'Timestamp when the inventory change occurred';
COMMENT ON COLUMN inventory_changes.reference_id IS 'Optional reference to related record (e.g., order_id for sales)';
COMMENT ON COLUMN inventory_changes.notes IS 'Additional notes about the inventory change';

-- ---------------------------------------------------------------------------
-- Views
-- ---------------------------------------------------------------------------

-- Customer Order Summary
-- View that summarizes order activity by customer
CREATE OR REPLACE VIEW customer_order_summary AS
SELECT 
    c.customer_id, 
    c.first_name, 
    c.last_name, 
    c.email, 
    COUNT(o.order_id) AS order_count, 
    SUM(o.total_amount) AS total_spent, 
    MAX(o.order_date) AS last_order_date
FROM 
    customers c
LEFT JOIN 
    orders o ON c.customer_id = o.customer_id
GROUP BY 
    c.customer_id, c.first_name, c.last_name, c.email;

COMMENT ON VIEW customer_order_summary IS 'View that summarizes order activity by customer';

-- Product Sales Summary
-- View that summarizes sales by product
CREATE OR REPLACE VIEW product_sales_summary AS
SELECT 
    p.product_id, 
    p.product_name, 
    p.category, 
    SUM(oi.quantity) AS units_sold, 
    SUM(oi.total_price) AS total_revenue
FROM 
    products p
LEFT JOIN 
    order_items oi ON p.product_id = oi.product_id
LEFT JOIN 
    orders o ON oi.order_id = o.order_id
WHERE 
    o.status != 'cancelled' OR o.status IS NULL
GROUP BY 
    p.product_id, p.product_name, p.category;

COMMENT ON VIEW product_sales_summary IS 'View that summarizes sales by product';

-- Recent Inventory Changes
-- View that shows recent inventory changes for CDC testing
CREATE OR REPLACE VIEW recent_inventory_changes AS
SELECT 
    ic.change_id, 
    p.product_id, 
    p.product_name, 
    ic.change_type, 
    ic.quantity, 
    ic.previous_quantity, 
    ic.new_quantity, 
    ic.changed_at
FROM 
    inventory_changes ic
JOIN 
    products p ON ic.product_id = p.product_id
ORDER BY 
    ic.changed_at DESC
LIMIT 100;

COMMENT ON VIEW recent_inventory_changes IS 'View that shows recent inventory changes for CDC testing';

-- ---------------------------------------------------------------------------
-- Triggers
-- ---------------------------------------------------------------------------

-- Update Product Inventory Trigger
-- Trigger to update product inventory when inventory_changes records are inserted
CREATE OR REPLACE FUNCTION update_product_inventory_func()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE products 
    SET 
        stock_quantity = NEW.new_quantity, 
        updated_at = CURRENT_TIMESTAMP 
    WHERE 
        product_id = NEW.product_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_product_inventory
AFTER INSERT ON inventory_changes
FOR EACH ROW
EXECUTE FUNCTION update_product_inventory_func();

COMMENT ON FUNCTION update_product_inventory_func() IS 'Function to update product inventory when inventory changes are recorded';
COMMENT ON TRIGGER update_product_inventory ON inventory_changes IS 'Trigger to update product inventory when inventory_changes records are inserted';

-- Update Order Timestamps Trigger
-- Trigger to update the updated_at timestamp when orders are modified
CREATE OR REPLACE FUNCTION update_order_timestamps_func()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_order_timestamps
BEFORE UPDATE ON orders
FOR EACH ROW
EXECUTE FUNCTION update_order_timestamps_func();

COMMENT ON FUNCTION update_order_timestamps_func() IS 'Function to update order timestamps when orders are modified';
COMMENT ON TRIGGER update_order_timestamps ON orders IS 'Trigger to update the updated_at timestamp when orders are modified';

COMMIT;

-- ---------------------------------------------------------------------------
-- Sample Data Insertion (Commented out, uncomment to use)
-- ---------------------------------------------------------------------------
/*
BEGIN;

-- Insert sample customers
INSERT INTO customers (first_name, last_name, email, phone_number, address_line1, city, state, postal_code, country) VALUES
('John', 'Doe', 'john.doe@example.com', '555-123-4567', '123 Main St', 'Anytown', 'CA', '12345', 'USA'),
('Jane', 'Smith', 'jane.smith@example.com', '555-987-6543', '456 Oak Ave', 'Somecity', 'NY', '67890', 'USA'),
('Robert', 'Johnson', 'robert.johnson@example.com', '555-456-7890', '789 Pine Blvd', 'Otherville', 'TX', '54321', 'USA'),
('Emily', 'Brown', 'emily.brown@example.com', '555-789-0123', '321 Cedar Ln', 'Lastcity', 'FL', '09876', 'USA');

-- Insert sample products
INSERT INTO products (product_name, description, category, price, cost, sku, stock_quantity) VALUES
('Smartphone X', 'Latest generation smartphone with advanced features', 'Electronics', 999.99, 750.00, 'PRDX001', 100),
('Laptop Pro', 'Professional laptop for developers and designers', 'Electronics', 1499.99, 1100.00, 'PRDL002', 50),
('Cotton T-Shirt', 'Comfortable cotton t-shirt, available in multiple colors', 'Clothing', 19.99, 5.00, 'PRDT003', 200),
('Running Shoes', 'Lightweight running shoes with cushioned soles', 'Footwear', 89.99, 35.00, 'PRDS004', 75),
('Coffee Maker', 'Automatic coffee maker with timer', 'Home Appliances', 49.99, 20.00, 'PRDC005', 30);

-- Insert sample orders
INSERT INTO orders (customer_id, status, total_amount, tax_amount, shipping_amount, shipping_address, payment_method) VALUES
(1, 'delivered', 1019.99, 90.00, 10.00, '123 Main St, Anytown, CA 12345, USA', 'Credit Card'),
(2, 'shipped', 1599.99, 140.00, 0.00, '456 Oak Ave, Somecity, NY 67890, USA', 'PayPal'),
(3, 'processing', 109.98, 10.00, 10.00, '789 Pine Blvd, Otherville, TX 54321, USA', 'Credit Card'),
(1, 'pending', 49.99, 5.00, 10.00, '123 Main St, Anytown, CA 12345, USA', 'Credit Card');

-- Insert sample order items
INSERT INTO order_items (order_id, product_id, quantity, unit_price, total_price) VALUES
(1, 1, 1, 999.99, 999.99),
(2, 2, 1, 1499.99, 1499.99),
(3, 3, 2, 19.99, 39.98),
(3, 4, 1, 89.99, 89.99),
(4, 5, 1, 49.99, 49.99);

-- Insert sample inventory changes
INSERT INTO inventory_changes (product_id, change_type, quantity, previous_quantity, new_quantity) VALUES
(1, 'sale', -1, 101, 100),
(2, 'sale', -1, 51, 50),
(3, 'sale', -2, 202, 200),
(4, 'sale', -1, 76, 75),
(5, 'sale', -1, 31, 30),
(1, 'restock', 50, 100, 150),
(3, 'adjustment', -10, 200, 190);

COMMIT;
*/