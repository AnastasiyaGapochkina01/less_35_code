
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    age INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE user_audit (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    action VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


INSERT INTO users (email, first_name, last_name, age) VALUES
('alice@example.com', 'Alice', 'Johnson', 28),
('bob@example.com', 'Bob', 'Smith', 32),
('carol@example.com', 'Carol', 'Davis', 25);


CREATE INDEX idx_users_email ON users(email);
