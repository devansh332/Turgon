CREATE TABLE silver_proposed.tickets (
  ticket_id TEXT NOT NULL,
  submitted_by VARCHAR(100),
  assigned_to VARCHAR(100),
  building VARCHAR(150),
  category VARCHAR(100),
  status VARCHAR(50),
  priority VARCHAR(30),
  description TEXT,
  resolution_notes TEXT,
  created_at TIMESTAMP,
  resolved_at TIMESTAMP,
  sla_hours INT,
  cost DECIMAL(12, 2)
)