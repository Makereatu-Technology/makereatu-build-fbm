INSERT INTO variation_trigger_type
(id, category_id, category_name, trigger_name, description, impacts_cost, impacts_time, impacts_governance)
VALUES
('3.2', 3, 'Unforeseen conditions', 'Existing services/utilities',
'Discovery or conflict with existing utilities/services affecting work', TRUE, TRUE, TRUE),
('5.1', 5, 'Exceptional events', 'Extreme weather',
'Weather exceeding normal seasonal expectations impacting time/cost', TRUE, TRUE, TRUE)
ON CONFLICT (id) DO NOTHING;