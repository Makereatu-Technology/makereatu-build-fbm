INSERT INTO variation_trigger_type
(id, category_id, category_name, trigger_name, description, impacts_cost, impacts_time, impacts_governance)
VALUES
('1.1', 1, 'Owner-directed changes', 'Scope addition', 'Scope addition', TRUE, FALSE, FALSE),
('1.2', 1, 'Owner-directed changes', 'Scope reduction/omission', 'Scope reduction/omission', TRUE, FALSE, FALSE),
('1.3', 1, 'Owner-directed changes', 'Quality/specification change', 'Quality/specification change', TRUE, FALSE, FALSE),
('1.4', 1, 'Owner-directed changes', 'Quantity change', 'Quantity change', TRUE, FALSE, FALSE),
('1.5', 1, 'Owner-directed changes', 'Layout/position change', 'Layout/position change', TRUE, FALSE, FALSE),
('1.6', 1, 'Owner-directed changes', 'Sequence/timing change', 'Sequence/timing change', TRUE, TRUE, FALSE),

('2.1', 2, 'Contract/commercial events', 'Late information from owner', 'Late information from owner', TRUE, TRUE, FALSE),
('2.2', 2, 'Contract/commercial events', 'Document discrepancy', 'Document discrepancy', TRUE, FALSE, FALSE),
('2.3', 2, 'Contract/commercial events', 'Ambiguous specification', 'Ambiguous specification', TRUE, FALSE, FALSE),
('2.4', 2, 'Contract/commercial events', 'Provisional sum adjustment', 'Provisional sum adjustment', TRUE, FALSE, FALSE),
('2.5', 2, 'Contract/commercial events', 'Subcontractor default', 'Subcontractor default', TRUE, FALSE, FALSE),
('2.6', 2, 'Contract/commercial events', 'Late owner-supplied materials', 'Late owner-supplied materials', TRUE, TRUE, FALSE),
('2.7', 2, 'Contract/commercial events', 'Late or restricted site access', 'Late or restricted site access', TRUE, TRUE, FALSE),

('3.1', 3, 'Site/physical conditions', 'Unforeseen site condition', 'Unforeseen site condition', TRUE, TRUE, TRUE),
('3.2', 3, 'Site/physical conditions', 'Existing services/utilities', 'Existing services/utilities', TRUE, TRUE, TRUE),
('3.3', 3, 'Site/physical conditions', 'Unexpected ground conditions', 'Unexpected ground conditions', TRUE, TRUE, TRUE),
('3.4', 3, 'Site/physical conditions', 'Geotechnical discrepancy', 'Geotechnical discrepancy', TRUE, TRUE, TRUE),
('3.5', 3, 'Site/physical conditions', 'Environmental discovery', 'Environmental discovery', TRUE, FALSE, TRUE),

('4.1', 4, 'Regulatory/legal events', 'Consent condition change', 'Consent condition change', TRUE, FALSE, TRUE),
('4.2', 4, 'Regulatory/legal events', 'Change in law/regulation', 'Change in law/regulation', TRUE, FALSE, TRUE),
('4.3', 4, 'Regulatory/legal events', 'Authority requirement', 'Authority requirement', TRUE, FALSE, TRUE),
('4.4', 4, 'Regulatory/legal events', 'Third-party requirement', 'Third-party requirement', TRUE, FALSE, TRUE),

('5.1', 5, 'Exceptional events', 'Extreme weather', 'Extreme weather', TRUE, TRUE, TRUE),
('5.2', 5, 'Exceptional events', 'Pandemic/epidemic', 'Pandemic/epidemic', TRUE, TRUE, TRUE),
('5.3', 5, 'Exceptional events', 'Suspension by owner', 'Suspension by owner', TRUE, TRUE, FALSE),
('5.4', 5, 'Exceptional events', 'Early occupation by owner', 'Early occupation by owner', TRUE, TRUE, FALSE),

('6.1', 6, 'Funder/MDB oversight events', 'Cumulative variation exceeds funder threshold', 'Cumulative variation exceeds funder threshold', TRUE, FALSE, TRUE),
('6.2', 6, 'Funder/MDB oversight events', 'Scope modification requiring no-objection', 'Scope modification requiring no-objection', TRUE, FALSE, TRUE),
('6.3', 6, 'Funder/MDB oversight events', 'Time extension affecting completion', 'Time extension affecting completion', TRUE, TRUE, TRUE),
('6.4', 6, 'Funder/MDB oversight events', 'Contract termination/replacement', 'Contract termination/replacement', TRUE, FALSE, TRUE),
('6.5', 6, 'Funder/MDB oversight events', 'Change affecting loan covenants', 'Change affecting loan covenants', TRUE, FALSE, TRUE),
('6.6', 6, 'Funder/MDB oversight events', 'Cost fluctuation adjustment', 'Cost fluctuation adjustment', TRUE, FALSE, TRUE)

ON CONFLICT (id) DO NOTHING;