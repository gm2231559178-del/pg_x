-- Sample material catalog for pgx graphql demo

CREATE TABLE materials (
    mat_no    VARCHAR(20) PRIMARY KEY,
    name      VARCHAR(100) NOT NULL,
    status    VARCHAR(20) NOT NULL DEFAULT 'active'
);

CREATE TABLE sizes (
    id        SERIAL PRIMARY KEY,
    size_code VARCHAR(10) NOT NULL,
    mat_no    VARCHAR(20) NOT NULL REFERENCES materials(mat_no),
    name      VARCHAR(100) NOT NULL
);

CREATE INDEX idx_sizes_mat_no ON sizes(mat_no);

CREATE TABLE colorways (
    id            SERIAL PRIMARY KEY,
    colorway_code VARCHAR(10) NOT NULL,
    mat_no        VARCHAR(20) NOT NULL REFERENCES materials(mat_no),
    name          VARCHAR(100) NOT NULL,
    hex           VARCHAR(7)
);

CREATE INDEX idx_colorways_mat_no ON colorways(mat_no);

INSERT INTO materials (mat_no, name, status) VALUES
    ('M001', 'Premium Cotton Canvas', 'active'),
    ('M002', 'Merino Wool Blend',     'active'),
    ('M003', 'Recycled Polyester',    'discontinued');

INSERT INTO sizes (size_code, mat_no, name) VALUES
    ('S',  'M001', 'Small'),
    ('M',  'M001', 'Medium'),
    ('L',  'M001', 'Large'),
    ('XL', 'M001', 'Extra Large'),
    ('S',  'M002', 'Small'),
    ('M',  'M002', 'Medium'),
    ('L',  'M002', 'Large'),
    ('S',  'M003', 'Small'),
    ('M',  'M003', 'Medium');

INSERT INTO colorways (colorway_code, mat_no, name, hex) VALUES
    ('WH', 'M001', 'White',    '#FFFFFF'),
    ('BK', 'M001', 'Black',    '#000000'),
    ('NV', 'M001', 'Navy',     '#000080'),
    ('RD', 'M002', 'Red',      '#FF0000'),
    ('GR', 'M002', 'Green',    '#00FF00'),
    ('BL', 'M002', 'Blue',     '#0000FF'),
    ('GY', 'M003', 'Grey',     '#808080');

CREATE TABLE material_features (
    id            SERIAL PRIMARY KEY,
    mat_no        VARCHAR(20) NOT NULL REFERENCES materials(mat_no),
    feature_name  VARCHAR(100) NOT NULL,
    description   VARCHAR(200)
);

CREATE INDEX idx_features_mat_no ON material_features(mat_no);

CREATE TABLE feature_attributes (
    id         SERIAL PRIMARY KEY,
    feature_id INTEGER NOT NULL REFERENCES material_features(id),
    attr_name  VARCHAR(50) NOT NULL,
    attr_value VARCHAR(200) NOT NULL
);

CREATE INDEX idx_fattrs_feature_id ON feature_attributes(feature_id);

INSERT INTO material_features (mat_no, feature_name, description) VALUES
    ('M001', 'Construction', 'Plain weave'),
    ('M001', 'Care',         'Standard care instructions'),
    ('M002', 'Construction', 'Knitted'),
    ('M002', 'Certification', NULL),
    ('M003', 'Construction', 'Twist'),
    ('M003', 'Eco',          'Recycled materials');

INSERT INTO feature_attributes (feature_id, attr_name, attr_value) VALUES
    -- M001 Construction
    (1, 'weave_type',   'plain'),
    (1, 'thread_count', '120'),
    -- M001 Care
    (2, 'wash',  '30°C'),
    (2, 'bleach', 'No'),
    -- M002 Construction
    (3, 'weave_type', 'knit'),
    (3, 'weight',     '180 gsm'),
    -- M002 Certification
    (4, 'standard', 'OEKO-TEX'),
    (4, 'class',    'I'),
    -- M003 Construction
    (5, 'weave_type', 'twist'),
    (5, 'weight',     '150 gsm'),
    -- M003 Eco
    (6, 'recycled_content', '100%'),
    (6, 'certification',    'GRS');

-- NOTIFY trigger for pgx graphql → Elasticsearch pipeline.
-- Sends ContractMessage format on INSERT/UPDATE/DELETE of materials.

CREATE OR REPLACE FUNCTION notify_material_change()
RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify(
    'materials',
    json_build_object(
      'meta', json_build_object(
        'event_type', 'MaterialFull',
        'schema_version', '1'
      ),
      'data', json_build_object(
        'mat_no', COALESCE(NEW.mat_no, OLD.mat_no)
      )
    )::text
  );
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER materials_notify
  AFTER INSERT OR UPDATE OR DELETE ON materials
  FOR EACH ROW
  EXECUTE FUNCTION notify_material_change();
