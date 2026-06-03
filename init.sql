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

CREATE TABLE material_attributes (
    id         SERIAL PRIMARY KEY,
    mat_no     VARCHAR(20) NOT NULL REFERENCES materials(mat_no),
    attr_name  VARCHAR(50) NOT NULL,
    attr_value VARCHAR(200) NOT NULL
);

CREATE INDEX idx_attrs_mat_no ON material_attributes(mat_no);

INSERT INTO material_attributes (mat_no, attr_name, attr_value) VALUES
    ('M001', 'weight',        '200 gsm'),
    ('M001', 'width',         '150 cm'),
    ('M001', 'care',          'Machine wash 30°C'),
    ('M002', 'weight',        '180 gsm'),
    ('M002', 'width',         '120 cm'),
    ('M002', 'care',          'Dry clean only'),
    ('M002', 'stretch',       '4-way stretch'),
    ('M003', 'weight',        '150 gsm'),
    ('M003', 'width',         '160 cm'),
    ('M003', 'water_resistant', 'yes');
