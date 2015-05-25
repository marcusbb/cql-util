
CREATE TABLE wide_row (
  id text,
  name text,
  value text,
  PRIMARY KEY ((id), name)
);

CREATE TABLE devices (
  id text,
  name text,
  value text,
  value_ascii ascii,
  value_d bigint,
  PRIMARY KEY (id, name)
);

CREATE TABLE devices2 (
  id text,
  name text,
  type text,
  value text,
  value_ascii ascii,
  value_d bigint,
  PRIMARY KEY (id, type, name)
);
