CREATE TABLE sample ( key text,  col1 text, col2 bigint, properties map<text,text>,date_col timestamp, b_info blob, PRIMARY KEY (key) );

CREATE TABLE id_only (first text, second bigint, primary key(first,second));