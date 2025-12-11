CREATE EXTENSION IF NOT EXISTS hstore;

CREATE TABLE davi_product (
	article TEXT PRIMARY KEY,
	title TEXT,
	description TEXT,
	price BIGINT,
	available INTEGER,
	url TEXT,
	last_visited TIMESTAMP WITH TIME ZONE,
	images TEXT[],
	properties HSTORE,
	categories TEXT[]
)
