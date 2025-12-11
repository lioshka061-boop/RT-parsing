CREATE TABLE subscription (
	id UUID,
	maximum_shops BIGINT,
	price DECIMAL,
	name TEXT,
	version BIGINT,
	yanked BOOLEAN,
	PRIMARY KEY(id, version)
);
