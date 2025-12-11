CREATE TABLE user_credentials (
	login TEXT PRIMARY KEY,
	password TEXT,
	salt BYTEA,
	access TEXT[],
	maximum_shops BIGINT,
	registration_token TEXT
)
