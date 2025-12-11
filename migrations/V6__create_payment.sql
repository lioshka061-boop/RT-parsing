CREATE TABLE payment (
	id UUID PRIMARY KEY,
	user_id TEXT,
	subscription_id UUID,
	subscription_version BIGINT,
	paid_days INTEGER,
	amount DECIMAL,
	currency TEXT,
	date TIMESTAMP WITH TIME ZONE,
	status TEXT,
	reason TEXT
)
