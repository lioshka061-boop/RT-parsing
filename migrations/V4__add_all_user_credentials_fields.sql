ALTER TABLE user_credentials ADD COLUMN subscription_id UUID;
ALTER TABLE user_credentials ADD COLUMN subscription_version BIGINT;

ALTER TABLE user_credentials
      ADD CONSTRAINT fk_user_subscription FOREIGN KEY (subscription_id, subscription_version) 
          REFERENCES subscription (id, version);
