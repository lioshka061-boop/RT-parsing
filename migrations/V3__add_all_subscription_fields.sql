ALTER TABLE subscription ADD COLUMN maximum_exports BIGINT;
ALTER TABLE subscription ADD COLUMN links_per_export BIGINT;
ALTER TABLE subscription ADD COLUMN unique_links BIGINT;
ALTER TABLE subscription ADD COLUMN descriptions BIGINT;
ALTER TABLE subscription ADD COLUMN maximum_description_size BIGINT;
ALTER TABLE subscription ADD COLUMN categories BIGINT;
ALTER TABLE subscription ADD COLUMN minimum_update_rate BIGINT;
