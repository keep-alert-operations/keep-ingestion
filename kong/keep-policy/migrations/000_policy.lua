return { postgres = { up = [[
  CREATE TABLE IF NOT EXISTS keep_policies (
    operator_id uuid PRIMARY KEY,
    generation bigint NOT NULL CHECK (generation > 0),
    document text NOT NULL
  );
  CREATE OR REPLACE FUNCTION keep_policy_monotonic() RETURNS trigger AS $$
  BEGIN
    IF NEW.generation < OLD.generation OR
       (NEW.generation = OLD.generation AND NEW.document <> OLD.document) THEN
      RAISE EXCEPTION 'keep policy generation conflict';
    END IF;
    RETURN NEW;
  END; $$ LANGUAGE plpgsql;
  CREATE TRIGGER keep_policy_monotonic BEFORE UPDATE ON keep_policies
    FOR EACH ROW EXECUTE FUNCTION keep_policy_monotonic();
]] } }
