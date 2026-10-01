return { postgres = { up = [[
  CREATE TABLE IF NOT EXISTS appchi_policies (
    operator_id uuid PRIMARY KEY,
    generation bigint NOT NULL CHECK (generation > 0),
    document text NOT NULL
  );
  CREATE OR REPLACE FUNCTION appchi_policy_monotonic() RETURNS trigger AS $$
  BEGIN
    IF NEW.generation < OLD.generation OR
       (NEW.generation = OLD.generation AND NEW.document <> OLD.document) THEN
      RAISE EXCEPTION 'appchi policy generation conflict';
    END IF;
    RETURN NEW;
  END; $$ LANGUAGE plpgsql;
  CREATE TRIGGER appchi_policy_monotonic BEFORE UPDATE ON appchi_policies
    FOR EACH ROW EXECUTE FUNCTION appchi_policy_monotonic();
]] } }
