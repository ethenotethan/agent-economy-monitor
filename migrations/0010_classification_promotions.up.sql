BEGIN;

CREATE TABLE agent_economy.classification_run_promotions (
    namespace_id uuid NOT NULL REFERENCES agent_economy.namespaces (namespace_id),
    buyer_handle_id text NOT NULL,
    promotion_sequence bigint NOT NULL CHECK (promotion_sequence > 0),
    run_id text NOT NULL,
    run_version integer NOT NULL CHECK (run_version > 0),
    promotion_method text NOT NULL CHECK (promotion_method <> ''),
    provenance_id uuid NOT NULL,
    promoted_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, buyer_handle_id, promotion_sequence),
    UNIQUE (namespace_id, buyer_handle_id, run_id, run_version),
    FOREIGN KEY (namespace_id, buyer_handle_id)
        REFERENCES agent_economy.buyer_handles (namespace_id, buyer_handle_id),
    FOREIGN KEY (namespace_id, run_id, run_version)
        REFERENCES agent_economy.classification_run_seals (
            namespace_id, run_id, run_version
        ),
    FOREIGN KEY (namespace_id, provenance_id)
        REFERENCES agent_economy.provenance_records (namespace_id, provenance_id)
);

CREATE FUNCTION agent_economy.validate_classification_run_promotion()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $function$
DECLARE
    target_buyer_handle_id text;
    expected_sequence bigint;
BEGIN
    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(
            NEW.namespace_id::pg_catalog.text || ':' || NEW.buyer_handle_id
                || ':classification-promotion',
            0
        )
    );

    SELECT run.buyer_handle_id INTO target_buyer_handle_id
    FROM agent_economy.classification_runs AS run
    JOIN agent_economy.classification_run_seals AS seal
      USING (namespace_id, run_id, run_version)
    WHERE run.namespace_id = NEW.namespace_id
      AND run.run_id = NEW.run_id
      AND run.run_version = NEW.run_version;

    IF NOT FOUND OR target_buyer_handle_id IS DISTINCT FROM NEW.buyer_handle_id THEN
        RAISE EXCEPTION 'classification promotion must target a sealed run for the same buyer'
            USING ERRCODE = '23514';
    END IF;

    SELECT coalesce(pg_catalog.max(promotion_sequence), 0) + 1
      INTO expected_sequence
    FROM agent_economy.classification_run_promotions
    WHERE namespace_id = NEW.namespace_id
      AND buyer_handle_id = NEW.buyer_handle_id;

    IF NEW.promotion_sequence <> expected_sequence THEN
        RAISE EXCEPTION 'classification promotion sequence must be contiguous'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END
$function$;

REVOKE ALL ON FUNCTION agent_economy.validate_classification_run_promotion() FROM PUBLIC;

CREATE TRIGGER classification_run_promotions_validate
BEFORE INSERT ON agent_economy.classification_run_promotions
FOR EACH ROW EXECUTE FUNCTION agent_economy.validate_classification_run_promotion();

CREATE TRIGGER classification_run_promotions_immutable
BEFORE UPDATE OR DELETE ON agent_economy.classification_run_promotions
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();

CREATE TRIGGER classification_run_promotions_truncate_immutable
BEFORE TRUNCATE ON agent_economy.classification_run_promotions
FOR EACH STATEMENT EXECUTE FUNCTION agent_economy.reject_immutable_mutation();

CREATE VIEW agent_economy.current_buyer_classification_runs AS
SELECT DISTINCT ON (promotion.namespace_id, promotion.buyer_handle_id)
    promotion.namespace_id,
    promotion.buyer_handle_id,
    promotion.run_id,
    promotion.run_version,
    promotion.promotion_sequence,
    promotion.promotion_method,
    promotion.provenance_id,
    promotion.promoted_at
FROM agent_economy.classification_run_promotions AS promotion
ORDER BY promotion.namespace_id, promotion.buyer_handle_id, promotion.promotion_sequence DESC;

COMMIT;
