-- This runs automatically when the container starts for the first time

CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL
);

CREATE OR REPLACE FUNCTION notify_user_changes()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify(
        'user_updates', 
        json_build_object(
            'action', TG_OP,
            'data', row_to_json(NEW)
        )::text
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trigger_user_changes
AFTER INSERT OR UPDATE ON users
FOR EACH ROW
EXECUTE FUNCTION notify_user_changes();


-- INSERT INTO users (username, email) VALUES ('alice', 'alice@example.com');
-- Usage example:
--   DATABASE_URL=$DATABASE_URL pgx listen -C user_updates shell --command 'echo "[$PGX_CHANNEL] $PGX_PAYLOAD" >> pg_notify.log' --mode simple
--   docker compose exec -it postgres psql -U postgres -d postgres -c "NOTIFY user_updates, 'optional_payload';"