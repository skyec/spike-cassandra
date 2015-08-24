CREATE TABLE IF NOT EXISTS spike.resources_simple (
        resource_id text,
        event_time timestamp,
        event text,
        PRIMARY KEY (resource_id, event_time)
                );
