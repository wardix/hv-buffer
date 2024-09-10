DEFINE TABLE IF NOT EXISTS hikvision_event SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS company ON TABLE hikvision_event TYPE record<company>;
DEFINE FIELD IF NOT EXISTS device_name ON TABLE hikvision_event TYPE string;
DEFINE FIELD IF NOT EXISTS employee_id ON TABLE hikvision_event TYPE string;
DEFINE FIELD IF NOT EXISTS event_time ON TABLE hikvision_event TYPE datetime;
DEFINE FIELD IF NOT EXISTS request_time ON TABLE hikvision_event TYPE datetime;
DEFINE FIELD IF NOT EXISTS created_at ON TABLE hikvision_event TYPE datetime;
DEFINE FIELD IF NOT EXISTS updated_at ON TABLE hikvision_event TYPE datetime;
