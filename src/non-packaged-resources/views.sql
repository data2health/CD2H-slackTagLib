create materialized view person as
select
    raw->>'id' as id,
    raw->>'name' as display_name,
    to_timestamp((raw->>'updated')::int) as updated,
    (raw->>'profile')::jsonb->>'first_name' as first_name,
    (raw->>'profile')::jsonb->>'last_name' as last_name,
    (raw->>'profile')::jsonb->>'real_name' as real_name,
    (raw->>'profile')::jsonb->>'email' as email,
    (raw->>'profile')::jsonb->>'skype' as skype,
    (raw->>'profile')::jsonb->>'title' as title,
    (raw->>'profile')::jsonb->>'fields' as fields,
    (raw->>'profile')::jsonb->>'image_24' as image_24
from user_raw;

create materialized view channel as
select
    raw->>'id' as id,
    raw->>'name' as name,
    to_timestamp((raw->>'created')::int) as created,
    raw->>'creator' as creator,
    (raw->>'num_members')::int as num_members,
    (raw->>'is_archived')::boolean as archived,
    (raw->>'topic')::jsonb->>'value' as topic,
    (raw->>'purpose')::jsonb->>'value' as purpose
from channel_raw;

create materialized view message_staging as
select
    channel_id,
    to_timestamp((raw->>'ts')::float) as ts,
    raw->>'user' as user,
    raw->>'type' as type,
    raw->>'subtype' as subtype,
    raw->>'text' as text,
    (raw->>'blocks')::jsonb as blocks,
    (raw->>'attachments')::jsonb as attachments,
    (raw->>'files')::jsonb as files,
    (raw->>'reactions')::jsonb as reactions
from message_raw
order by ts;
