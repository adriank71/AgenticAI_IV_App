create extension if not exists pgcrypto;

create table if not exists public.calendar_events (
  id uuid primary key default gen_random_uuid(),
  user_id text not null,
  title text not null,
  description text,
  start_at timestamptz not null,
  end_at timestamptz,
  all_day boolean not null default false,
  category text,
  location text,
  color text,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists calendar_events_user_start_idx
  on public.calendar_events (user_id, start_at);

create index if not exists calendar_events_user_title_idx
  on public.calendar_events (user_id, lower(title));

alter table public.calendar_events enable row level security;

do $$
begin
  if to_regclass('public.events') is not null then
    execute $sql$
      insert into public.calendar_events (
        id,
        user_id,
        title,
        description,
        start_at,
        end_at,
        all_day,
        category,
        location,
        color,
        metadata,
        created_at,
        updated_at
      )
      select
        gen_random_uuid(),
        'default',
        title,
        nullif(notes, ''),
        (
          event_date::timestamp
          + case
            when all_day or nullif(start_time, '') is null then time '00:00'
            else start_time::time
          end
        ) at time zone 'Europe/Berlin',
        case
          when all_day then (event_date::timestamp + interval '1 day') at time zone 'Europe/Berlin'
          when nullif(end_time, '') is not null then (event_date::timestamp + end_time::time) at time zone 'Europe/Berlin'
          when nullif(start_time, '') is not null then (event_date::timestamp + start_time::time + interval '30 minutes') at time zone 'Europe/Berlin'
          else null
        end,
        all_day,
        category,
        nullif(transport_address, ''),
        null,
        jsonb_build_object(
          'legacy_event_id', event_id,
          'notes', notes,
          'hours', hours,
          'assistant_hours', coalesce(assistant_hours, '{}'::jsonb),
          'transport_mode', transport_mode,
          'transport_kilometers', transport_kilometers,
          'transport_address', transport_address
        ),
        created_at,
        updated_at
      from public.events legacy_events
      where not exists (
        select 1
        from public.calendar_events existing_events
        where existing_events.metadata->>'legacy_event_id' = legacy_events.event_id
      );
    $sql$;
  end if;
end $$;
