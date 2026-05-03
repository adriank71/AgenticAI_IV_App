create extension if not exists pgcrypto;

create table if not exists public.document_folders (
  folder_id uuid primary key default gen_random_uuid(),
  user_id text not null,
  name text not null,
  parent_folder_id uuid references public.document_folders(folder_id) on delete set null,
  color text,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists document_folders_user_parent_name_idx
  on public.document_folders (
    user_id,
    coalesce(parent_folder_id, '00000000-0000-0000-0000-000000000000'::uuid),
    lower(name)
  );

create table if not exists public.documents (
  document_id uuid primary key default gen_random_uuid(),
  user_id text not null,
  folder_id uuid references public.document_folders(folder_id) on delete set null,
  file_name text not null,
  safe_file_name text not null,
  storage_bucket text not null default 'Invoice_upload',
  storage_key text not null unique,
  storage_url text not null,
  content_type text not null,
  content_size bigint not null default 0,
  checksum_sha256 text not null,
  document_type text,
  institution text,
  document_date date,
  year integer not null,
  month integer not null check (month between 1 and 12),
  tags text[] not null default '{}'::text[],
  summary text,
  extracted_text text,
  extraction_status text not null default 'pending',
  extraction_error text,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists documents_user_created_idx
  on public.documents (user_id, created_at desc);

create index if not exists documents_user_month_idx
  on public.documents (user_id, year, month);

create index if not exists documents_user_type_idx
  on public.documents (user_id, lower(document_type));

create index if not exists documents_user_institution_idx
  on public.documents (user_id, lower(institution));

create index if not exists documents_user_tags_idx
  on public.documents using gin (tags);

create table if not exists public.document_matches (
  match_id uuid primary key default gen_random_uuid(),
  user_id text not null,
  source_document_id uuid not null references public.documents(document_id) on delete cascade,
  target_document_id uuid not null references public.documents(document_id) on delete cascade,
  match_type text not null default 'related',
  score double precision not null default 0,
  reason text,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  unique (source_document_id, target_document_id, match_type)
);

create index if not exists document_matches_user_source_idx
  on public.document_matches (user_id, source_document_id);

alter table public.document_folders enable row level security;
alter table public.documents enable row level security;
alter table public.document_matches enable row level security;
