CREATE TABLE posts (
   id BIGSERIAL PRIMARY KEY, -- auto-incrementing primary key
   subreddit TEXT NOT NULL, -- name of the subreddit
   post_id TEXT NOT NULL, -- unique Reddit post ID
   data JSONB NOT NULL, -- store Reddit post data as JSONB
   created_at TIMESTAMPTZ DEFAULT NOW() -- timestamp for when the post is added
);
