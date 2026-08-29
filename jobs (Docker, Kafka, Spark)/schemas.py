from pyspark.sql.types import StructType, StringType, IntegerType, FloatType, BooleanType

stream_schema = (StructType().add("id", StringType()).add("user_id", StringType()).add("user_login", StringType()).add("user_name", StringType()).add("game_id", StringType()).add("game_name", StringType())
.add("type", StringType()).add("title", StringType()).add("viewer_count", IntegerType()).add("started_at", StringType()).add("language", StringType()).add("thumbnail_url", StringType()).add("tag_ids", StringType()).add("tags", StringType()).add("is_mature", StringType()))

user_schema = (StructType().add("id", StringType()).add("login", StringType()).add("display_name", StringType()).add("type", StringType()).add("broadcaster_type", StringType()).add("description", StringType())
.add("profile_image_url", StringType()).add("offline_image_url", StringType()).add("view_count", IntegerType()).add("created_at", StringType()))

video_schema = (StructType().add("id", StringType()).add("stream_id", StringType()).add("user_id", StringType()).add("user_login", StringType()).add("user_name", StringType()).add("title", StringType())
.add("description", StringType()).add("created_at", StringType()).add("published_at", StringType()).add("url", StringType()).add("thumbnail_url", StringType()).add("viewable", StringType()).add("view_count", IntegerType()).add("language", StringType()).add("type", StringType()).add("duration", StringType()).add("muted_segments", StringType()))

clip_schema = (StructType().add("id", StringType()).add("url", StringType()).add("embed_url", StringType()).add("broadcaster_id", StringType()).add("broadcaster_name", StringType())
.add("creator_id", StringType()).add("creator_name", StringType()).add("video_id", StringType()).add("game_id", StringType()).add("language", StringType())
.add("title", StringType()).add("view_count", IntegerType()).add("created_at", StringType()).add("thumbnail_url", StringType()).add("duration", FloatType())
.add("vod_offset", IntegerType()).add("is_featured", BooleanType()))

top_game_schema = StructType().add("id", StringType()).add("name", StringType()).add("box_art_url", StringType()).add("igdb_id", StringType())
