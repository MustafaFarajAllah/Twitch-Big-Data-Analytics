def write_to_gcs_batch(df, name):
    df.write.mode("overwrite").parquet(f"gs://twitch-big-data/Processed_data/{name}.parquet")


def write_to_bq_batch(df, table_name):
    df.write.format("bigquery").option("table", f"twitch-data-493815:TwitchAnalytics.{table_name}").option("writeMethod", "direct").option("createDisposition", "CREATE_IF_NEEDED").mode("overwrite").save()


def write_to_gcs_stream(df, name):
    return df.writeStream.outputMode("append").format("delta").option("path", f"gs://twitch-big-data/delta/{name}").option("checkpointLocation", f"gs://twitch-big-data/checkpoints/{name}").trigger(processingTime="5 seconds").start()

def write_to_bq_stream(df, table_name):
    checkpoint = "gs://twitch-big-data/checkpoints"
    return (df.writeStream.outputMode("append").foreachBatch(lambda batch_df, batch_id:batch_df.write.format("bigquery").option("table", f"twitch-data-493815:TwitchAnalytics.{table_name}") .option("writeMethod", "direct")
    .mode("append").save()).option("checkpointLocation", f"{checkpoint}/{table_name}_bq").trigger(processingTime="15 seconds").start())