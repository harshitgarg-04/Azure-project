import dlt

@dlt.table
def DimArt_Stg():
    df = spark.read.table("spotify_cata.silver.dimart")
    return df


dlt.create_streaming_table("dimartist")

dlt.create_auto_cdc_flow(
  target = "dimartist",
  source = "dimart_stg",
  keys = ["artist_id"],
  sequence_by = "updated_at",
  stored_as_scd_type = 2,
  track_history_except_column_list = None,
  name = None,
  once = False
)