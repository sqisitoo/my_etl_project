resource "snowflake_database" "raw_db" {
  name         = "RAW"
  is_transient = false
}

resource "snowflake_database" "analytic_db" {
  name         = "ANALYTICS"
  is_transient = false
}

resource "snowflake_schema" "raw_air_pollution" {
  database = snowflake_database.raw_db.name
  name     = "AIR_POLLUTION"
}


resource "snowflake_warehouse" "tf_warehouse" {
  name                      = "SNOWFLAKE_WH"
  warehouse_type            = "STANDARD"
  warehouse_size            = "XSMALL"
  max_cluster_count         = 1
  min_cluster_count         = 1
  auto_suspend              = 60
  auto_resume               = true
  enable_query_acceleration = false
  initially_suspended       = true
}

