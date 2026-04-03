############################################
# TAPE PIPELINE
# Author: Dave Nkoro
# Purpose: Kobo → Transform → PostgreSQL
############################################

# ---------- Libraries ----------
suppressPackageStartupMessages({
  library(tidyverse)
  library(httr)
  library(jsonlite)
  library(DBI)
  library(RPostgres)
library(dotenv)

})

load_dot_env()

# ---------- Environment variables ----------
kobo_token <- Sys.getenv("KOBO_TOKEN")
pg_db      <- Sys.getenv("PG_DB")
pg_host    <- Sys.getenv("PG_HOST")
pg_user    <- Sys.getenv("PG_USER")
pg_pass    <- Sys.getenv("PG_PASSWORD")

stopifnot(
  kobo_token != "",
  pg_db      != "",
  pg_host    != "",
  pg_user    != "",
  pg_pass    != ""
)

message("✅ Environment variables loaded")

# ---------- Kobo API call ----------
kobo_url <- "https://kobo.fao.org/api/v2/assets/ajfT4SY4zcJPVqKCJuuQxH/export-settings/eswn645vPV35hd3ULUuZgG9/data.csv"

res <- GET(
  kobo_url,
  add_headers(Authorization = kobo_token)
)

stopifnot(status_code(res) == 200)

csv_content <- content(res, as = "text", encoding = "UTF-8")

data <- read_delim(csv_content, delim = ";", show_col_types = FALSE) %>%
  select(-ends_with("note"))

message("✅ Kobo data downloaded")

# ---------- Data preparation ----------
data <- data %>%
  mutate(
    Farm_id = ifelse(
      is.na(inquirer),
      paste0("Unknown_", `_id`),
      paste0(inquirer, "_", `_id`)
    ),
    area_agric =
      area_tem_crop +
      area_tem_meapas +
      area_tem_fal +
      area_per_crop +
      area_per_meapas
  ) %>%
  select(
    Farm_id, country, region,
    `_gps_loc_latitude`, `_gps_loc_longitude`, area_total, hh_or_not,
    irrig, farm_mgt, hh_men, hh_women,
    div_score, synergy_score, recycling_score, efficiency_score,
    resilience_score, cultfood_score, cocrea_score, human_score,
    circular_score, respgov_score, caet_score, area_agric,
    plant_diversity, temp_spatial_div, anim_diversity,
    crop_liv_aqua, habitat_mgt, seed_breed, recycling_biomass,
    mgt_soilfert, mgt_pestdis, water_and_energy_use,
    soc_econ_res, soil_conserve, anim_health_nutri,
    dietdiv_foodself, food_heritage, int_ageco, soc_know,
    wom_emp_caet, labour_socinq, labor_condition,
    local_market, local_circ, prod_empow, prod_access, prod_orgs
  )

message("✅ Data transformed")

# ---------- Indexes table ----------
indexes <- data %>%
  pivot_longer(
    cols = c(
      plant_diversity, temp_spatial_div, anim_diversity,
      crop_liv_aqua, habitat_mgt, seed_breed, recycling_biomass,
      mgt_soilfert, mgt_pestdis, water_and_energy_use,
      soc_econ_res, soil_conserve, anim_health_nutri,
      dietdiv_foodself, food_heritage, int_ageco, soc_know,
      wom_emp_caet, labour_socinq, labor_condition,
      local_market, local_circ, prod_empow, prod_access, prod_orgs
    ),
    names_to  = "Indexes",
    values_to = "indexes_score"
  ) %>%
  mutate(
    Indexes = recode(Indexes,
                     plant_diversity = "Plant Diversity",
                     temp_spatial_div = "Temporal and spatial diversity",
                     anim_diversity = "Animal diversity",
                     crop_liv_aqua = "Plant-livestock-aquaculture integration",
                     habitat_mgt = "Habitat management",
                     seed_breed = "Use of seeds and breeds",
                     recycling_biomass = "Biomass and waste management",
                     mgt_soilfert = "Management of soil fertility",
                     mgt_pestdis = "Management of pests and diseases",
                     water_and_energy_use = "Water and energy use",
                     soc_econ_res = "Social and economic resilience",
                     soil_conserve = "Soil conservation practices",
                     anim_health_nutri = "Animal health, nutrition, and management",
                     dietdiv_foodself = "Dietary diversity and food self sufficiency",
                     food_heritage = "Local and traditional food heritage",
                     int_ageco = "Accessing and valuing agroecological knowledge",
                     soc_know = "Peer learning and co-creation of knowledge",
                     wom_emp_caet = "Women empowerment and social equity",
                     labour_socinq = "Labour conditions",
                     labor_condition = "Labour condition improvement",
                     local_market = "Products marketed locally",
                     local_circ = "Local sourcing and circularity",
                     prod_empow = "Producers' empowerment",
                     prod_access = "Access to resources",
                     prod_orgs = "Producers' organizations"
    )
  ) %>%
  select(Farm_id, country, Indexes, indexes_score)

# ---------- Elements table ----------
element <- data %>%
  pivot_longer(
    cols = c(
      div_score, synergy_score, recycling_score, efficiency_score,
      resilience_score, cultfood_score, cocrea_score, human_score,
      circular_score, respgov_score
    ),
    names_to  = "Element",
    values_to = "avg_score"
  ) %>%
  mutate(
    Element = recode(Element,
                     div_score = "Div",
                     synergy_score = "Syn",
                     efficiency_score = "Eff",
                     recycling_score = "Rec",
                     resilience_score = "Res",
                     cultfood_score = "CulFood",
                     cocrea_score = "Co-Cre",
                     human_score = "Human",
                     circular_score = "Circu",
                     respgov_score = "Resg"
    )
  ) %>%
  select(Farm_id, country, Element, avg_score)

# ---------- PostgreSQL export ----------
con <- dbConnect(
  RPostgres::Postgres(),
  dbname   = pg_db,
  host     = pg_host,
  port     = 5432,
  user     = pg_user,
  password = pg_pass
)

dbWriteTable(con, "tape_data_kobo", data, overwrite = TRUE)
dbWriteTable(con, "tape_indexes", indexes, overwrite = TRUE)
dbWriteTable(con, "tape_element", element, overwrite = TRUE)

dbExecute(con, "ANALYZE;")
dbDisconnect(con)

message("✅ PostgreSQL tables updated successfully")
message("🎯 Pipeline completed without errors")
