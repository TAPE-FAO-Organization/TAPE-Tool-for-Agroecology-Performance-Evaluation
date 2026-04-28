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
    library(tidyverse)
    library(dplyr)
    library(httr)
    library(readr)
    library(googlesheets4)
    library(KoboconnectR)
    library(janitor)
    library(lubridate)
    library(sf)
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
  pg_db != "",
  pg_host != "",
  pg_user != "",
  pg_pass    != ""
)

message("✅ Environment variables loaded")

# ---------- Kobo API call ----------
##### Note: Hardcoded Asset and export UIDs are not recommended for sensitive data.
# No real data should be sent through any forms whose asset UIDs are exposed here.

kobo_url <- "https://kobo.fao.org/api/v2/assets/ajfT4SY4zcJPVqKCJuuQxH/export-settings/eswn645vPV35hd3ULUuZgG9/data.csv"


res <- GET(
  kobo_url,
  add_headers(Authorization = kobo_token)
)

stopifnot(status_code(res) == 200)

csv_content <- content(res, as = "text", encoding = "UTF-8")

clean_df <- read_delim(csv_content, delim = ";", show_col_types = FALSE) %>%
  rename_with(~ gsub("^_", "", .x)) %>%
  clean_names() %>%
  select(-ends_with("note"))

message("✅ Kobo data downloaded")

# ---------- Data Quality check  ----------


#***************** Detect numeric columns that have been exported as character************
#*# Exclude multi-select patterns: contains space/comma/semicolon/slash/pipe
#* # Must be exactly a single integer/decimal (whole string)
is_single_number <- function(x) {
  if (!is.character(x)) return(FALSE)
  x2 <- na_if(x, "")
  x2 <- x2[!is.na(x2)]
  if (length(x2) == 0) return(FALSE)
  if (any(str_detect(x2, "[,;|/]|\\s+\\d"))) return(FALSE)
  all(str_detect(x2, "^[-+]?(\\d+\\.?\\d*|\\.\\d+)$"))
}

numeric_like_vars <- clean_df %>%
  select(where(is.character)) %>%
  select(where(is_single_number)) %>%
  names()

clean_df <- clean_df %>%
  mutate(across(all_of(numeric_like_vars), as.numeric))



# ------------------------------------------------------------
# Identifies columns stored as numeric that actually represent
# boolean information (i.e. only 0/1 values, with possible NA).
# Convert these variables to logical (0/1)
# ------------------------------------------------------------

# Group all variables classified as decimal in definition file
decimal_vars <- c(
  "hh_men",
  "hh_women",
  "hh_myoung",
  "hh_fyoung",
  "employees_utf",
  "seasonal_workers_weeks",
  "seasonal_workers_hours_per_week",
  "freelance_workers_weeks",
  "freelance_workers_hours_per_week",
  "land_owned",
  "land_rentin",
  "area_common",
  "land_other"
)

# Function to convert numeric to boolean
convert_numeric_booleans <- function(df, decimal_vars) {
  stopifnot(is.data.frame(df))
  stopifnot(is.character(decimal_vars))

  for (col in names(df)) {
    x <- df[[col]]

    if (
      is.numeric(x) &&
        !(col %in% decimal_vars) &&
        length(unique(na.omit(x))) <= 2 &&
        all(na.omit(x) %in% c(0, 1))
    ) {
      df[[col]] <- as.logical(x)
    }
  }

  return(df)
}

# Apply boolean type correction to the cleaned dataset
clean_df <- convert_numeric_booleans(clean_df, decimal_vars)

# Function to enforce decimal type
enforce_decimal_types <- function(df, decimal_vars) {
  stopifnot(is.data.frame(df))
  stopifnot(is.character(decimal_vars))

  for (col in intersect(decimal_vars, names(df))) {
    if (is.logical(df[[col]])) {
      df[[col]] <- as.numeric(df[[col]])
    }
  }

  return(df)
}

# Enforce decimal type to the dataset after any boolean conversion
clean_df <- enforce_decimal_types(clean_df, decimal_vars)



# ------------------------------------------------------------
# Converts mixed area units (sqm, acre, ha) into hectares (ha)
# - sqm  → ha (divide by 10,000)
# - acre → ha (multiply by 0.40468564224)
# - ha   → unchanged
# After conversion, the unit column is normalized to "ha".
# ------------------------------------------------------------
convert_area_to_ha <- function(df, unit_col, area_cols) {
  df %>%
    mutate(
      across(
        all_of(area_cols),
        ~ case_when(
          !!sym(unit_col) == "sqm" ~ .x / 10000,
          !!sym(unit_col) == "acre" ~ .x * 0.40468564224,
          !!sym(unit_col) == "ha" ~ .x,
          TRUE ~ NA_real_
        )
      ),
      !!unit_col := "ha"
    )
}

# Define area columns
area_vars <- c(
  "area_tem_crop",
  "area_tem_meapas",
  "area_tem_fal",
  "area_per_crop",
  "area_per_meapas",
  "area_yard",
  "area_forest",
  "area_aquac",
  "area_other",
  "area_total",
  "area_common",
  "land_owned",
  "land_rentin",
  "land_other"
)

# Apply area unit conversion
clean_df <- convert_area_to_ha(
  clean_df,
  unit_col = "area_umeas",
  area_cols = area_vars
)



#-------------------------------------------------------------
# Interview duration quality control
# Compares expected interview duration (start → end)
# with actual submission duration (start → submission_time)
# using a  log-ratio approach to flag anomalies
# ------------------------------------------------------------
survey_duration_df <- clean_df %>%
  mutate(
    expected_duration = as.numeric(difftime(end, start, units = "hours")),
    actual_duration   = as.numeric(difftime(submission_time, start, units = "hours")),
    ratio = actual_duration / expected_duration,
    log_ratio = log(ratio),
    qc_time_issue = case_when(
      is.na(start) | is.na(end) | is.na(submission_time) ~ "missing_timestamp",
      expected_duration <= 0 ~ "invalid_expected",
      actual_duration <= 0   ~ "invalid_actual",
      TRUE ~ NA_character_
    )
  ) %>%
  mutate(
    log_ratio_median = median(log_ratio, na.rm = TRUE),
    log_ratio_mad    = mad(log_ratio, constant = 1, na.rm = TRUE),
    robust_z = (log_ratio - log_ratio_median) / log_ratio_mad,
    duration_flag = case_when(
      !is.na(qc_time_issue) ~ qc_time_issue,
      robust_z <= -3 ~ "too short severe",
      robust_z <= -2 ~ "too short moderate",
      robust_z >=  3 ~ "too long severe",
      robust_z >=  2 ~ "too long moderate",
      TRUE           ~ "ok"
    )
  ) %>%
  select(-log_ratio_median, -log_ratio_mad)


# ------------------------------------------------------------
# Daily enumerator productivity
# Computes the number of interviews completed per enumerator
# per survey day, used to detect unusually high workloads
# that may indicate rushed or low-quality interviews
# ------------------------------------------------------------

survey_productivity_df <- survey_duration_df %>%
  group_by(enumerator, today) %>%
  summarise(
    interviews_per_day = n()
  )
# ------------------------------------------------------------
# Interview timing plausibility checks
# Flags interviews conducted at unusual hours of the day
# that may indicate data quality or supervision issues
# ------------------------------------------------------------

survey_timing_df <- survey_duration_df %>%
  mutate(
    start = as.POSIXct(start, origin = "1970-01-01", tz = "UTC"),
    start_hour = if_else(is.na(start), NA_integer_, hour(start)),
    timing_flag = case_when(
      is.na(start_hour)        ~ "missing start time",
      start_hour < 6           ~ "implausible: very early",
      start_hour >= 22         ~ "implausible: very late",
      TRUE                     ~ "normal"
    )
  )

# Validate GPS coordinates for presence and plausible ranges
# Default (0,0) coordinates often indicate GPS not captured
# Latitude must be within [-90, 90]
# Longitude must be within [-180, 180]
survey_completeness_df <- survey_timing_df %>%
  mutate(
    gps_valid = case_when(
      is.na(gps_loc_latitude) | is.na(gps_loc_longitude) ~
        "incomplete: missing gps",
      gps_loc_latitude == 0 & gps_loc_longitude == 0 ~
        "incomplete: invalid gps (0,0)",
      gps_loc_latitude < -90 | gps_loc_longitude > 90 ~
        "incomplete: invalid latitude",
      gps_loc_latitude < -180 | gps_loc_longitude > 180 ~
        "incomplete: invalid longitude",

      TRUE ~ "complete"
    )
  )



# ------------------------------------------------------------
# GPS clustering check
# Detects multiple interviews conducted at the same (rounded)
# GPS location on the same day by the same enumerator,
# which may indicate interview fabrication or insufficient movement
# ------------------------------------------------------------
# Precision for rounding GPS coordinates (~11 m at equator )
# Round GPS coordinates to identify near-identical locations
# Count interviews conducted at the same location
# Flag potential GPS clustering issues

digits <- 4

survey_cluster_df <- survey_completeness_df %>%
  mutate(
    lat_round = if_else(gps_valid == "complete",
                        round(gps_loc_latitude, digits),
                        NA_real_),
    lon_round = if_else(gps_valid == "complete",
                        round(gps_loc_longitude, digits),
                        NA_real_)
  ) %>%
  group_by(submitted_by, today, lat_round, lon_round) %>%
  mutate(
    n_same_location = if_else(gps_valid == "complete", n(), NA_integer_),
    cluster_flag = case_when(
      gps_valid != "complete" ~ "data issue: missing/invalid gps",
      n_same_location >= 3    ~ "flag: more than 3 interviews same spot",
      n_same_location == 2    ~ "flag: 2 interviews same spot",
      TRUE                    ~ "ok"
    )
  ) %>%
  ungroup()


# ------------------------------------------------------------
# Respondent production system validation
# Harmonizes system type labels and checks consistency
# between declared system type and reported activities
# ------------------------------------------------------------

survey_system_validation_df <- survey_cluster_df %>%
  mutate(
    # Recode numeric system_type into interpretable labels
    system_type_declared = case_when(
      agecon_focus == 1 ~ "crop only",
      agecon_focus == 2 ~ "livestock only",
      agecon_focus == 3 ~ "mixed farming",
      TRUE ~ NA_character_
    )
  ) %>%
  mutate(
    # Determine whether respondent actually has crop activities
    # using cultivated land as the strongest universal proxy
    has_crop_activity = case_when(
      system_type_declared == "crop only" ~ TRUE,
      coalesce(area_total, 0) > 0 ~ TRUE,
      coalesce(land_owned, 0) > 0 ~ TRUE,
      coalesce(land_rentin, 0) > 0 ~ TRUE,
      coalesce(area_common, 0) > 0 ~ TRUE,
      coalesce(land_other, 0) > 0 ~ TRUE,
      TRUE ~ FALSE
    )
  ) %>%
  mutate(
    # Determine whether respondent has livestock activities
    # using ownership, counts, or sales of animal products
    has_livestock_activity = case_when(
      raise_animals == 1 ~ TRUE,
      coalesce(num_animal, 0) > 0 ~ TRUE,
      TRUE ~ FALSE
    )
  ) %>%
  mutate(
    # Flag inconsistencies between declared system type
    # and observed crop / livestock activities
    system_validation_flag = case_when(
      is.na(system_type_declared) ~
        "check: specialization missing/unknown code",

      system_type_declared == "crop farming" & has_livestock_activity ~
        "inconsistent: declared crop-only but livestock reported",

      system_type_declared == "livestock" & has_crop_activity ~
        "inconsistent: declared livestock-only but land/crop activity reported",

      system_type_declared == "mixed" &
        (!has_crop_activity | !has_livestock_activity) ~
        "inconsistent: declared mixed but missing crop or livestock evidence",

      TRUE ~ "consistent"
    )
  ) %>%
  mutate(
    # Determine whether respondent has economic activities
    # Using existence variables defined as economic/market engagement proxies
    has_economic_activity = case_when(
      !is.na(time_market) ~ TRUE,
      !is.na(local_market_share) ~ TRUE,
      TRUE ~ FALSE
    ),

    economic_activity_flag = case_when(
      (system_type_declared %in%
        c("crop only", "livestock only", "mixed farming")) &
        !has_economic_activity ~
        "inconsistent: production system declared but no economic activity signal",
      TRUE ~ "consistent"
    )
  ) %>%
  mutate(
    # Check whether respondent gave household details
    household_characteristics_flag = case_when(
      is.na(hh_or_not) | hh_or_not == "" ~
        "check: holding type missing",

      # Household composition fields all missing (presence check only)
      is.na(hh_men) &
        is.na(hh_women) &
        is.na(hh_myoung) &
        is.na(hh_fyoung) &
        is.na(hh_children) &
        is.na(hh_fem) ~
        "check: household composition counts all missing",

      TRUE ~ "ok"
    )
  )


# ------------------------------------------------------------
# Household composition and size validation
# Checks internal consistency of household member counts
# and flags implausible household sizes
# ------------------------------------------------------------

survey_household_df <- survey_system_validation_df %>%
  mutate(
    # Reconstruct household size from demographic subgroups
    household_sum = coalesce(hh_men, 0) +
      coalesce(hh_women, 0) +
      coalesce(hh_myoung, 0) +
      coalesce(hh_fyoung, 0) +
      coalesce(hh_children, 0) +
      coalesce(hh_fem, 0),

    # Validate reported household total against demographic breakdown
    household_flag = case_when(
      is.na(household_sum) ~ "missing household total",
      household_sum < 0 ~ "invalid negative total",
      household_sum == 0 ~ "invalid zero total",
      TRUE ~ "ok"
    ),

    # Apply updated flag logic
    household_plausibility_flag = case_when(
      household_sum > 10 ~ "household size above average",
      TRUE ~ "within expected range"
    ),

    # Cap values above 10 to 4.5
    household_sum = if_else(household_sum > 10, 4.5, household_sum),
  )

# ------------------------------------------------------------
# Agricultural workforce consistency checks
# Ensures agricultural labour counts are internally consistent
# with household size and demographic composition
# ------------------------------------------------------------

survey_agr_workforce_df <- survey_household_df %>%
  mutate(
    # Totals
    agr_sum = coalesce(ag_men, 0) +
      coalesce(ag_women, 0) +
      coalesce(ag_fyoung, 0) +
      coalesce(ag_myoung, 0) +
      coalesce(ag_children, 0)
  ) %>%
  rowwise() %>%
  mutate(
    # Detailed flag
    agr_workforce_flag = case_when(
      # --------------------
      # ERRORS (invalid data)
      # --------------------
      is.na(household_sum) | is.na(agr_sum) ~
        "missing household or agriculture details",

      household_sum < 0 | agr_sum < 0 ~
        "invalid negative total",

      any(
        c(
          ag_men,
          ag_women,
          ag_fyoung,
          ag_myoung,
          ag_children
        ) <
          0,
        na.rm = TRUE
      ) ~
        "invalid negative demographic",

      agr_sum > household_sum ~
        "agricultural workforce exceeds household size",

      # --------------------
      # WARNINGS (implausible)
      # --------------------
      ag_men > hh_men ~
        "men workforce exceeds household men",

      ag_women > hh_women ~
        "women workforce exceeds household women",

      ag_myoung > hh_myoung ~
        "young men workforce exceeds household young men",

      ag_fyoung > hh_fyoung ~
        "young women workforce exceeds household young women",

      ag_children > hh_children ~
        "children workforce exceeds household children",

      TRUE ~ "ok"
    ),

    # Severity classification
    agr_workforce_severity = case_when(
      agr_workforce_flag %in%
        c(
          "missing household or agriculture details",
          "invalid negative total",
          "invalid negative demographic",
          "agricultural workforce exceeds household size"
        ) ~ "ERROR",

      agr_workforce_flag != "ok" ~ "WARNING",

      TRUE ~ "OK"
    )
  ) %>%
  ungroup()


# ------------------------------------------------------------
# Production and land-use consistency checks
# Validates reported crop and livestock production
# against cultivated area and animal ownership
# ------------------------------------------------------------

survey_indicators_check_df <- survey_agr_workforce_df %>%
  mutate(
    # Clean negative land area values (not physically meaningful)
    land_owned_clean = if_else(
      !is.na(land_owned) & land_owned < 0,
      NA_real_,
      land_owned
    ),
    land_rentin_clean = if_else(
      !is.na(land_rentin) & land_rentin < 0,
      NA_real_,
      land_rentin
    ),
    area_common_clean = if_else(
      !is.na(area_common) & area_common < 0,
      NA_real_,
      area_common
    ),
    land_other_clean = if_else(
      !is.na(land_other) & land_other < 0,
      NA_real_,
      land_other
    ),
    area_total_clean = if_else(
      !is.na(area_total) & area_total < 0,
      NA_real_,
      area_total
    ),

    # Crop production must be supported by positive cultivated area
    has_positive_land_area = case_when(
      coalesce(area_total_clean, 0) > 0 ~ TRUE,
      coalesce(land_owned_clean, 0) > 0 ~ TRUE,
      coalesce(land_rentin_clean, 0) > 0 ~ TRUE,
      coalesce(area_common_clean, 0) > 0 ~ TRUE,
      coalesce(land_other_clean, 0) > 0 ~ TRUE,
      TRUE ~ FALSE
    ),

    crop_quantity_flag = case_when(
      # If declared crop farming or mixed, we expect some positive land area
      system_type_declared %in%
        c("crop farming", "mixed") &
        !has_positive_land_area ~
        "inconsistent: crop/mixed declared but no positive land area reported",
      TRUE ~ "ok"
    )
  ) %>%
  mutate(
    # Clean negative animal values
    num_animal_clean = if_else(
      !is.na(num_animal) & num_animal < 0,
      NA_real_,
      num_animal
    ),

    # Livestock production requires positive animal ownership
    livestock_quantity_flag = case_when(
      # If declared livestock or mixed, we expect livestock evidence
      system_type_declared %in%
        c("livestock", "mixed") &
        !(raise_animals == 1 | coalesce(num_animal_clean, 0) > 0) ~
        "inconsistent: livestock/mixed declared but no positive animal count reported",

      # If raise_animals is FALSE but num_animal > 0, flag
      raise_animals == 0 & coalesce(num_animal_clean, 0) > 0 ~
        "inconsistent: raise_animals=FALSE but num_animal>0",

      TRUE ~ "ok"
    )
  ) %>%
  # Drop intermediate variables used only for validation
  select(
    -land_owned_clean,
    -land_rentin_clean,
    -area_common_clean,
    -land_other_clean,
    -area_total_clean,
    -num_animal_clean
  )



# ------------------------------------------------------------
# Species ownership consistency checks
# Ensures logical alignment between selected species
# and reported total number of species owned
# ------------------------------------------------------------

survey_species_df <- survey_indicators_check_df %>%
  mutate(
    # Clean negative animal values
    num_animal_clean = if_else(
      !is.na(num_animal) & num_animal < 0,
      NA_real_,
      num_animal
    ),

    # Validate consistency between raise_animals and num_animal
    species_quantity_flag = case_when(
      raise_animals == 0 & coalesce(num_animal_clean, 0) > 0 ~
        "inconsistent: raise_animals=FALSE but num_animal>0",

      raise_animals == 1 & (is.na(num_animal_clean) | num_animal_clean <= 0) ~
        "inconsistent: raise_animals=TRUE but num_animal missing/zero",

      TRUE ~ "consistent"
    )
  ) %>%
  select(-num_animal_clean)

clean_df = survey_species_df
# Group all calculated variables
calculated_vars <- c(
  "area_total",

  # Biodiversity / environment
  "plant_diversity",
  "temp_spatial_div",
  "anim_diversity",
  "soilhealth_score",

  # Pesticides
  "mgt_pestdis",

  # Economy perception indicators
  "stabincome",
  "livcon",
  "envfuture",
  "profitable",
  "productive",
  "valueadd",
  "econ_index",

  # Food and nutrition
  "fies_score",
  "dietary_score",

  # Youth
  "youth_m_score",
  "youth_f_score",
  "youth_score",

  # Women empowerment
  "wom_emp_caet",

  # Land tenure
  "landtenure_score_men",
  "landtenure_score_women",
  "landtenure_score_men_pct",
  "landtenure_score_women_pct",

  # Other agroecology indicators
  "seed_breed",
  "recycling_biomass",
  "mgt_soilfert",
  "sum_mgt_pestdis",
  "water_and_energy_use",
  "soc_res",
  "econ_res",
  "soil_conserve",
  "dietdiv_foodself",
  "local_market",
  "local_circ",
  "prod_empow",
  "prod_access",

  # CAET
  "div_score",
  "synergy_score",
  "recycling_score",
  "sum_efficiency",
  "efficiency_score",
  "resilience_score",
  "sum_cultfood",
  "cultfood_score",
  "sum_cocrea",
  "cocrea_score",
  "human_score",
  "sum_circular",
  "circular_score",
  "sum_respgov",
  "respgov_score",
  "caet_score"
)



view_boxplots_grid <- function(
  data,
  vars,
  output_file = "boxplots_calculated_variables.pdf",
  ncol = 8,
  nrow = 10,
  width = 24,
  height = 30
) {
  stopifnot(is.data.frame(data))
  stopifnot(is.character(vars))

  vars_present <- intersect(vars, names(data))

  vars_numeric <- vars_present[
    sapply(data[vars_present], is.numeric)
  ]

  if (length(vars_numeric) == 0) {
    stop("None of the provided variables are numeric.")
  }

  plot_df <- data %>%
    dplyr::select(dplyr::all_of(vars_numeric)) %>%
    tidyr::pivot_longer(
      cols = everything(),
      names_to = "variable",
      values_to = "value"
    )

  p <- ggplot(plot_df, aes(y = value)) +
    geom_boxplot(na.rm = TRUE) +
    facet_wrap(~variable, ncol = ncol, scales = "free_y") +
    labs(
      title = "Boxplots of Calculated Numeric Variables",
      x = NULL,
      y = NULL
    ) +
    theme_minimal() +
    theme(
      strip.text = element_text(size = 8),
      plot.title = element_text(face = "bold")
    )

  ggsave(
    filename = output_file,
    plot = p,
    width = width,
    height = height
  )

  print(p)
  message("Saved: ", output_file)
}


#call the function
view_boxplots_grid(
  data = clean_df,
  vars = calculated_vars
)

# To include the desc stat (calculated the p-values)
desc_stats <- clean_df %>%
  summarise(across(
    all_of(calculated_vars),
    list(
      mean   = ~mean(.x, na.rm = TRUE),
      sd     = ~sd(.x, na.rm = TRUE),
      median = ~median(.x, na.rm = TRUE),
      min    = ~min(.x, na.rm = TRUE),
      max    = ~max(.x, na.rm = TRUE),
      n      = ~sum(!is.na(.x))
    ),
    .names = "{.col}_{.fn}"
  )) %>%
  pivot_longer(
    everything(),
    names_to = c("variable", "stat"),
    names_pattern = "(.+)_(mean|sd|median|min|max|n)"
  ) %>%
  pivot_wider(
    names_from = stat,
    values_from = value
  )



# ------------------------------------------------------------
# DATA CLEANING & PROCESSING on clean_df
# ------------------------------------------------------------

# # Read shapefile
# shp_file <- st_read("gadm41_BDI_shp/gadm41_BDI_2.shp")
#
# # Create centroids
# centroids_data <- shp_file %>%
#   st_transform(3857) %>%
#   st_point_on_surface() %>%
#   st_transform(4326)
#
# # Extract longitude and latitude into columns
# coords <- st_coordinates(centroids_data)
# shp_file$centroid_lon <- coords[,1]
# shp_file$centroid_lat <- coords[,2]
#
# # Build a centroid lookup from the shapefile
# lookup_data <- shp_file %>%
#   st_drop_geometry() %>%
#   transmute(
#     location2 = str_squish(str_to_lower(NAME_2)),
#     centroid_lat,
#     centroid_lon
#   )
#
# # Fix possible case sensitive issues
# clean_df <- clean_df %>%
#   mutate(location2 = str_squish(str_to_lower(location2)))
#
# # Join to clean_df and update localization
# clean_df <- clean_df %>%
#   left_join(lookup_data, by = "location2") %>%
#   mutate(
#     gps_original = gps_loc,
#
#     gps_loc = if_else(
#       is.na(gps_loc) & !is.na(centroid_lat) & !is.na(centroid_lon),
#       paste(centroid_lat, centroid_lon, "0 0"),
#       gps_loc
#     ),
#
#     localization_source = case_when(
#       is.na(gps_original) &
#         !is.na(centroid_lat) &
#         !is.na(centroid_lon) ~ "location 2 centroids",
#       !is.na(gps_original) ~ "farm gps",
#       TRUE ~ "missing"
#     )
#   ) %>%
#
#   # Extract latitude & longitude from gps_loc
#   separate(
#     gps_loc,
#     into = c("gps_loc_latitude", "gps_loc_longitude", "altitude", "accuracy"),
#     sep = " ",
#     remove = FALSE,
#     convert = TRUE
#   ) %>%
#   select(-centroid_lat, -centroid_lon, -gps_original) %>%
#   relocate(
#     localization_source, gps_loc, gps_loc_latitude, gps_loc_longitude,
#     .after = location2
#   )
#

#---------------------------------------TO REVIEW-------------------------------
# select data and prepare to export to db
#include average CAET score per project
# disaggregate by location 2
# check the dietary diversity
# check the Economic perception (check by farming system)
# Household with access to pesticides
# the number of farms that are adopting two or above agroecological practices using the CAET, maybe at the criteria level
# see where we have low scores and see why and  how the project can help (use the threshold of CAET and go to indicess level)
# To show the performance indicator at average and at farm level
clean_df <- clean_df %>%
  mutate(
    Farm_id = paste0(enumerator, " ", row_number()))

data <- clean_df %>%
  select(
    Farm_id,
    gps_loc_latitude,
    gps_loc_longitude,
    area_total,
    hh_or_not,
    irrig,
    farm_mgt,
    hh_men,
    hh_women,
    sex_respondent,
    sex_head,
    age_head,
    agecon_focus,
    farminput_auto,
    time_market,
    "area_total",
    "plant_diversity",
    "temp_spatial_div",
    "anim_diversity",
    "mgt_pestdis",
    "stabincome",
    "livcon",
    "envfuture",
    "profitable",
    "productive",
    "valueadd",
    "econ_index",
    "fies_score",
    "dietary_score",
    "youth_m_score",
    "youth_f_score",
    "youth_score",
    "wom_emp_caet",
    "landtenure_score_men",
    "landtenure_score_women",
    "landtenure_score_men_pct",
    "landtenure_score_women_pct",
    "seed_breed",
    "recycling_biomass",
    "mgt_soilfert",
    "sum_mgt_pestdis",
    "water_and_energy_use",
    "soc_res",
    "econ_res",
    "soil_conserve",
    "dietdiv_foodself",
    "local_market",
    "local_circ",
    "prod_empow",
    "prod_access",
    "div_score",
    "synergy_score",
    "recycling_score",
    "sum_efficiency",
    "efficiency_score",
    "resilience_score",
    "sum_cultfood",
    "cultfood_score",
    "sum_cocrea",
    "cocrea_score",
    "human_score",
    "sum_circular",
    "circular_score",
    "sum_respgov",
    "respgov_score",
    "caet_score"
  ) %>%
  pivot_longer(
    cols = c(
      "area_total",
      "plant_diversity",
      "temp_spatial_div",
      "anim_diversity",
      "mgt_pestdis",
      "stabincome",
      "livcon",
      "envfuture",
      "profitable",
      "productive",
      "valueadd",
      "econ_index",
      "fies_score",
      "dietary_score",
      "youth_m_score",
      "youth_f_score",
      "youth_score",
      "wom_emp_caet",
      "landtenure_score_men",
      "landtenure_score_women",
      "landtenure_score_men_pct",
      "landtenure_score_women_pct",
      "seed_breed",
      "recycling_biomass",
      "mgt_soilfert",
      "sum_mgt_pestdis",
      "water_and_energy_use",
      "soc_res",
      "econ_res",
      "soil_conserve",
      "dietdiv_foodself",
      "local_market",
      "local_circ",
      "prod_empow",
      "prod_access",
      "div_score",
      "synergy_score",
      "recycling_score",
      "sum_efficiency",
      "efficiency_score",
      "resilience_score",
      "sum_cultfood",
      "cultfood_score",
      "sum_cocrea",
      "cocrea_score",
      "human_score",
      "sum_circular",
      "circular_score",
      "sum_respgov",
      "respgov_score",
      "caet_score"
    ),
    names_to = "Variables",
    values_to = "Value"
  )


message("✅ Data transformed")

# ---------- crietria table ----------

criteria <- clean_df %>%
  select(
    Farm_id,
    num_plant, plant_gendiv,
    temp_div, space_div,
    num_animal, anim_gendiv,
    feed_prod, vary_service,
    tree_farm, tree_divspe,
    plot_mosaic, natveg_pct,
    useloc_seedbreed, nat_seedbreed,
    reuse_orgres, nonorg_waste,
    onfarm_nutcycle,
    nitrogen_plant,
    onf_eco,
    ## onf_eco_prac,
    use_vet,
    water_use_save, energy_use_save,
    community_coop, ext_finance,
    div_activ, farminput_auto,
    soil_cover, soil_disturb,
    food_div, farm_ingred,
    trad_food, value_heritage,
    context_know, share_aginnov,
    know_platform, know_process,
    women_role, youth,
    workcond_farmer, workcond_emp,
    agro_innov, activ_outside,
    local_market_share, pgs_cert,
    orig_inputs, resource_share,
    resp_gov, auto_dec,
    access_finance, access_land, access_gene
  ) %>%
  pivot_longer(
    -Farm_id,
    names_to = "Criteria",
    values_to = "criteria_score"
  )

criteria_map <- tibble::tibble(
  Criteria = c(
    "num_plant",
    "plant_gendiv",
    "temp_div",
    "space_div",
    "num_animal",
    "anim_gendiv",
    "feed_prod",
    "vary_service",
    "tree_farm",
    "tree_divspe",
    "plot_mosaic",
    "natveg_pct",
    "useloc_seedbreed",
    "nat_seedbreed",
    "reuse_orgres",
    "nonorg_waste",
    "onfarm_nutcycle",
    "nitrogen_plant",
    "onf_eco",
    "onf_eco_prac",
    "use_vet",
    "water_use_save",
    "energy_use_save",
    "community_coop",
    "ext_finance",
    "div_activ",
    "farminput_auto",
    "soil_cover",
    "soil_disturb",
    "food_div",
    "farm_ingred",
    "trad_food",
    "value_heritage",
    "context_know",
    "share_aginnov",
    "know_platform",
    "know_process",
    "women_role",
    "youth",
    "workcond_farmer",
    "workcond_emp",
    "agro_innov",
    "activ_outside",
    "local_market_share",
    "pgs_cert",
    "orig_inputs",
    "resource_share",
    "resp_gov",
    "auto_dec",
    "access_finance",
    "access_land",
    "access_gene"
  ),

  Indexes = c(
    "plant_diversity",
    "plant_diversity",
    "temp_spatial_div",
    "temp_spatial_div",
    "anim_diversity",
    "anim_diversity",
    "crop_liv_aqua",
    "crop_liv_aqua",
    "integ_trees",
    "integ_trees",
    "connect_ag",
    "connect_ag",
    "seed_breed",
    "seed_breed",
    "recycling_biomass",
    "recycling_biomass",
    "water_energy",
    "water_energy",
    "mgt_soilfert",
    "mgt_soilfert",
    "mgt_pestdis",
    "mgt_pestdis",
    "water_and_energy_use",
    "water_and_energy_use",
    "soc_res",
    "soc_res",
    "econ_res",
    "econ_res",
    "soil_conserve",
    "soil_conserve",
    "dietdiv_foodself",
    "dietdiv_foodself",
    "food_heritage",
    "food_heritage",
    "int_ageco",
    "int_ageco",
    "soc_know",
    "soc_know",
    "wom_emp_caet",
    "wom_emp_caet",
    "labour_socinq",
    "labour_socinq",
    "labor_condition",
    "labor_condition",
    "local_market",
    "local_market",
    "local_circ",
    "local_circ",
    "prod_empow",
    "prod_empow",
    "prod_access",
    "prod_access"
  )
) %>%
  mutate(
    Index = recode(
      Indexes,
      plant_diversity = "Plant diversity",
      temp_spatial_div = "Temporal and spatial diversity",
      anim_diversity = "Animal diversity (including fishes and insects)",
      crop_liv_aqua = "Plant-livestock-aquaculture integration",
      integ_trees = "Integration with trees",
      connect_ag = "Habitat management",
      seed_breed = "Use of seeds and breeds",
      recycling_biomass = "Biomass and waste management",
      water_energy = "Water and energy sources",
      mgt_soilfert = "Management of soil fertility",
      mgt_pestdis = "Management of pests and diseases",
      water_and_energy_use = "Water and energy use",
      soc_res = "Social resilience",
      econ_res = "Economic resilience",
      soil_conserve = "Soil conservation practices",
      dietdiv_foodself = "Dietary diversity and food self sufficiency",
      food_heritage = "Local and traditional food heritage",
      int_ageco = "Co-creation of knowledge",
      soc_know = "Peer learning and sharing of knowledge",
      wom_emp_caet = "Women empowerment",
      labour_socinq = "Labour conditions",
      labor_condition = "Motivation and youth installation",
      local_market = "Local and solidarity-based markets",
      local_circ = "Local sourcing and circularity",
      prod_empow = "Producers' empowerment",
      prod_access = "Producers' access to and control over resources"
    )
  )

criteria <- criteria %>%
  left_join(criteria_map, by = "Criteria")

# ---------- Indexes table ----------
indexes <- clean_df %>%
  pivot_longer(
    cols = c(
      plant_diversity,
      temp_spatial_div,
      anim_diversity,
      crop_liv_aqua,
      integ_trees,
      connect_ag,
      seed_breed,
      recycling_biomass,
      water_energy,
      mgt_soilfert,
      mgt_pestdis,
      water_and_energy_use,
      soc_res,
      econ_res,
      soil_conserve,
      dietdiv_foodself,
      food_heritage,
      int_ageco,
      soc_know,
      wom_emp_caet,
      labour_socinq,
      labor_condition,
      local_market,
      local_circ,
      prod_empow,
      prod_access
    ),
    names_to = "index_var",
    values_to = "index_score"
  ) %>%

  # ✅ Clean labeling of indexes (FAO TAPE standard)
  mutate(
    Index = recode(
      index_var,
      plant_diversity = "Plant diversity",
      temp_spatial_div = "Temporal and spatial diversity",
      anim_diversity = "Animal diversity (including fishes and insects)",
      crop_liv_aqua = "Plant-livestock-aquaculture integration",
      integ_trees = "Integration with trees",
      connect_ag = "Habitat management",
      seed_breed = "Use of seeds and breeds",
      recycling_biomass = "Biomass and waste management",
      water_energy = "Water and energy sources",
      mgt_soilfert = "Management of soil fertility",
      mgt_pestdis = "Management of pests and diseases",
      water_and_energy_use = "Water and energy use",
      soc_res = "Social resilience",
      econ_res = "Economic resilience",
      soil_conserve = "Soil conservation practices",
      dietdiv_foodself = "Dietary diversity and food self sufficiency",
      food_heritage = "Local and traditional food heritage",
      int_ageco = "Co-creation of knowledge",
      soc_know = "Peer learning and sharing of knowledge",
      wom_emp_caet = "Women empowerment",
      labour_socinq = "Labour conditions",
      labor_condition = "Motivation and youth installation",
      local_market = "Local and solidarity-based markets",
      local_circ = "Local sourcing and circularity",
      prod_empow = "Producers' empowerment",
      prod_access = "Producers' access to and control over resources"
    )
  ) %>%

  # ✅ Assign TAPE Elements (10 elements)
  mutate(
    Element = case_when(
      Index %in%
        c(
          "Plant diversity",
          "Temporal and spatial diversity",
          "Animal diversity (including fishes and insects)"
        ) ~ "Diversity",

      Index %in%
        c(
          "Plant-livestock-aquaculture integration",
          "Integration with trees",
          "Habitat management"
        ) ~ "Synergies",

      Index %in%
        c(
          "Use of seeds and breeds",
          "Biomass and waste management"
        ) ~ "Recycling",

      Index %in%
        c(
          "Water and energy sources",
          "Management of soil fertility",
          "Management of pests and diseases",
          "Water and energy use"
        ) ~ "Efficiency",

      Index %in%
        c(
          "Social resilience",
          "Economic resilience",
          "Soil conservation practices"
        ) ~ "Resilience",

      Index %in%
        c(
          "Dietary diversity and food self sufficiency",
          "Local and traditional food heritage"
        ) ~ "Culture and food traditions",

      Index %in%
        c(
          "Peer learning and sharing of knowledge",
          "Co-creation of knowledge"
        ) ~ "Co-creation and sharing of knowledge",

      Index %in%
        c(
          "Women empowerment",
          "Labour conditions",
          "Motivation and youth installation"
        ) ~ "Human and social values",

      Index %in%
        c(
          "Local and solidarity-based markets",
          "Local sourcing and circularity"
        ) ~ "Circular and solidarity economy",

      Index %in%
        c(
          "Producers' empowerment",
          "Producers' access to and control over resources"
        ) ~ "Responsible governance",

      TRUE ~ NA_character_
    )
  ) %>%
  select(
    Farm_id,
    gps_loc_latitude,
    gps_loc_longitude,
    area_total,
    hh_or_not,
    irrig,
    farm_mgt,
    hh_men,
    hh_women,
    sex_respondent,
    sex_head,
    age_head,
    agecon_focus,
    farminput_auto,
    time_market,
    Index,
    index_score,
    Element
  )


# ---------- Elements table ----------
element <- clean_df %>%
  pivot_longer(
    cols = c(
      div_score,
      synergy_score,
      recycling_score,
      efficiency_score,
      resilience_score,
      cultfood_score,
      cocrea_score,
      human_score,
      circular_score,
      respgov_score
    ),
    names_to = "Element",
    values_to = "avg_score"
  ) %>%
  mutate(
    Element = recode(
      Element,
      div_score = "Diversity",
      synergy_score = "Synergies",
      efficiency_score = "Efficiencies",
      recycling_score = "Recycling",
      resilience_score = "Resilience",
      cultfood_score = "CulFood",
      cocrea_score = "Co-Creation",
      human_score = "Human",
      circular_score = "Circular",
      respgov_score = "Responsible gov"
    )
  ) %>%
  select(
    Farm_id,
    gps_loc_latitude,
    gps_loc_longitude,
    area_total,
    hh_or_not,
    irrig,
    farm_mgt,
    hh_men,
    hh_women,
    sex_respondent,
    sex_head,
    age_head,
    agecon_focus,
    farminput_auto,
    time_market,
    Element,
    avg_score
  )

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
dbWriteTable(con, "tape_criteria", criteria, overwrite = TRUE)

dbExecute(con, "ANALYZE;")
dbDisconnect(con)

message("✅ PostgreSQL tables updated successfully")
message("🎯 Pipeline completed without errors")
