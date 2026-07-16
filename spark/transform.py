from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("mtg_transform").master("local[*]").getOrCreate()

df = spark.read.option("multiline", "true").json("all_cards.json")

# ---------------------------------------------------------------------------
# STEP A: Filter out promos and foil-only printings.
#
# Promos and foils are driven by collector scarcity / print
# run size, not by the playability + mechanical signals I am modeling on.
# Leaving them in would inject price noise the model can't explain.
# "nonfoil == True" means this printing has a real non-foil version to price.
# ---------------------------------------------------------------------------
df_filtered = df.filter((F.col("promo") == False) & (F.col("nonfoil") == True))
print(f"After dropping promos and foil-only printings: {df_filtered.count()} rows")

# ---------------------------------------------------------------------------
# STEP A2: Drop basic lands (Forest, Plains, Mountain, Swamp, Island).
#
# Basic lands are printed in effectively unlimited quantities in almost
# every set. Their price is driven by cosmetic art variation, not by any
# of our modeling signals
#
# ---------------------------------------------------------------------------
df_filtered = df_filtered.filter(~F.col("type_line").contains("Basic Land"))
print(f"After dropping basic lands: {df_filtered.count()} rows")


# ---------------------------------------------------------------------------
# STEP B: Extract and clean the price target.
#
# ---------------------------------------------------------------------------
df_priced = df_filtered.withColumn(
    "price_usd", F.col("prices.usd").cast("double")
).filter(F.col("price_usd").isNotNull())
print(f"After requiring a non-null usd price: {df_priced.count()} rows")
 
 
# ---------------------------------------------------------------------------
# STEP C: Log-transform the target.
#
# Card prices are heavily right-skewed (many cheap cards, a few very
# expensive ones) log1p (ln(x + 1)) corrects that skew so the model
# isn't dominated by a handful of expensive outliers.
# ---------------------------------------------------------------------------
df_target = df_priced.withColumn("log_price_usd", F.log1p(F.col("price_usd")))
 
 
# ---------------------------------------------------------------------------
# STEP D: Printing age, based on the card's ORIGINAL release -- not just
# this specific printing's release date.
#
# oracle_id is stable across all printings/reprints of the same card, so we
# build a small lookup of the minimum released_at per oracle_id, computed
# from the FULL unfiltered df (a card's true first printing might itself be
# a promo or foil-only, so we can't compute this from already-filtered rows
# without biasing the "original" date later).
# ---------------------------------------------------------------------------
original_release_lookup = (
    df.withColumn("released_date", F.to_date(F.col("released_at")))
      .groupBy("oracle_id")
      .agg(F.min("released_date").alias("original_release_date"))
)
 
df_age = df_target.join(original_release_lookup, on="oracle_id", how="left")
 
df_age = df_age.withColumn(
    "printing_age_days", F.datediff(F.current_date(), F.col("original_release_date"))
)
 
null_dates = df_age.filter(F.col("original_release_date").isNull()).count()
print(f"Rows with no resolvable original_release_date: {null_dates}")
 
 
# ---------------------------------------------------------------------------
# STEP E: Reserved List flag.
#
# Already a clean boolean in the source data (Wizards' permanent no-reprint
# list)
# ---------------------------------------------------------------------------
df_reserved = df_age.withColumn("is_reserved_list", F.col("reserved"))
 
reserved_count = df_reserved.filter(F.col("is_reserved_list") == True).count()
print(f"Rows on the Reserved List: {reserved_count}")
 
 
# ---------------------------------------------------------------------------
# STEP F: EDHREC rank as a playability/popularity signal.
#
# Lower rank = more commonly played in EDH/Commander decks. Null means "not
# ranked at all" (usually obscure or unplayable cards) I deliberately
# leave nulls as nulls rather than filling in a fake number, since
# XGBoost/LightGBM handle missing values natively at training time.
#
# ---------------------------------------------------------------------------
df_edhrec = df_reserved.withColumn("edhrec_rank", F.col("edhrec_rank"))
 
null_rank_count = df_edhrec.filter(F.col("edhrec_rank").isNull()).count()
print(f"Rows with no EDHREC rank at all: {null_rank_count}")
 
df_edhrec.select(
    "name", "set", "released_at", "original_release_date", "printing_age_days",
    "is_reserved_list", "edhrec_rank", "price_usd", "log_price_usd"
).show(10, truncate=False)

spark.stop()