"""Leakage-safe preprocessing builders for the fare-prediction model (Phase 2).

Two ColumnTransformer variants over the feature frame produced by
features.build_features():

- "scaled": StandardScaler on numerics — for linear models / SVM / NN
- "tree":   numerics passed through raw — for tree ensembles

Shared by both variants:
- OneHotEncoder (handle_unknown="ignore") on low-cardinality categoricals
- sklearn TargetEncoder on the high-cardinality od_corridor key. Its
  fit_transform is internally CROSS-FITTED (out-of-fold encodings), which is
  the plan's leakage requirement — a naive group-mean would leak the target.

Every preprocessor is meant to live inside Pipeline(preprocessor, model) and
be fit on training folds only.
"""

from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler, TargetEncoder

# Column groups (intersected with whatever columns the caller actually has,
# so the trip_duration_min ablation just works).
# Only the derived half of each raw/derived pair appears here: distance_capped
# (not trip_distance) and temp_band_ord (not temperature). features.py already
# drops the raw halves; remainder="drop" below is the second line of defence.
NUMERIC_COLUMNS = [
    "distance_capped",
    "trip_duration_min",
    "passenger_count",
    "pickup_dow",
    "humidity",
    "windSpeed",
    "visibility",
    "pickup_hour_sin",
    "pickup_hour_cos",
    "temp_band_ord",
]
BINARY_COLUMNS = ["is_airport_trip"]
OHE_COLUMNS = ["service_type", "pickup_borough", "dropoff_borough"]
TARGET_ENCODED_COLUMNS = ["od_corridor"]

VARIANTS = ("scaled", "tree")

RANDOM_STATE = 42


def build_preprocessor(variant: str, feature_columns) -> ColumnTransformer:
    """Build the ColumnTransformer for the given variant.

    `feature_columns` is the X frame's column list; each block only claims
    the columns that are actually present.
    """
    if variant not in VARIANTS:
        raise ValueError(f"Unknown variant '{variant}' — expected one of {VARIANTS}")

    cols = set(feature_columns)
    numeric = [c for c in NUMERIC_COLUMNS if c in cols]
    binary = [c for c in BINARY_COLUMNS if c in cols]
    ohe = [c for c in OHE_COLUMNS if c in cols]
    target_encoded = [c for c in TARGET_ENCODED_COLUMNS if c in cols]

    numeric_transformer = StandardScaler() if variant == "scaled" else "passthrough"

    return ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, numeric),
            ("bin", "passthrough", binary),
            (
                "ohe",
                OneHotEncoder(
                    # Drop a reference level per categorical on the scaled
                    # variant only. Full one-hot makes each block sum to 1, so
                    # the blocks are mutually dependent and the design matrix
                    # is rank-deficient (measured: rank 25/27, cond ~4e15).
                    # Predictions survive — lstsq takes the minimum-norm
                    # solution — but coefficients don't: pickup_borough_EWR
                    # swung 8.2 -> 27.1 across CV folds. Trees keep every
                    # level; they are indifferent to collinearity and would
                    # only find the dropped level harder to split on.
                    #
                    # Caveat of pairing drop with handle_unknown="ignore": an
                    # unseen category encodes to all-zeros, which is exactly
                    # the reference level's encoding. Unknown and reference
                    # become indistinguishable. Accepted — boroughs and
                    # service_type are closed sets from the zone lookup.
                    drop="first" if variant == "scaled" else None,
                    handle_unknown="ignore",
                    sparse_output=False,
                ),
                ohe,
            ),
            (
                "te",
                TargetEncoder(target_type="continuous", random_state=RANDOM_STATE),
                target_encoded,
            ),
        ],
        remainder="drop",
    )
