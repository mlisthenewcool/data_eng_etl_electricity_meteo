# DataFrameModel : validation déclarative de DataFrame Polars

## Problème

Les schémas Silver sont **implicites** : pour connaître les colonnes, types et
contraintes attendus d'un dataset, il faut lire le code du transform, le wrapper du
registry, et le DDL Postgres. Il n'y a pas de source de vérité unique.

Concrètement, il manque deux choses :

1. **Schéma déclaratif** — une classe = un contrat lisible par dataset
2. **Contraintes de valeurs** — bornes (`ge`, `le`), valeurs autorisées (`isin`)

Les contraintes structurelles (non-vide, nullabilité des clés, unicité) sont déjà
couvertes par `shared.py` + le registry wrapper.

## Concept

Reproduire le pattern `pydantic.BaseModel` + `Field()`, mais pour des colonnes Polars :

```python
from typing import Annotated


class MeteoSchema(DataFrameModel):
    id_station: Annotated[str, Column(nullable=False)]
    date_utc: Annotated[datetime, Column(nullable=False, unique=True)]
    temperature_k: Annotated[float, Column(ge=173.15, le=333.15)]
    humidite: Annotated[int, Column(ge=0, le=100)]
    pression_station: float  # nullable par défaut, pas de contrainte


# Validation
df = MeteoSchema.validate(df)  # DataFrame → vérifie tout
lf = MeteoSchema.validate_lazy(lf)  # LazyFrame → vérifie le schéma sans collect
schema = MeteoSchema.polars_schema()  # → pl.Schema réutilisable
```

## Architecture

Trois composants :

```
Column (dataclass)          — descripteur de contraintes par colonne
DataFrameModelMeta (type)   — metaclass qui introspecte les type hints
DataFrameModel (classe)     — API de validation (validate, validate_lazy, polars_schema)
```

### 1. `Column` — descripteur de contraintes

Équivalent de `pydantic.Field()` pour une colonne DataFrame.

```python
@dataclass(frozen=True, slots=True)
class Column:
    """Contraintes déclaratives sur une colonne Polars."""

    nullable: bool = True
    unique: bool = False
    dtype: pl.DataType | None = None  # override du mapping automatique
    ge: float | int | None = None
    le: float | int | None = None
    gt: float | int | None = None
    lt: float | int | None = None
    isin: Sequence[Any] | None = None
```

Le champ `dtype` permet de surcharger le mapping automatique pour des cas comme
`Annotated[int, Column(dtype=pl.Int16)]` quand `Int64` est trop large.

### 2. `DataFrameModelMeta` — introspection des type hints

La metaclass lit les annotations de la classe au moment de sa définition (`__new__`)
et construit un dictionnaire `__columns__` de colonnes résolues.

```python
_TYPE_MAP: dict[type, pl.DataType] = {
    int: pl.Int64,
    float: pl.Float64,
    str: pl.String,
    bool: pl.Boolean,
    datetime: pl.Datetime("us"),
    date: pl.Date,
}


@dataclass(frozen=True, slots=True)
class _ResolvedColumn:
    """Colonne résolue : nom + dtype Polars + contraintes."""

    name: str
    dtype: pl.DataType
    constraints: Column


class DataFrameModelMeta(type):
    """Metaclass qui extrait les définitions de colonnes des type hints."""

    def __new__(
            mcs,
            name: str,
            bases: tuple[type, ...],
            namespace: dict[str, Any],
    ) -> type:
        cls = super().__new__(mcs, name, bases, namespace)
        if name == "DataFrameModel":
            return cls

        hints = get_type_hints(cls, include_extras=True)
        columns: dict[str, _ResolvedColumn] = {}

        for field_name, hint in hints.items():
            if field_name.startswith("_"):
                continue
            python_type, col_meta = _parse_annotated(hint)
            dtype = col_meta.dtype or _TYPE_MAP.get(python_type)
            if dtype is None:
                msg = f"No Polars dtype mapping for {python_type!r} on '{field_name}'"
                raise TypeError(msg)
            columns[field_name] = _ResolvedColumn(field_name, dtype, col_meta)

        cls.__columns__ = columns
        return cls


def _parse_annotated(hint: Any) -> tuple[type, Column]:
    """Extrait (python_type, Column) d'un type hint, avec support Annotated."""
    if get_origin(hint) is Annotated:
        args = get_args(hint)
        for arg in args[1:]:
            if isinstance(arg, Column):
                return args[0], arg
        return args[0], Column()
    return hint, Column()
```

**Mécanisme** : `get_type_hints(cls, include_extras=True)` déplie les
`Annotated[T, ...]`
et conserve les métadonnées. On cherche une instance de `Column` dans les arguments
supplémentaires. En l'absence, un `Column()` par défaut (nullable, sans contrainte) est
utilisé.

### 3. `DataFrameModel` — validation en deux passes

```python
class DataFrameModel(metaclass=DataFrameModelMeta):
    """Validation déclarative de DataFrame Polars, syntaxe Pydantic."""

    __columns__: ClassVar[dict[str, _ResolvedColumn]]

    @classmethod
    def polars_schema(cls) -> pl.Schema:
        """Retourne un pl.Schema utilisable avec scan_parquet(schema=...)."""
        return pl.Schema(
            {c.name: c.dtype for c in cls.__columns__.values()}
        )

    @classmethod
    def validate(cls, df: pl.DataFrame) -> pl.DataFrame:
        """Valide schéma + valeurs. Lève SchemaValidationError si invalide."""
        errors = cls._check_schema(df.schema) + cls._check_values(df)
        if errors:
            raise SchemaValidationError(errors)
        return df

    @classmethod
    def validate_lazy(cls, lf: pl.LazyFrame) -> pl.LazyFrame:
        """Valide uniquement le schéma (colonnes + dtypes), sans collect."""
        errors = cls._check_schema(lf.collect_schema())
        if errors:
            raise SchemaValidationError(errors)
        return lf
```

#### Passe 1 — schéma (sans collect, fonctionne sur LazyFrame)

Vérifie la présence des colonnes et la compatibilité des dtypes.

```python
@classmethod
def _check_schema(cls, schema: pl.Schema) -> list[str]:
    errors: list[str] = []
    expected = set(cls.__columns__)
    actual = set(schema.names())

    for name in sorted(expected - actual):
        errors.append(f"Missing column: {name}")
    for name in sorted(actual - expected):
        errors.append(f"Unexpected column: {name}")

    for name, col in cls.__columns__.items():
        if name in schema and schema[name] != col.dtype:
            errors.append(
                f"{name}: expected {col.dtype}, got {schema[name]}"
            )
    return errors
```

#### Passe 2 — valeurs (DataFrame uniquement, expressions Polars vectorisées)

Vérifie les contraintes de contenu colonne par colonne.

```python
@classmethod
def _check_values(cls, df: pl.DataFrame) -> list[str]:
    errors: list[str] = []

    for name, col in cls.__columns__.items():
        if name not in df.columns:
            continue
        c = col.constraints
        series = df[name]

        if not c.nullable and series.null_count() > 0:
            errors.append(f"{name}: {series.null_count()} nulls")

        if c.unique:
            n_non_null = len(series) - series.null_count()
            if series.n_unique() - (1 if series.null_count() else 0) != n_non_null:
                errors.append(f"{name}: contains duplicates")

        if c.ge is not None:
            n = df.filter(pl.col(name) < c.ge).height
            if n:
                errors.append(f"{name}: {n} values < {c.ge}")

        if c.le is not None:
            n = df.filter(pl.col(name) > c.le).height
            if n:
                errors.append(f"{name}: {n} values > {c.le}")

        if c.gt is not None:
            n = df.filter(pl.col(name) <= c.gt).height
            if n:
                errors.append(f"{name}: {n} values <= {c.gt}")

        if c.lt is not None:
            n = df.filter(pl.col(name) >= c.lt).height
            if n:
                errors.append(f"{name}: {n} values >= {c.lt}")

        if c.isin is not None:
            n = df.filter(
                pl.col(name).is_not_null() & ~pl.col(name).is_in(c.isin)
            ).height
            if n:
                errors.append(f"{name}: {n} values outside allowed set")

    return errors
```

## Intégration avec le code existant

### Dans le registry wrapper

```python
# registry.py — le wrapper existant appellerait schema.validate()
def get_silver_transform(dataset_name, transform_fn, primary_key, schema=None):
    def wrapped(path: Path) -> pl.DataFrame:
        df = pl.read_parquet(path)
        df = prepare_silver(df, dataset_name)
        df = transform_fn(df)
        if schema is not None:
            df = schema.validate(df)
        else:
            # Fallback sur les checks existants
            validate_not_empty(df, dataset_name)
            if primary_key:
                validate_no_nulls(df, primary_key, dataset_name)
                validate_unique(df, primary_key, dataset_name)
        return df

    return wrapped
```

### Schémas par dataset

```python
# transformations/schemas.py
from data_eng_etl_electricity_meteo.transformations.dataframe_model import (
    Column,
    DataFrameModel,
)


class MeteoClimatologieSchema(DataFrameModel):
    id_station: Annotated[str, Column(nullable=False)]
    date_heure: Annotated[datetime, Column(nullable=False)]
    temperature_k: Annotated[float, Column(ge=173.15, le=333.15)]
    point_de_rosee_k: Annotated[float, Column(ge=173.15, le=333.15)]
    humidite: Annotated[int, Column(ge=0, le=100, dtype=pl.Int16)]
    pression_station: float
    pression_mer: float
    direction_vent: Annotated[int, Column(ge=0, le=360, dtype=pl.Int16)]
    vitesse_vent: Annotated[float, Column(ge=0)]
    nebulosite: Annotated[int, Column(ge=0, le=9, dtype=pl.Int16)]
    precipitations_dernieres_24h: Annotated[float, Column(ge=0)]


class Eco2mixSchema(DataFrameModel):
    code_insee_region: Annotated[str, Column(nullable=False)]
    date_heure: Annotated[datetime, Column(nullable=False)]
    consommation: Annotated[int, Column(ge=0)]
    thermique: int
    nucleaire: Annotated[int, Column(ge=0)]
    eolien: Annotated[int, Column(ge=0)]
    solaire: Annotated[int, Column(ge=0)]
    hydraulique: int
    bioenergies: Annotated[int, Column(ge=0)]
```

### Validation Bronze (LazyFrame)

```python
# Dans un transform bronze
lf = pl.scan_parquet(landing_path)
lf = MeteoClimatologieSchema.validate_lazy(lf)  # schéma uniquement, pas de collect
```

## Limites et edge cases

### Types composites Polars

Le `_TYPE_MAP` ne couvre que les types scalaires. Pour `pl.List`, `pl.Struct`, ou
`pl.Categorical`, il faut utiliser le `dtype` override :

```python
tags: Annotated[list, Column(dtype=pl.List(pl.String))]
```

Pas de parsing automatique de `list[str]` → `pl.List(pl.String)`. C'est faisable
(via `get_args` récursif), mais ça complexifie le code pour un cas rare dans ce projet.

### Timezone-aware Datetime

`datetime` mappe vers `pl.Datetime("us")` sans timezone. Pour `Datetime("us", "UTC")` :

```python
date_utc: Annotated[datetime, Column(dtype=pl.Datetime("us", "UTC"))]
```

### Colonnes optionnelles

Toutes les colonnes déclarées sont attendues. Il n'y a pas de concept de colonne
"optionnelle" (présente ou absente). Si un dataset a des colonnes variables, ne pas
les inclure dans le schéma — la passe schéma signale les colonnes en trop et en moins.

## Alternatives considérées

### Option A — Dict de schéma + `validate_bounds()` (recommandé pour l'instant)

Approche minimaliste sans metaclass : un dict `pl.Schema` + un dict de bornes par
dataset, et une seule fonction `validate_bounds()` dans `shared.py`.

```python
# Dans le module de transformation
SILVER_SCHEMA = pl.Schema({
    "id_station": pl.String,
    "temperature_k": pl.Float64,
    "humidite": pl.Int16,
})

BOUNDS = {
    "temperature_k": (173.15, 333.15),
    "humidite": (0, 100),
}


# shared.py
def validate_bounds(
        df: pl.DataFrame,
        bounds: dict[str, tuple[float, float]],
        dataset_name: str,
) -> None:
    for col, (lo, hi) in bounds.items():
        n = df.filter(
            pl.col(col).is_not_null()
            & ((pl.col(col) < lo) | (pl.col(col) > hi))
        ).height
        if n:
            raise TransformValidationError(
                dataset_name=dataset_name,
                reason=f"{col}: {n} values outside [{lo}, {hi}]",
            )
```

**Avantages** : zéro abstraction, 20 lignes, s'intègre en 2 lignes dans le registry.
**Inconvénients** : schéma et contraintes dans deux endroits séparés, pas de validation
de types intégrée.

### Option B — Patito (bibliothèque externe)

```python
import patito as pt


class MeteoModel(pt.Model):
    id_station: str = pt.Field(nullable=False)
    temperature_k: float = pt.Field(ge=173.15, le=333.15)


MeteoModel.validate(df)
```

**Avantages** : déjà écrit et testé, syntaxe Pydantic native.
**Inconvénients** : dernière release en 2023, projet peu actif.

### Option C — Pandera (bibliothèque externe)

```python
import pandera.polars as pa


class MeteoSchema(pa.DataFrameModel):
    id_station: str = pa.Field(nullable=False)
    temperature_k: float = pa.Field(ge=173.15, le=333.15)


MeteoSchema.validate(df)
```

**Avantages** : activement maintenu, support Polars natif depuis v0.19, tests
statistiques.
**Inconvénients** : dépendance supplémentaire (~5 deps), API parfois verbeuse.