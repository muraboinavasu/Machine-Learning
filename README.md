**Distributed Data Engineering & Scalable Machine Learning
Conversational AI Behaviour Segmentation Using PySpark**
Project Overview

This project implements a scalable distributed big data engineering and machine learning pipeline using Apache PySpark to perform unsupervised behavioural segmentation on the WildChat-4.8M conversational dataset.

The system demonstrates:

Distributed data ingestion (Parquet)

Advanced feature engineering with custom transformers

Dimensionality reduction (PCA)

Distributed clustering (MLlib)

Hyperparameter tuning using CrossValidator

Weak and strong scaling analysis

Business visualization via Tableau dashboards

The final model achieved:

Silhouette Score (PCA + KMeans): 0.8062

Cross-validated best score: 0.9329

Significantly outperforming scikit-learn single-node baseline (0.4329)

Repository Structure
Data ingestion.ipynb
 Feature Engineering.ipynb
Model engineering.ipynb
 Vasu_bigdata.docx
 README.md
Dataset

Dataset: WildChat-4.8M (Allen Institute for AI)
Source: Hugging Face
Sample Used: 200,000 stratified records (4.2% of total dataset)

Attributes (10 Columns)

conversation hash

model

timestamp

turn

language

toxic (Boolean)

redacted (Boolean)

country

hashed IP

state

Data Quality

✔ Schema validation (Boolean, Long, Timestamp types)
✔ No missing values
✔ No duplicate rows
✔ Timestamp type conversions verified
Architecture Overview
Data Engineering (Distributed)

SparkSession configured:

4GB executor memory

4GB driver memory

50 shuffle partitions

16 default parallelism

Storage Format: Parquet

Columnar

Compression

Predicate pushdown

Catalyst optimization compatible

DataFrame API preferred over RDD (Catalyst + Tungsten optimization)

 Feature Engineering Pipeline
Categorical Encoding

StringIndexer

OneHotEncoder

Applied to: model, language, country, state

Numerical Feature Scaling

message count

conversation length

max turn

hour

day of week

StandardScaler

Custom Transformer

EngagementScoreTransformer

Aggregates behavioural engagement metrics

 Dimensionality Reduction

PCA (k=2 components)

Purpose:

Reduce multicollinearity

Improve cluster separation

Reduce noise

Enable 2D cluster visualization

PCA significantly improved cluster cohesion and separation.

 Model Engineering
Distributed MLlib Models Compared
Model	Silhouette Score
Numeric KMeans	0.5906
BisectingKMeans	~ similar
Gaussian Mixture Model	~ similar
PCA + KMeans	0.8062
scikit-learn baseline	0.4329
 Hyperparameter Tuning

Grid Search: K ∈ {2,3,4,5,6,7,8}

Optimal K = 3

CrossValidator:

k ∈ {2,3,4,5}

maxIter ∈ {20,50}

3-fold CV

Best CV score: 0.9329

Behavioural Cluster Interpretation

Three behavioural segments identified:

 Cluster 0 — Power Users

High message volume

Long sessions

Technical discussions

 Cluster 1 — Moderate Users

Medium session length

Consistent engagement

Cluster 2 — Casual Users

Low frequency

Short interactions

Exploratory usage

 Scalability Analysis
Weak Scaling (Data Volume Increase)

20% → 100%

Training time: 0.96s → 1.15s

Near-linear scaling

Strong Scaling (Partition Variation)

Partitions tested: {4, 8, 16, 32}

Partitions	Time
4	1.41s
8	0.30s
16	1.18s
32	1.60s

Optimal partition count: 8

Observation:

Over-parallelization increases overhead

Partitioning must match dataset size

 Tableau Dashboards

Four dashboards developed:

Dashboard 1 — Data Quality

Row counts

Null analysis

Column validation metrics

Dashboard 2 — Model Performance

Silhouette scores

PCA variance

K vs Silhouette curve

Feature importance

Dashboard 3 — Business Insights

Hourly/daily trends

Country distribution

Engagement segmentation

Dashboard 4 — Scalability & Cost

Weak/strong scaling curves

Stage timing analysis

Cost-performance trade-offs

 Research Questions Addressed

Can distributed PySpark ingest and process conversational big data reliably?
→ Yes, with validated schema and integrity checks.

Do distributed MLlib models outperform single-node scikit-learn?
→ Yes (0.8062 vs 0.4329).

Does the pipeline demonstrate scalability?
→ Yes, near-linear weak scaling and optimal strong scaling at 8 partitions.
