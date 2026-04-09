## dbt Healthcare Models

This dbt project standardises healthcare data that has already been landed in the warehouse database. The source system database is kept separate from the warehouse target.

The warehouse database is expected to contain:

- `landing`: ingested source-system and partner-feed tables
- `staging`: dbt standardisation layers
- `gold`: reporting and ML marts

The warehouse now supports three consumption patterns:

- reporting marts for partner, city, medication, and access-channel analytics
- patient-level ML features for outreach and adherence-risk modeling
- a GNN-ready edge list that represents patient, partner, and medication relationships

### Typical commands

Try running the following commands:
- dbt run
- dbt test

### Modeling intent

- `raw.customers`: patient master data
- `raw.products`: medication catalogue by city
- `raw.transactions`: dispensing and fulfilment events
- `raw.resellers`: partner clinic and pharmacy organisations
- `raw.resellerscsv` and `raw.resellersxml`: external partner daily feeds
- `staging_partner_events`: unified event spine across internal and partner sources
- `fct_dispensing_events`: reporting fact table
- `ml_patient_features`: feature set for care-coordination and adherence models
- `ml_gnn_edges`: graph edge list for a heterogeneous GNN

### GNN use case

One realistic use case is proactive care coordination. A heterogeneous GNN can learn over patient to medication, patient to partner, and partner to medication edges to predict which patients are drifting toward fragmented care, for example repeated cross-partner dispensing with long refill gaps and channel switching.

### Resources

- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
