# Flink API Examples for DataStream API and Table API

The Table API is not a new kid on the block. But the community has worked hard on reshaping its
future. Today, it is one of the core abstractions in Flink next to the DataStream API. The Table API
can deal with bounded and unbounded streams in a unified and highly optimized ecosystem inspired by
databases and SQL. Various connectors and catalogs integrate with the outside world.

But this doesn't mean that the DataStream API will become obsolete any time soon. This repository
demos what Table API is capable of today. We present how the API solves different scenarios:
as a batch processor, a changelog processor, or a streaming ETL tool with many built-in functions
and operators for deduplicating, joining, and aggregating data.

It shows hybrid pipelines in which both APIs interact in symbiosis and contribute their unique
strengths.
